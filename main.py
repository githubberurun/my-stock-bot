import pandas as pd
import yfinance as yf
import time
import io
import requests
import re
import os
import json
import numpy as np
from datetime import datetime, timedelta
from google.genai import Client
import gspread
from google.oauth2.service_account import Credentials

# --- 1. 認証・設定 ---
GEMINI_API_KEY = os.environ.get('GEMINI_API_KEY')
GOOGLE_SERVICE_ACCOUNT_JSON = os.environ.get('GOOGLE_SERVICE_ACCOUNT_JSON')
SPREADSHEET_ID = os.environ.get('SPREADSHEET_ID')

client = Client(api_key=GEMINI_API_KEY)

def get_latest_jpx_list():
    url = "https://www.jpx.co.jp/markets/statistics-equities/misc/tvdivq0000001vg2-att/data_j.xls"
    res = requests.get(url)
    with io.BytesIO(res.content) as f:
        df = pd.read_excel(f)
        df = df[['コード', '銘柄名', '市場・商品区分', '33業種区分']]
        df.columns = ['コード', '社名', '市場', '業種']
    return df[df['市場'].str.contains('プライム|スタンダード|グロース', na=False)].copy()

def calculate_rsi(series, period=14):
    if len(series) < period + 1: return pd.Series([50] * len(series))
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss.replace(0, np.nan)
    return (100 - (100 / (1 + rs))).fillna(50)

# --- 2. 銘柄スキャンと選抜 ---
df_all = get_latest_jpx_list()
now_jst = datetime.utcnow() + timedelta(hours=9)
date_str = now_jst.strftime("%Y-%m-%d")

print(f"全 {len(df_all)} 社のスキャンを開始します。")
q_growth_candidates = []
d_value_candidates = []

for i, (idx, row) in enumerate(df_all.iterrows()):
    ticker_str = f"{str(row['コード']).strip()}.T"
    try:
        s = yf.Ticker(ticker_str)
        inf = s.info
        if not inf.get('currentPrice'): continue
        
        roe = inf.get('returnOnEquity', 0) * 100
        per = inf.get('trailingPE', 0)
        pbr = inf.get('priceToBook', 0)
        yield_val = inf.get('dividendYield', 0) * 100
        
        # 暫定スコア計算（選抜用）
        score = 0
        if roe > 10: score += 30
        if yield_val > 3: score += 20
        
        # 戦略分類
        if roe > 10 and 0 < per < 25:
            q_growth_candidates.append({'ticker': ticker_str, 'row': row, 'score': score, 'inf': inf})
        else:
            if 0 < pbr < 1.0: score += 20
            d_value_candidates.append({'ticker': ticker_str, 'row': row, 'score': score, 'inf': inf})
        
        if (len(q_growth_candidates) + len(d_value_candidates)) % 100 == 0:
            print(f"...{len(q_growth_candidates) + len(d_value_candidates)}社 取得済み")
            
        # タイムアウト回避：各500社集まったら選抜へ
        if len(q_growth_candidates) >= 500 and len(d_value_candidates) >= 500: break
    except: continue

# 各戦略から上位100社ずつ抽出
top_q = sorted(q_growth_candidates, key=lambda x: x['score'], reverse=True)[:100]
top_v = sorted(d_value_candidates, key=lambda x: x['score'], reverse=True)[:100]
target_200 = top_q + top_v

# --- 3. 200社の詳細分析 ---
print(f"選抜された200社の詳細分析（全15指標）を開始します。")
json_data = json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)
credentials = Credentials.from_service_account_info(json_data, scopes=['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive'])
gc = gspread.authorize(credentials)
sh = gc.open_by_key(SPREADSHEET_ID)

# 前日比用データの取得
prev_prices = {}
try:
    last_ws = sh.get_worksheet(0)
    for r in last_ws.get_all_records(): prev_prices[str(r['コード'])] = r.get('現在値', 0)
except: pass

final_data = []
for item in target_200:
    inf, row = item['inf'], item['row']
    try:
        s = yf.Ticker(item['ticker'])
        hist = s.history(period="3mo")
        cp = inf.get('currentPrice')
        
        # 指標算出（全指示網羅）
        eq_ratio = inf.get('equityRatio', 0) * 100
        fcf = (inf.get('operatingCashflow', 0) or 0) + (inf.get('investingCashflow', 0) or 0)
        net_cash = (inf.get('totalCash', 0) or 0) - (inf.get('totalDebt', 0) or 0)
        roe = inf.get('returnOnEquity', 0) * 100
        per = inf.get('trailingPE', 0)
        pbr = inf.get('priceToBook', 0)
        yld = inf.get('dividendYield', 0) * 100
        payout = inf.get('payoutRatio', 0) * 100
        rev_g = inf.get('revenueGrowth', 0) * 100
        eps = inf.get('trailingEps', 0)
        
        rsi = calculate_rsi(hist['Close']).iloc[-1]
        ma25 = hist['Close'].rolling(window=25).mean().iloc[-1]
        dev = ((cp - ma25) / ma25) * 100 if ma25 else 0

        # 戦略とレンジ
        st = "クオリティ・グロース" if roe > 10 and 0 < per < 25 else "ディープ・バリュー"
        range_top = max(eps * 12, (inf.get('dividendRate', 0) or 0) / 0.04)
        
        # AI分析（指示通り30-50文字、カンマ排除）
        prompt = (f"銘柄:{row['社名']}, 業種:{row['業種']}\nROE:{roe:.1f}%, FCF:{fcf/1e6:.0f}M, 配当性向:{payout:.1f}%\n"
                  f"必ず「スコア(-15〜15)|為替ラベル|AI診断(50字以内)」で回答。")
        res = client.models.generate_content(model='gemini-2.0-flash', contents=prompt).text.strip().replace("\n"," ").replace(",","、")
        
        ai_val, ai_fx, ai_diag = 0, "中立", res
        if "|" in res:
            p = res.split("|")
            ai_val = int(re.search(r'(-?\d+)', p[0]).group(1)) if re.search(r'(-?\d+)', p[0]) else 0
            ai_fx, ai_diag = p[1].strip(), p[-1].strip()

        score = min(100, max(0, 50 + ai_val + (10 if roe>12 else 0) + (10 if fcf>0 else 0) + (10 if eq_ratio>50 else 0)))

        final_data.append([
            date_str, row['コード'], row['社名'], st, int(score), round(cp, 1),
            round(cp - prev_prices.get(str(row['コード']), cp), 1), ai_fx, round(range_top, 1),
            round(((cp / range_top) - 1) * 100, 1) if range_top else 0,
            round(yld, 2), round(payout, 1), round(roe, 1), round(per, 1), round(pbr, 2),
            round(eq_ratio, 1), round(fcf/1e6, 1), round(net_cash/1e6, 1),
            round(rsi, 1), round(dev, 1), ai_diag[:150]
        ])
        time.sleep(0.5) # API制限考慮
    except: continue

# --- 4. 保存 ---
if final_data:
    ws = sh.add_worksheet(title=date_str, rows="1000", cols="25", index=0)
    legend = ["【凡例】", "総合評価:100点満点(収益・財務・CF統合)", "FCF:営業CF+投資CF(百万)", "ネットキャッシュ:現預金-負債(百万)", "25日乖離:移動平均からの離れ率", "戦略:Qグロース(高ROE)/Dバリュー(低PBR)"]
    header = ['日付', 'コード', '社名', '戦略', '総合評価', '現在値', '前日比', '為替ラベル', 'レンジ上限', '上限乖離率', '利回り', '配当性向', 'ROE', 'PER', 'PBR', '自己資本比率', 'FCF(百万)', 'ネットキャッシュ', 'RSI', '25日乖離', 'AI深層診断']
    ws.append_row(legend)
    ws.append_row(header)
    ws.append_rows(final_data)
    print(f"✅ {date_str} の200社詳細分析完了")
