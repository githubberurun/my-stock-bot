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

# --- 2. メインロジック ---
df_all = get_latest_jpx_list()
target_list = df_all.head(100).copy()

results = []
now_jst = datetime.utcnow() + timedelta(hours=9)
date_str = now_jst.strftime("%Y-%m-%d")

# スプレッドシート準備
json_data = json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)
scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
credentials = Credentials.from_service_account_info(json_data, scopes=scopes)
gc = gspread.authorize(credentials)
sh = gc.open_by_key(SPREADSHEET_ID)

# 前日価格取得用
prev_prices = {}
try:
    last_sheet = sh.get_worksheet(0)
    if last_sheet and last_sheet.title != date_str:
        records = last_sheet.get_all_records()
        for r in records:
            prev_prices[str(r['コード'])] = r.get('現在値', 0)
except:
    pass

for i, (idx, row) in enumerate(target_list.iterrows()):
    ticker_str = f"{str(row['コード']).strip()}.T"
    print(f"[{i+1}/100] {row['社名']} 詳細分析中...")

    try:
        s = yf.Ticker(ticker_str)
        hist = s.history(period="3mo")
        if hist.empty: continue

        inf = s.info
        def g(k, default=0):
            val = inf.get(k, default)
            return val if (val is not None and np.isfinite(val) if isinstance(val, float) else True) else default

        # 【指標抽出】
        current_price = g('currentPrice', hist['Close'].iloc[-1])
        equity_ratio = g('equityRatio', 0) * 100
        fcf = g('operatingCashflow', 0) + g('investingCashflow', 0)
        net_cash = g('totalCash', 0) - g('totalDebt', 0)
        roe = g('returnOnEquity', 0) * 100
        per = g('trailingPE', 0)
        pbr = g('priceToBook', 0)
        yield_val = g('dividendYield', 0) * 100
        payout_ratio = g('payoutRatio', 0) * 100
        rev_growth = g('revenueGrowth', 0) * 100
        eps = g('trailingEps', 0)
        
        rsi = calculate_rsi(hist['Close']).iloc[-1]
        ma25 = hist['Close'].rolling(window=25).mean().iloc[-1]
        deviation = ((current_price - ma25) / ma25) * 100 if (ma25 and ma25 != 0) else 0

        # 戦略と理論上限
        st_type = "クオリティ・グロース" if roe > 10 and 0 < per < 25 else "ディープ・バリュー"
        safe_eps = eps if eps > 0 else (current_price / 15)
        buy_range_top = max(safe_eps * 12, (g('dividendRate', 0) / 0.04) if g('dividendRate', 0) > 0 else 0)

        # AI分析（全重要指標をGeminiに渡す）
        prompt = (f"銘柄:{row['社名']}, 業種:{row['業種']}\n"
                  f"ROE:{roe:.1f}%, PER:{per}, 成長率:{rev_growth:.1f}%, FCF:{fcf/1e6:.1f}M, 自己資本比率:{equity_ratio:.1f}%\n"
                  f"必ず「スコア(-10〜10)|為替感応度|診断(40文字程度)」で回答。")
        response = client.models.generate_content(model='gemini-2.0-flash', contents=prompt)
        res_text = response.text.strip().replace("\n", " ").replace(",", "、")
        
        ai_val, ai_fx, ai_diag = 0, "中立", res_text
        if "|" in res_text:
            parts = res_text.split("|")
            ai_val = int(re.search(r'(-?\d+)', parts[0]).group(1)) if re.search(r'(-?\d+)', parts[0]) else 0
            ai_fx = parts[1].strip()
            ai_diag = parts[-1].strip()

        # 【厳密なスコアリング：最大100点】
        base_score = 40  # 基礎点
        if roe > 12: base_score += 10
        if fcf > 0: base_score += 10
        if equity_ratio > 50: base_score += 10
        if rev_growth > 5: base_score += 5
        if rsi < 35: base_score += 10
        if yield_val > 3.5: base_score += 10
        if deviation < -10: base_score += 5
        total_score = min(100, max(0, base_score + ai_val))

        day_diff = round(current_price - prev_prices.get(str(row['コード']), 0), 1) if prev_prices.get(str(row['コード']), 0) > 0 else 0

        res_row = [
            date_str, row['コード'], row['社名'], st_type, int(total_score),
            round(current_price, 1), day_diff, ai_fx, round(buy_range_top, 1),
            round(((current_price / buy_range_top) - 1) * 100, 1) if buy_range_top > 0 else 0,
            round(yield_val, 2), round(payout_ratio, 1), round(roe, 1), round(per, 1), round(pbr, 2),
            round(equity_ratio, 1), round(fcf/1e6, 1), round(net_cash/1e6, 1),
            round(rsi, 1), round(deviation, 1), ai_diag[:150]
        ]
        results.append([x if not (isinstance(x, float) and not np.isfinite(x)) else 0 for x in res_row])
        time.sleep(1)

    except Exception as e:
        print(f"Error: {row['社名']} - {e}")

# --- 3. スプレッドシート保存 ---
if results:
    try:
        try:
            worksheet = sh.worksheet(date_str)
        except gspread.exceptions.WorksheetNotFound:
            worksheet = sh.add_worksheet(title=date_str, rows="1000", cols="25", index=0)
            legend = ["【凡例】", "総合評価:100点満点(財務・成長・テクニカル統合)", "FCF:営業CF+投資CF(百万)", "ネットキャッシュ:現預金-有利子負債(百万)", "25日乖離:移動平均からの乖離率", "配当性向:利益に対する配当総額の割合"]
            worksheet.append_row(legend)
            header = ['日付', 'コード', '社名', '戦略', '総合評価', '現在値', '前日比', '為替ラベル', 'レンジ上限', '上限乖離率', '利回り', '配当性向', 'ROE', 'PER', 'PBR', '自己資本比率', 'FCF(百万)', 'ネットキャッシュ', 'RSI', '25日乖離', 'AI深層診断']
            worksheet.append_row(header)
        worksheet.append_rows(results)
        print(f"✅ シート「{date_str}」に完全版を保存しました")
    except Exception as e:
        print(f"Spreadsheet Error: {e}")
