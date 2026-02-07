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
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload

# --- 1. 認証・設定 ---
GEMINI_API_KEY = os.environ.get('GEMINI_API_KEY')
GOOGLE_SERVICE_ACCOUNT_JSON = os.environ.get('GOOGLE_SERVICE_ACCOUNT_JSON')
SPREADSHEET_ID = os.environ.get('SPREADSHEET_ID')

client = Client(api_key=GEMINI_API_KEY)
json_data = json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)
creds = Credentials.from_service_account_info(json_data, scopes=[
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
])

def get_latest_jpx_list():
    url = "https://www.jpx.co.jp/markets/statistics-equities/misc/tvdivq0000001vg2-att/data_j.xls"
    res = requests.get(url)
    with io.BytesIO(res.content) as f:
        df = pd.read_excel(f, engine='xlrd')
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

# --- 2. 銘柄フルスキャン (全3800社) ---
df_all = get_latest_jpx_list()
print(f"全 {len(df_all)} 社のスキャンを開始...")

all_candidates = []
for i, (idx, row) in enumerate(df_all.iterrows()):
    ticker = f"{str(row['コード']).strip()}.T"
    try:
        s = yf.Ticker(ticker)
        inf = s.info
        if not inf.get('currentPrice'): continue
        
        roe = inf.get('returnOnEquity', 0) * 100
        per = inf.get('trailingPE', 0)
        pbr = inf.get('priceToBook', 0)
        yld = inf.get('dividendYield', 0) * 100
        
        # 評価点（ここで3800社から絞り込み）
        score = (30 if roe > 10 else 0) + (20 if yld > 3.5 else 0) + (20 if 0 < pbr < 1 else 0)
        
        all_candidates.append({
            'ticker': ticker, 'row': row, 'score': score, 'inf': inf,
            'is_growth': (roe > 10 and 0 < per < 25)
        })
        if (i + 1) % 500 == 0: print(f"{i+1}社完了...")
    except: continue

# 上位100社ずつ抽出
q_top = sorted([c for c in all_candidates if c['is_growth']], key=lambda x: x['score'], reverse=True)[:100]
v_top = sorted([c for c in all_candidates if not c['is_growth']], key=lambda x: x['score'], reverse=True)[:100]
target_list = q_top + v_top

# --- 3. 詳細分析 (200社) ---
final_rows = []
now_jst = datetime.utcnow() + timedelta(hours=9)
date_str = now_jst.strftime("%Y-%m-%d")

for item in target_list:
    inf, row = item['inf'], item['row']
    try:
        s = yf.Ticker(item['ticker'])
        hist = s.history(period="3mo")
        cp = inf.get('currentPrice')
        
        # 15指標算出
        eq_ratio = inf.get('equityRatio', 0) * 100
        fcf = (inf.get('operatingCashflow', 0) or 0) + (inf.get('investingCashflow', 0) or 0)
        net_cash = (inf.get('totalCash', 0) or 0) - (inf.get('totalDebt', 0) or 0)
        roe, per, pbr = inf.get('returnOnEquity', 0)*100, inf.get('trailingPE', 0), inf.get('priceToBook', 0)
        yld, payout = inf.get('dividendYield', 0)*100, inf.get('payoutRatio', 0)*100
        eps = inf.get('trailingEps', 0)
        rsi = calculate_rsi(hist['Close']).iloc[-1]
        ma25 = hist['Close'].rolling(window=25).mean().iloc[-1]
        dev = ((cp - ma25) / ma25) * 100 if ma25 else 0

        # AI分析
        prompt = f"銘柄:{row['社名']}\nROE:{roe:.1f}%, FCF:{fcf/1e6:.0f}M\n「スコア(-15〜15)|為替ラベル|診断(50字以内)」で回答。"
        res_ai = client.models.generate_content(model='gemini-2.0-flash', contents=prompt).text.strip().replace(",","、")
        
        ai_val, ai_fx, ai_diag = 0, "中立", res_ai
        if "|" in res_ai:
            p = res_ai.split("|")
            ai_val = int(re.search(r'(-?\d+)', p[0]).group(1)) if re.search(r'(-?\d+)', p[0]) else 0
            ai_fx, ai_diag = p[1].strip(), p[-1].strip()

        score = min(100, max(0, 50 + ai_val + (10 if roe>12 else 0) + (10 if fcf>0 else 0) + (10 if eq_ratio>40 else 0)))

        final_rows.append([
            date_str, row['コード'], row['社名'], 
            "クオリティ・グロース" if item['is_growth'] else "ディープ・バリュー",
            int(score), round(cp, 1), ai_fx, round(max(eps*12, (inf.get('dividendRate',0) or 0)/0.04), 1),
            round(yld, 2), round(payout, 1), round(roe, 1), round(per, 1), round(pbr, 2),
            round(eq_ratio, 1), round(fcf/1e6, 1), round(net_cash/1e6, 1), round(rsi, 1), round(dev, 1), ai_diag[:150]
        ])
        time.sleep(0.5)
    except: continue

# --- 4. 保存実行 ---
if not final_rows:
    print("❌ データが空のため保存を中止しました。")
    exit()

header = ['日付', 'コード', '社名', '戦略', '総合評価', '現在値', '為替ラベル', 'レンジ上限', '利回り', '配当性向', 'ROE', 'PER', 'PBR', '自己資本比率', 'FCF(百万)', 'ネットキャッシュ', 'RSI', '25日乖離', 'AI深層診断']

# A. Spreadsheet (日付名のタブを作成/更新)
gc = gspread.authorize(creds)
sh = gc.open_by_key(SPREADSHEET_ID)
try:
    ws = sh.add_worksheet(title=date_str, rows="1000", cols="25", index=0)
except:
    ws = sh.worksheet(date_str)
ws.clear()
ws.append_row(["【凡例】", "総合評価:100点満点", "FCF:営業+投資CF", "ネットキャッシュ:現預金-負債"])
ws.append_row(header)
ws.append_rows(final_rows)

# B. Drive CSV (既存の「GitHub用」を含むファイルを更新)
drive_service = build('drive', 'v3', credentials=creds)
csv_file = io.BytesIO()
pd.DataFrame(final_rows, columns=header).to_csv(csv_file, index=False, encoding='utf-8-sig')
media = MediaIoBaseUpload(csv_file, mimetype='text/csv', resumable=True)

query = "name contains 'GitHub用' and trashed = false"
files = drive_service.files().list(q=query, fields='files(id, name)').execute().get('files', [])
if files:
    for f in files:
        drive_service.files().update(fileId=f['id'], media_body=media).execute()
        print(f"✅ {f['name']} を {len(final_rows)}件のデータで更新しました。")
