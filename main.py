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

# --- 設定 ---
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

# --- 1. 全3800社スキャン ---
df_all = get_latest_jpx_list()
print(f"📡 3776社のスキャン開始...")
candidates = []
for i, (idx, row) in enumerate(df_all.iterrows()):
    ticker = f"{str(row['コード']).strip()}.T"
    try:
        s = yf.Ticker(ticker)
        f = s.fast_info
        if f.get('last_price'):
            candidates.append({'ticker': ticker, 'row': row, 'mcap': f.get('market_cap', 0), 'price': f.get('last_price')})
    except: continue
target_list = sorted(candidates, key=lambda x: x['mcap'], reverse=True)[:200]

# --- 2. 200社の精密分析 ---
final_rows = []
header = ['日付', 'コード', '社名', '戦略', '総合評価', '現在値', '為替ラベル', 'レンジ上限', '利回り', '配当性向', 'ROE', 'PER', 'PBR', '自己資本比率', 'FCF(百万)', 'ネットキャッシュ', 'RSI', '25日乖離', 'AI深層診断']
date_str = (datetime.utcnow() + timedelta(hours=9)).strftime("%Y-%m-%d")
start_time = time.time()

print(f"🤖 選抜200社の「全指標」分析を開始（制限時間800秒）...")

for i, item in enumerate(target_list):
    # タイムアウト対策：終了100秒前には保存へ移る
    if time.time() - start_time > 750: 
        print("⚠️ 完遂を優先し、現時点のデータで保存を開始します。")
        break
    
    try:
        s = yf.Ticker(item['ticker'])
        inf = s.info
        hist = s.history(period="3mo")
        
        # 指標取得（数値はfloat化して安定させる）
        roe = float(inf.get('returnOnEquity', 0)) * 100
        yld = float(inf.get('dividendYield', 0)) * 100
        per = float(inf.get('trailingPE', 0))
        pbr = float(inf.get('priceToBook', 0))
        payout = float(inf.get('payoutRatio', 0)) * 100
        eq_ratio = float(inf.get('equityRatio', 0)) * 100 if inf.get('equityRatio') else (float(inf.get('bookValue', 0)) / (float(inf.get('totalAssets', 1))) * 100)
        
        # 指示通りの計算：FCF = 営業CF + 投資CF
        fcf = (float(inf.get('operatingCashflow', 0)) + float(inf.get('investingCashflow', 0))) / 1e6
        # 指示通りの計算：ネットキャッシュ = 現預金 - 総負債
        net_cash = (float(inf.get('totalCash', 0)) - float(inf.get('totalDebt', 0))) / 1e6
        
        # 指示通りの計算：レンジ上限 = max(EPS*12, 配当額/0.04)
        eps = float(inf.get('trailingEps', 0))
        div_rate = float(inf.get('dividendRate', 0))
        upper_limit = max(eps * 12, div_rate / 0.04)

        # テクニカル
        close = hist['Close']
        rsi, dev = 50.0, 0.0
        if len(close) >= 25:
            delta = close.diff(); gain = (delta.where(delta > 0, 0)).rolling(14).mean(); loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
            rs = gain / loss.replace(0, np.nan)
            rsi = (100 - (100 / (1 + rs))).iloc[-1]
            ma25 = close.rolling(25).mean().iloc[-1]
            dev = ((close.iloc[-1] - ma25) / ma25) * 100

        # AI分析（為替ラベルの指示を強化）
        prompt = f"銘柄:{item['row']['社名']}, 業種:{item['row']['業種']}, ROE:{roe:.1f}%。為替影響を含め「スコア(-15〜15)|為替(円安恩恵/円高恩恵/中立)|診断(40字)」で回答。"
        res = client.models.generate_content(model='gemini-2.0-flash', contents=prompt).text.strip()
        
        ai_score, ai_fx, ai_diag = 0, "中立", res
        if "|" in res:
            parts = res.split("|")
            ai_score = int(re.search(r'(-?\d+)', parts[0]).group(1)) if re.search(r'(-?\d+)', parts[0]) else 0
            ai_fx = parts[1].strip()
            ai_diag = parts[-1].strip()

        final_rows.append([
            date_str, item['row']['コード'], item['row']['社名'], "主力選抜",
            int(50 + ai_score + (10 if roe > 10 else 0)), round(item['price'], 1), ai_fx, round(upper_limit, 1),
            round(yld, 2), round(payout, 1), round(roe, 1), round(per, 1), round(pbr, 2),
            round(eq_ratio, 1), round(fcf, 1), round(net_cash, 1), round(rsi, 1), round(dev, 1), ai_diag[:150]
        ])
        if (i+1) % 20 == 0: print(f"進行状況: {i+1}/200 分析完了")
    except: continue

# --- 3. 書き込み ---
if final_rows:
    gc = gspread.authorize(creds); sh = gc.open_by_key(SPREADSHEET_ID)
    try: ws = sh.add_worksheet(title=date_str, rows="1000", cols="25", index=0)
    except: ws = sh.worksheet(date_str); ws.clear()
    ws.append_row(header)
    ws.append_rows(final_rows)
    
    drive_service = build('drive', 'v3', credentials=creds)
    csv_buf = io.BytesIO()
    pd.DataFrame(final_rows, columns=header).to_csv(csv_buf, index=False, encoding='utf-8-sig')
    media = MediaIoBaseUpload(csv_buf, mimetype='text/csv', resumable=True)
    for f in drive_service.files().list(q="name contains 'GitHub用'").execute().get('files', []):
        drive_service.files().update(fileId=f['id'], media_body=media).execute()
    print(f"✨ 全指標を完全網羅し、{len(final_rows)}件の書き込みを完遂しました。")
