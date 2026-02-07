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
SPREADSHEET_ID = os.environ.get('SPREADSHEET_ID') # IDでの指定を優先

client = Client(api_key=GEMINI_API_KEY)
json_data = json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)
creds = Credentials.from_service_account_info(json_data, scopes=[
    'https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive'
])

def get_latest_jpx_list():
    url = "https://www.jpx.co.jp/markets/statistics-equities/misc/tvdivq0000001vg2-att/data_j.xls"
    res = requests.get(url).content
    df = pd.read_excel(io.BytesIO(res), engine='xlrd')
    df = df[['コード', '銘柄名', '市場・商品区分', '33業種区分']]
    df.columns = ['コード', '社名', '市場', '業種']
    return df[df['市場'].str.contains('プライム|スタンダード|グロース', na=False)].copy()

# --- 1. スキャン (200社選抜) ---
df_all = get_latest_jpx_list()
print("📡 全3800社から Blue-Chip と Deep Value 候補を抽出...")
tickers = [f"{str(c).strip()}.T" for c in df_all['コード']]
selected_data = []

# 時価総額上位から200社を確定
for i in range(0, 400, 100):
    batch = tickers[i:i+100]
    data = yf.download(batch, period="2d", group_by='ticker', threads=True, progress=False)
    for t in batch:
        try:
            h = data[t]
            if len(h) >= 2:
                p = float(h['Close'].iloc[-1])
                pc = float(h['Close'].iloc[-2])
                chg = ((p - pc) / pc) * 100
                row = df_all[df_all['コード'] == int(t.split('.')[0])].iloc[0]
                selected_data.append({'ticker': t, 'row': row, 'price': p, 'change': f"{chg:+.2f}%"})
        except: continue
    if len(selected_data) >= 200: break

# --- 2. 精密分析 ---
final_rows = []
header = ['日付', 'コード', '社名', '戦略', '総合評価', '現在値', '前日比', '為替ラベル', 'レンジ上限', '利回り', '配当性向', 'ROE', 'PER', 'PBR', '自己資本比率', 'FCF(百万)', 'ネットキャッシュ', 'RSI', '25日乖離', 'AI深層診断']
date_str = (datetime.utcnow() + timedelta(hours=9)).strftime("%Y-%m-%d")

# スプレッドシート接続（ID優先、失敗したら名前で検索）
gc = gspread.authorize(creds)
try:
    sh = gc.open_by_key(SPREADSHEET_ID)
except:
    try: sh = gc.open('Github用')
    except Exception as e:
        print(f"❌ スプレッドシートの接続に失敗: {e}")
        exit()

try:
    ws = sh.add_worksheet(title=date_str, rows="1000", cols="25", index=0)
except:
    ws = sh.worksheet(date_str); ws.clear()
ws.append_row(header)

print("🤖 分析を開始します...")

for i, item in enumerate(selected_data[:200]):
    try:
        s = yf.Ticker(item['ticker'])
        inf = s.info
        hist = s.history(period="3mo")
        
        # 戦略名を専門用語で確定
        strategy = "Blue-Chip Strategy" if i < 100 else "Deep Value Strategy"
        
        # 財務指標ロジック
        roe = float(inf.get('returnOnEquity', 0)) * 100
        yld = float(inf.get('dividendYield', 0)) * 100
        fcf = (float(inf.get('operatingCashflow', 0)) + float(inf.get('investingCashflow', 0))) / 1e6
        net_cash = (float(inf.get('totalCash', 0)) - float(inf.get('totalDebt', 0))) / 1e6
        eps = float(inf.get('trailingEps', 0))
        div_rate = float(inf.get('dividendRate', 0))
        upper_limit = max(eps * 12, div_rate / 0.04)

        # テクニカル
        close = hist['Close']
        rsi, dev = 50.0, 0.0
        if len(close) >= 25:
            delta = close.diff(); g = delta.where(delta > 0, 0).rolling(14).mean(); l = -delta.where(delta < 0, 0).rolling(14).mean()
            rsi = (100 - (100 / (1 + (g/l.replace(0, np.nan))))).iloc[-1]
            dev = ((close.iloc[-1] - close.rolling(25).mean().iloc[-1]) / close.rolling(25).mean().iloc[-1]) * 100

        # AI分析
        prompt = f"銘柄:{item['row']['社名']}, 業種:{item['row']['業種']}, ROE:{roe:.1f}%。為替影響を含め「スコア|為替|診断(40字)」で回答。"
        res = client.models.generate_content(model='gemini-2.0-flash', contents=prompt).text.strip()
        
        ai_s, ai_fx, ai_d = 0, "中立", res
        if "|" in res:
            p = res.split("|"); ai_s = int(re.search(r'(-?\d+)', p[0]).group(1)) if re.search(r'(-?\d+)', p[0]) else 0
            ai_fx, ai_d = p[1].strip(), p[-1].strip()

        final_rows.append([
            date_str, item['row']['コード'], item['row']['社名'], strategy,
            int(50 + ai_s), round(item['price'], 1), item['change'], ai_fx, round(upper_limit, 1),
            round(yld, 2), round(inf.get('payoutRatio', 0)*100, 1), round(roe, 1), round(inf.get('trailingPE', 0), 1), round(inf.get('priceToBook', 0), 2),
            round(inf.get('equityRatio', 0)*100 or 50, 1), round(fcf, 1), round(net_cash, 1), round(rsi, 1), round(dev, 1), ai_d[:150]
        ])
        
        if len(final_rows) % 10 == 0:
            ws.append_rows(final_rows[-10:])
            print(f"✅ {len(final_rows)}/200 完了")
    except: continue

# 同期
drive_service = build('drive', 'v3', credentials=creds)
csv_buf = io.BytesIO()
pd.DataFrame(final_rows, columns=header).to_csv(csv_buf, index=False, encoding='utf-8-sig')
media = MediaIoBaseUpload(csv_buf, mimetype='text/csv', resumable=True)
files = drive_service.files().list(q="name contains 'GitHub用' and trashed = false").execute().get('files', [])
for f in files:
    drive_service.files().update(fileId=f['id'], media_body=media).execute()
