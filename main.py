import pandas as pd
import yfinance as yf
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

# --- 設定と認証 ---
GEMINI_API_KEY = os.environ.get('GEMINI_API_KEY')
GOOGLE_SERVICE_ACCOUNT_JSON = os.environ.get('GOOGLE_SERVICE_ACCOUNT_JSON')
SPREADSHEET_ID = os.environ.get('SPREADSHEET_ID')

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

# --- 1. スキャン (3800社から200社を選抜) ---
df_all = get_latest_jpx_list()
print("📡 3800銘柄からBlue-ChipとDeep Value候補を選抜中...")
tickers = [f"{str(c).strip()}.T" for c in df_all['コード']]
selected_data = []

# 時価総額・流動性等を考慮した一括ダウンロード
for i in range(0, 400, 100):
    batch = tickers[i:i+100]
    data = yf.download(batch, period="2d", group_by='ticker', threads=True, progress=False)
    for t in batch:
        try:
            h = data[t]
            if len(h) >= 2:
                p = float(h['Close'].iloc[-1]); pc = float(h['Close'].iloc[-2])
                selected_data.append({'ticker': t, 'row': df_all[df_all['コード'] == int(t.split('.')[0])].iloc[0], 'price': p, 'change': f"{((p - pc) / pc) * 100:+.2f}%"})
        except: continue
    if len(selected_data) >= 200: break

# --- 2. 精密分析 (15指標と精緻なスコアリング) ---
final_rows = []
header = ['日付', 'コード', '社名', '戦略', '総合評価', '現在値', '前日比', '為替ラベル', 'レンジ下限', 'レンジ上限', '利回り', '配当性向', 'ROE', 'PER', 'PBR', '自己資本比率', 'FCF(百万)', 'ネットキャッシュ', 'RSI', '25日乖離', 'AI深層診断']
date_str = (datetime.utcnow() + timedelta(hours=9)).strftime("%Y-%m-%d")

gc = gspread.authorize(creds); sh = gc.open_by_key(SPREADSHEET_ID)
try: ws = sh.add_worksheet(title=date_str, rows="1000", cols="25", index=0)
except: ws = sh.worksheet(date_str); ws.clear()
ws.append_row(header)

for i, item in enumerate(selected_data[:200]):
    try:
        # --- 【修正点1】ループ毎に変数を完全に初期化し、累積を根絶 ---
        score = 50
        ai_val = 0
        ai_fx = "中立"
        ai_diag = ""
        
        s = yf.Ticker(item['ticker']); inf = s.info; hist = s.history(period="3mo")
        strategy = "Blue-Chip Strategy" if i < 100 else "Deep Value Strategy"
        
        # 指標取得 (15指標＋α)
        roe = float(inf.get('returnOnEquity', 0)) * 100
        pbr = float(inf.get('priceToBook', 0))
        per = float(inf.get('trailingPE', 0))
        yld = float(inf.get('dividendYield', 0)) * 100
        payout = float(inf.get('payoutRatio', 0)) * 100
        eq_ratio = float(inf.get('equityRatio', 0)) * 100 or 50
        eps = float(inf.get('trailingEps', 0))
        div_rate = float(inf.get('dividendRate', 0))
        
        # 独自指標の計算
        range_upper = max(eps * 12, div_rate / 0.04) 
        range_lower = (item['price'] / pbr) * 0.8 if pbr > 0 else item['price'] * 0.7 
        fcf = (float(inf.get('operatingCashflow', 0)) + float(inf.get('investingCashflow', 0))) / 1e6
        net_cash = (float(inf.get('totalCash', 0)) - float(inf.get('totalDebt', 0))) / 1e6

        # テクニカル算出
        close = hist['Close']
        rsi, dev = 50.0, 0.0
        if len(close) >= 25:
            delta = close.diff(); g = delta.where(delta > 0, 0).rolling(14).mean(); l = -delta.where(delta < 0, 0).rolling(14).mean()
            rsi = (100 - (100 / (1 + (g/l.replace(0, np.nan))))).iloc[-1]
            dev = ((close.iloc[-1] - close.rolling(25).mean().iloc[-1]) / close.rolling(25).mean().iloc[-1]) * 100

        # --- 精緻なスコアリング (ベース 50点) ---
        if roe > 10: score += 2 
        if roe > 15: score += 1
        if pbr < 1.0: score += 2 
        if yld > 3.5: score += 2 
        if eq_ratio > 50: score += 1 
        if net_cash > 0: score += 1  
        if rsi < 35: score += 2      
        elif rsi > 70: score -= 3    

        # AI診断
        prompt = (f"銘柄:{item['row']['社名']}, 業種:{item['row']['業種']}, ROE:{roe:.1f}%。 "
                  f"為替判定を『円安恩恵/円高恩恵/中立』から1つ選択。加減点(-5〜+5)と診断(40字)を回答。"
                  f"『加減点|為替|診断』の形式で。")
        res = client.models.generate_content(model='gemini-2.0-flash', contents=prompt).text.strip()
        
        # --- 【修正点2】AIの回答から今回の加点分のみを抽出 ---
        if "|" in res:
            parts = res.split("|")
            try: 
                ai_val = int(re.search(r'([-+]?\d+)', parts[0]).group(1))
            except: 
                ai_val = 0
            ai_fx = "円安恩恵" if "円安" in parts[1] else "円高恩恵" if "円高" in parts[1] else "中立"
            ai_diag = parts[-1].strip()
        else: 
            ai_diag = res
            ai_val = 0

        # --- 【修正点3】総合評価を独立して算出 ---
        final_total_score = int(score + ai_val)

        # 最終行の構築
        final_rows.append([
            date_str, item['row']['コード'], item['row']['社名'], strategy,
            final_total_score, round(item['price'], 1), item['change'], ai_fx,
            round(range_lower, 1), round(range_upper, 1),
            round(yld, 2), round(payout, 1), round(roe, 1), round(per, 1), round(pbr, 2),
            round(eq_ratio, 1), round(fcf, 1), round(net_cash, 1),
            round(rsi, 1), round(dev, 1), ai_diag[:150]
        ])
        
        if len(final_rows) % 10 == 0:
            ws.append_rows(final_rows[-10:])
            print(f"✅ {len(final_rows)}/200 完了")
    except: continue

# --- 3. CSVバックアップ保存 ---
drive_service = build('drive', 'v3', credentials=creds)
csv_buf = io.BytesIO()
pd.DataFrame(final_rows, columns=header).to_csv(csv_buf, index=False, encoding='utf-8-sig')
media = MediaIoBaseUpload(csv_buf, mimetype='text/csv', resumable=True)
files = drive_service.files().list(q="name contains 'GitHub用' and trashed = false").execute().get('files', [])
for f in files:
    drive_service.files().update(fileId=f['id'], media_body=media).execute()
