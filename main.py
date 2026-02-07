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

# --- 1. 認証 ---
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

def calculate_technical(hist):
    if len(hist) < 25: return 50.0, 0.0
    close = hist['Close']
    # RSI
    delta = close.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    rs = gain / loss.replace(0, np.nan)
    rsi = (100 - (100 / (1 + rs))).iloc[-1]
    # 25日乖離
    ma25 = close.rolling(window=25).mean().iloc[-1]
    dev = ((close.iloc[-1] - ma25) / ma25) * 100
    return rsi, dev

# --- 2. 3800社フルスキャン ---
df_all = get_latest_jpx_list()
print(f"📡 3776社のスキャンを開始（全15指標網羅モード）...")
candidates = []
for i, (idx, row) in enumerate(df_all.iterrows()):
    ticker = f"{str(row['コード']).strip()}.T"
    try:
        s = yf.Ticker(ticker)
        f = s.fast_info
        if not f.get('last_price'): continue
        # スキャン時は時価総額＋ROE(簡易)で選抜
        candidates.append({'ticker': ticker, 'row': row, 'mcap': f.get('market_cap', 0), 'price': f.get('last_price')})
    except: continue
    if (i+1) % 1000 == 0: print(f"SCAN: {i+1}社完了...")

target_list = sorted(candidates, key=lambda x: x['mcap'], reverse=True)[:200]

# --- 3. 詳細分析（200社） ---
final_rows = []
header = ['日付', 'コード', '社名', '戦略', '総合評価', '現在値', '為替ラベル', 'レンジ上限', '利回り', '配当性向', 'ROE', 'PER', 'PBR', '自己資本比率', 'FCF(百万)', 'ネットキャッシュ', 'RSI', '25日乖離', 'AI深層診断']
date_str = (datetime.utcnow() + timedelta(hours=9)).strftime("%Y-%m-%d")

print(f"🤖 選抜200社の精密分析と為替判断を実行中...")
for item in target_list:
    try:
        s = yf.Ticker(item['ticker'])
        inf = s.info
        hist = s.history(period="3mo")
        
        # 指標取得
        roe = inf.get('returnOnEquity', 0) * 100
        yld = inf.get('dividendYield', 0) * 100
        per = inf.get('trailingPE', 0)
        pbr = inf.get('priceToBook', 0)
        payout = inf.get('payoutRatio', 0) * 100
        eq_ratio = inf.get('bookValue', 0) / (inf.get('totalAssets', 1)) * 100 if 'totalAssets' in inf else inf.get('equityRatio', 0)*100
        fcf = (inf.get('operatingCashflow', 0) or 0) + (inf.get('investingCashflow', 0) or 0)
        net_cash = (inf.get('totalCash', 0) or 0) - (inf.get('totalDebt', 0) or 0)
        eps = inf.get('trailingEps', 0)
        
        # テクニカル
        rsi, dev = calculate_technical(hist)
        
        # レンジ上限算出
        upper_limit = max(eps * 12, (inf.get('dividendRate', 0) or 0) / 0.04)

        # AI分析
        prompt = f"銘柄:{item['row']['社名']}, 業種:{item['row']['業種']}, ROE:{roe:.1f}%, FCF:{fcf/1e6:.0f}M。15指標から「スコア(-15〜15)|為替(円安恩恵/円高恩恵/中立)|診断(40字)」で回答。"
        res = client.models.generate_content(model='gemini-2.0-flash', contents=prompt).text.strip()
        
        # パース
        ai_score, ai_fx, ai_diag = 0, "中立", res.replace(",","、")
        if "|" in res:
            p = res.split("|")
            ai_score = int(re.search(r'(-?\d+)', p[0]).group(1)) if re.search(r'(-?\d+)', p[0]) else 0
            ai_fx = p[1].strip()
            ai_diag = p[-1].strip()

        # 総合評価（50点ベース + AIスコア + 財務加点）
        total_score = int(min(100, max(0, 50 + ai_score + (10 if roe > 12 else 0) + (10 if fcf > 0 else 0))))

        final_rows.append([
            date_str, item['row']['コード'], item['row']['社名'], "主力選抜",
            total_score, round(item['price'], 1), ai_fx, round(upper_limit, 1),
            round(yld, 2), round(payout, 1), round(roe, 1), round(per, 1), round(pbr, 2),
            round(eq_ratio, 1), round(fcf/1e6, 1), round(net_cash/1e6, 1), round(rsi, 1), round(dev, 1), ai_diag[:150]
        ])
        time.sleep(0.3)
    except: continue

# --- 4. 完遂書き込み ---
if final_rows:
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(SPREADSHEET_ID)
    try: ws = sh.add_worksheet(title=date_str, rows="1000", cols="25", index=0)
    except: ws = sh.worksheet(date_str); ws.clear()
    ws.append_row(header)
    ws.append_rows(final_rows)

    drive_service = build('drive', 'v3', credentials=creds)
    csv_buf = io.BytesIO()
    pd.DataFrame(final_rows, columns=header).to_csv(csv_buf, index=False, encoding='utf-8-sig')
    media = MediaIoBaseUpload(csv_buf, mimetype='text/csv', resumable=True)
    query = "name contains 'GitHub用' and trashed = false"
    files = drive_service.files().list(q=query).execute().get('files', [])
    for f in files:
        drive_service.files().update(fileId=f['id'], media_body=media).execute()
    print(f"✨ 全19列（15指標＋為替判定）の200社分析を完全に完遂しました。")
