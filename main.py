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

# --- 1. 認証設定 ---
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

# テクニカル指標計算用
def calculate_rsi(series, period=14):
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))

# --- 2. メインロジック ---
df_all = get_latest_jpx_list()
target_list = df_all.head(100).copy()

results = []
now_jst = datetime.utcnow() + timedelta(hours=9)
date_str = now_jst.strftime("%Y-%m-%d")

for i, (idx, row) in enumerate(target_list.iterrows()):
    ticker_str = f"{str(row['コード']).strip()}.T"
    print(f"[{i+1}/100] {row['社名']}...")

    try:
        s = yf.Ticker(ticker_str)
        hist = s.history(period="3mo") # テクニカル計算用に3ヶ月分
        if hist.empty: continue

        inf = s.info
        def g(k, default=0):
            val = inf.get(k, default)
            return val if val is not None else default

        # 基礎データ取得
        current_price = g('currentPrice', hist['Close'].iloc[-1])
        dps = g('dividendRate', 0)
        eps = g('trailingEps', 1)
        roe = g('returnOnEquity', 0) * 100
        per = g('trailingPE', 0)
        pbr = g('priceToBook', 1)
        bps = current_price / pbr if pbr > 0 else current_price
        net_inc = g('netIncomeToCommon', 1)
        ocf = g('operatingCashflow', 0)

        # テクニカル指標の算出
        rsi = calculate_rsi(hist['Close']).iloc[-1]
        ma25 = hist['Close'].rolling(window=25).mean().iloc[-1]
        deviation = ((current_price - ma25) / ma25) * 100 if ma25 else 0

        # 戦略判定
        st_type = "王道" if roe > 10 and 0 < per < 25 else "お宝"

        # レンジ算出
        if st_type == "王道":
            buy_range_top = max(eps * 12, (dps / 0.04) if dps > 0 else 0)
            buy_range_bottom = max(bps * 1.0, (dps / 0.05) if dps > 0 else 0)
        else:
            buy_range_top = max(eps * 10, (dps / 0.045) if dps > 0 else 0)
            buy_range_bottom = max(bps * 0.8, (dps / 0.06) if dps > 0 else 0)

        # AI診断 (フォーマットを厳格に指定)
        prompt = (f"銘柄:{row['社名']}\nROE:{roe:.1f}%, PER:{per}\n"
                  f"必ず以下の形式のみで回答：スコア|為替耐性|診断(15字内)\n"
                  f"スコアは-30から30の数値。")
        response = client.models.generate_content(model='gemini-2.0-flash', contents=prompt)
        res_text = response.text.strip().replace("\n", " ")
        
        # 1. AI回答の分解
        score_ai, fx_label, diag_ai = 0, "不明", res_text
        if "|" in res_text:
            parts = res_text.split("|")
            # 数値だけを抽出する正規表現
            match = re.search(r'(-?\d+)', parts[0])
            score_ai = int(match.group(1)) if match else 0
            fx_label = parts[1].strip() if len(parts) > 1 else "不明"
            diag_ai = parts[2].strip() if len(parts) > 2 else parts[-1]

        # 2. 総合スコア計算 (ベース50点)
        total_score = 50 + score_ai
        if roe > 12: total_score += 10
        if ocf > net_inc: total_score += 5 # 利益の質
        if rsi < 35: total_score += 10    # 売られすぎ
        if deviation < -10: total_score += 10 # 乖離

        res_row = [
            date_str, row['コード'], row['社名'], st_type, int(total_score),
            round(current_price, 1), round(buy_range_top, 1), round(buy_range_bottom, 1),
            round(((current_price / buy_range_top) - 1) * 100, 1), # 乖離率
            round(rsi, 1), round(deviation, 1), # テクニカル
            round(g('dividendYield', 0)*100, 2), round(roe, 1), diag_ai[:20]
        ]
        results.append(res_row)
        time.sleep(1.5)

    except Exception as e:
        print(f"Error: {row['社名']} - {e}")

# --- 3. スプレッドシートへの保存 ---
if results:
    scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
    credentials = Credentials.from_service_account_info(json.loads(GOOGLE_SERVICE_ACCOUNT_JSON), scopes=scopes)
    gc = gspread.authorize(credentials)
    sh = gc.open_by_key(SPREADSHEET_ID)
    worksheet = sh.get_worksheet(0)
    
    existing_data = worksheet.get_all_values()
    if not existing_data:
        header = ['日付', 'コード', '社名', '種別', '総合スコア', '現在値', '上限', '下限', '上限乖離率', 'RSI', '25日乖離', '利回り', 'ROE', '診断']
        worksheet.append_row(header)
    
    worksheet.append_rows(results)
    print(f"✅ {len(results)}件のデータを追記完了")
