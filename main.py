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

def calculate_rsi(series, period=14):
    if len(series) < period + 1: return pd.Series([50] * len(series))
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss.replace(0, np.nan)
    rsi = 100 - (100 / (1 + rs))
    return rsi.fillna(50)

# --- 2. メインロジック ---
df_all = get_latest_jpx_list()
target_list = df_all.head(100).copy()

results = []
now_jst = datetime.utcnow() + timedelta(hours=9)
date_str = now_jst.strftime("%Y-%m-%d")

json_data = json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)
scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
credentials = Credentials.from_service_account_info(json_data, scopes=scopes)
gc = gspread.authorize(credentials)
sh = gc.open_by_key(SPREADSHEET_ID)

prev_prices = {}
try:
    last_sheet = sh.get_worksheet(0)
    if last_sheet and last_sheet.title != date_str:
        records = last_sheet.get_all_records()
        for r in records:
            prev_prices[str(r['コード'])] = r['現在値']
except:
    pass

for i, (idx, row) in enumerate(target_list.iterrows()):
    ticker_str = f"{str(row['コード']).strip()}.T"
    print(f"[{i+1}/100] {row['社名']}...")

    try:
        s = yf.Ticker(ticker_str)
        hist = s.history(period="3mo")
        if hist.empty: continue

        inf = s.info
        def g(k, default=0):
            val = inf.get(k, default)
            return val if (val is not None and np.isfinite(val) if isinstance(val, float) else True) else default

        current_price = g('currentPrice', hist['Close'].iloc[-1])
        prev_p = prev_prices.get(str(row['コード']), 0)
        day_diff = round(current_price - prev_p, 1) if prev_p > 0 else 0
        
        dps = g('dividendRate', 0)
        eps = g('trailingEps', 0)
        roe = g('returnOnEquity', 0) * 100
        per = g('trailingPE', 0)
        pbr = g('priceToBook', 0)
        bps = current_price / pbr if pbr > 0 else current_price
        
        rsi_series = calculate_rsi(hist['Close'])
        rsi = rsi_series.iloc[-1]
        ma25 = hist['Close'].rolling(window=25).mean().iloc[-1]
        deviation = ((current_price - ma25) / ma25) * 100 if (ma25 and ma25 != 0) else 0

        # 戦略名称の変更
        st_type = "クオリティ・グロース" if roe > 10 and 0 < per < 25 else "ディープ・バリュー"
        safe_eps = eps if eps > 0 else (current_price / 15)

        if st_type == "クオリティ・グロース":
            buy_range_top = max(safe_eps * 12, (dps / 0.04) if dps > 0 else 0)
            buy_range_bottom = max(bps * 1.0, (dps / 0.05) if dps > 0 else 0)
        else:
            buy_range_top = max(safe_eps * 10, (dps / 0.045) if dps > 0 else 0)
            buy_range_bottom = max(bps * 0.8, (dps / 0.06) if dps > 0 else 0)

        prompt = (f"銘柄:{row['社名']}\nROE:{roe:.1f}%, PER:{per}, 業種:{row['業種']}\n"
                  f"必ず「スコア|為替|診断」の形式で回答。\n"
                  f"診断は30文字〜50文字程度で、ファンダメンタルズに基づいた投資根拠を述べてください。")
        response = client.models.generate_content(model='gemini-2.0-flash', contents=prompt)
        res_text = response.text.strip()
        
        score_ai = 0
        diag_ai = ""
        if "|" in res_text:
            parts = res_text.split("|")
            match = re.search(r'(-?\d+)', parts[0])
            score_ai = int(match.group(1)) if match else 0
            diag_ai = parts[-1].strip().replace("\n", " ").replace(",", "、")
        else:
            diag_ai = res_text.replace("\n", " ").replace(",", "、")

        total_score = 50 + score_ai
        if roe > 12: total_score += 10
        if rsi < 35: total_score += 10
        if deviation < -10: total_score += 10

        res_row = [
            date_str, row['コード'], row['社名'], st_type, int(total_score),
            round(current_price, 1), day_diff, round(buy_range_top, 1), round(buy_range_bottom, 1),
            round(((current_price / buy_range_top) - 1) * 100, 1) if buy_range_top > 0 else 0,
            round(rsi, 1), round(deviation, 1), round(g('dividendYield', 0)*100, 2),
            round(roe, 1), diag_ai[:100] # 文字数上限に余裕を持たせる
        ]
        
        cleaned_row = [x if not (isinstance(x, float) and not np.isfinite(x)) else 0 for x in res_row]
        results.append(cleaned_row)
        time.sleep(1)

    except Exception as e:
        print(f"Error: {row['社名']} - {e}")

# --- 3. 保存 ---
if results:
    try:
        try:
            worksheet = sh.worksheet(date_str)
        except gspread.exceptions.WorksheetNotFound:
            worksheet = sh.add_worksheet(title=date_str, rows="1000", cols="20", index=0)
            legend = ["【凡例】", "総合スコア: 50点基準。クオリティと割安度で算出", "クオリティ・グロース: 高ROEの優良成長株", "ディープ・バリュー: 資産価値・利回りが極めて割安な株", "上限乖離率: 0%以下で目標レンジ内", "診断: Geminiによるファンダメンタルズ分析"]
            worksheet.append_row(legend)
            header = ['日付', 'コード', '社名', '戦略', '総合スコア', '現在値', '前日比', '上限', '下限', '上限乖離率', 'RSI', '25日乖離', '利回り', 'ROE', 'AI診断']
            worksheet.append_row(header)

        worksheet.append_rows(results)
        print(f"✅ シート「{date_str}」に保存完了")
    except Exception as e:
        print(f"Spreadsheet Error: {e}")
