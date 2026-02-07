import os
import json
import pandas as pd
import yfinance as yf
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime, timedelta
import time
import numpy as np

# --- 1. main.py と完全に一致させた認証設定 ---
GOOGLE_SERVICE_ACCOUNT_JSON = os.environ.get('GOOGLE_SERVICE_ACCOUNT_JSON')
SPREADSHEET_ID = os.environ.get('SPREADSHEET_ID')

# JSONの読み込みと認証
json_data = json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)
creds = Credentials.from_service_account_info(json_data, scopes=[
    'https://www.googleapis.com/auth/spreadsheets', 
    'https://www.googleapis.com/auth/drive'
])
gc = gspread.authorize(creds)

def analyze_ticker(ticker_symbol):
    """財務・需給の深掘り分析"""
    try:
        ticker = yf.Ticker(f"{ticker_symbol}.T")
        inf = ticker.info
        
        # 財務健全性スコア (0-3点)
        f_score = 0
        # 営業CFがプラスか
        if float(inf.get('operatingCashflow', 0)) > 0: f_score += 1
        # 現金が負債より多いか
        if float(inf.get('totalCash', 0)) > float(inf.get('totalDebt', 0)): f_score += 1
        # 自己資本比率が50%以上か
        if float(inf.get('bookValue', 0)) > 0: # 簡易判定
            f_score += 1
        
        # 直近の出来高変化
        hist = ticker.history(period="1mo")
        vol_ratio = 1.0
        if len(hist) > 10:
            vol_ratio = round(hist['Volume'].tail(3).mean() / hist['Volume'].mean(), 2)
            
        return f_score, vol_ratio
    except:
        return 0, 1.0

def main():
    print("🚀 深層分析エンジン起動...")
    sh = gc.open_by_key(SPREADSHEET_ID)
    
    # 2. main.pyが作った最新のワークシート(一番左のタブ)を取得
    # main.pyは index=0 でシートを追加しているので、一番左が最新です
    source_ws = sh.get_worksheet(0)
    print(f"📊 データ読み込み元: {source_ws.title}")
    
    # 全データを取得してDataFrame化
    raw_data = pd.DataFrame(source_ws.get_all_records())
    
    # 3. 分析対象の絞り込み (総合評価が高い上位20銘柄)
    # 総合評価でソート
    top_stocks = raw_data.sort_values('総合評価', ascending=False).head(20)
    
    results = []
    date_str = (datetime.utcnow() + timedelta(hours=9)).strftime("%Y-%m-%d")

    for _, row in top_stocks.iterrows():
        code = row['コード']
        print(f"🔎 銘柄分析中: {code} {row['社名']}")
        
        f_score, v_ratio = analyze_ticker(code)
        
        # 判定ロジック
        # 財務が良く(2点以上)、RSIが過熱していない(70未満)ならGO
        judgment = "🔥強い買い" if (f_score >= 2 and row['RSI'] < 70) else "⚡️様子見"
        if row['RSI'] < 35: judgment = "💎絶好の仕込み時"

        results.append([
            date_str, code, row['社名'], row['戦略'], row['総合評価'],
            f_score, v_ratio, row['RSI'], row['AI深層診断'], judgment
        ])
        time.sleep(1) # API制限対策

    # 4. 「ハイスコア深層分析」スプレッドシートへの書き込み
    # ※同じスプレッドシート内に「深層分析結果」という名前の別シートを作るか、
    # もし別ファイルにするならここを書き換えますが、まずは同じファイル内に作成します。
    
    target_sheet_name = f"深層分析_{date_str}"
    try:
        target_ws = sh.add_worksheet(title=target_sheet_name, rows="100", cols="15")
    except:
        target_ws = sh.worksheet(target_sheet_name)
        target_ws.clear()

    header = ['分析日', 'コード', '社名', '戦略', '元スコア', '財務スコア(0-3)', '出来高変化率', 'RSI', 'AI診断(引用)', '最終判定']
    target_ws.append_row(header)
    target_ws.append_rows(results)
    
    print(f"✅ 全工程完了！シート「{target_sheet_name}」を確認してください。")

if __name__ == "__main__":
    main()
