import pandas as pd
import yfinance as yf
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
import time

# --- 設定エリア ---
# Google Sheets API設定 (JSONキーファイルが必要)
JSON_KEY_FILE = 'path/to/your/service_account_key.json' 
FOLDER_NAME = "Colog_GitHub用"
SOURCE_SS_NAME = "Github用"
DEST_SS_NAME = "ハイスコア深層分析"

# 家計予算設定 (Saved Informationより)
SAVINGS_DEFENSE_FUND = 80000 # 毎月の防衛資金

def get_ss_client():
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name(JSON_KEY_FILE, scope)
    return gspread.authorize(creds)

def fetch_deep_data(ticker_symbol):
    """
    Step 1 & 2 & 3 のロジックを統合したデータ取得
    """
    ticker = yf.Ticker(f"{ticker_symbol}.T")
    info = ticker.info
    
    # --- Step 1: 財務深掘り (F-Score等) ---
    # 簡易版Fスコア ( yfinanceで取得可能な範囲 )
    f_score = 0
    try:
        bs = ticker.balance_sheet
        is_stmt = ticker.financials
        cf = ticker.cashflow
        
        # 1. 当期純利益 > 0
        if is_stmt.loc['Net Income'].iloc[0] > 0: f_score += 1
        # 2. 営業CF > 0
        if cf.loc['Operating Cash Flow'].iloc[0] > 0: f_score += 1
        # 3. 営業CF > 純利益
        if cf.loc['Operating Cash Flow'].iloc[0] > is_stmt.loc['Net Income'].iloc[0]: f_score += 1
        # 4. 自己資本比率 (簡易チェック)
        total_assets = bs.loc['Total Assets'].iloc[0]
        equity = bs.loc['Stockholders Equity'].iloc[0]
        if (equity / total_assets) > (bs.loc['Stockholders Equity'].iloc[1] / bs.loc['Total Assets'].iloc[1]): f_score += 1
    except:
        f_score = "N/A"

    # --- Step 2: 需給 (出来高変化) ---
    hist = ticker.history(period="1mo")
    vol_change = "N/A"
    if len(hist) > 20:
        recent_vol = hist['Volume'].tail(3).mean()
        avg_vol = hist['Volume'].mean()
        vol_change = round(recent_vol / avg_vol, 2)

    # --- Step 3: ニュース簡易フィルター ---
    news = ticker.news
    bad_news_flag = "なし"
    bad_words = ["不祥事", "下方修正", "提訴", "減配", "赤字転落"]
    for n in news[:5]:
        if any(word in n['title'] for word in bad_words):
            bad_news_flag = "要警戒"
            break

    return f_score, vol_change, bad_news_flag

def main():
    client = get_ss_client()
    
    # 1. 元データ取得 (Github用)
    source_ss = client.open(SOURCE_SS_NAME)
    source_sheet = source_ss.get_worksheet(0) # 一番左（最新）のタブ
    data = pd.DataFrame(source_sheet.get_all_records())
    
    # 2. 分析対象(20銘柄)の抽出
    # 総合評価60以上、Blue-Chip上位10、Deep Value上位10
    top_blue = data[(data['戦略'] == 'Blue-Chip Strategy') & (data['総合評価'] >= 60)].nlargest(10, '総合評価')
    top_value = data[(data['戦略'] == 'Deep Value Strategy') & (data['総合評価'] >= 60)].nlargest(10, '総合評価')
    target_df = pd.concat([top_blue, top_value])

    results = []
    
    # 3. 各銘柄を深掘り
    for index, row in target_df.iterrows():
        print(f"Analyzing: {row['社名']}...")
        f_score, vol_change, news_status = fetch_deep_data(row['コード'])
        
        # 最終判定ロジック
        judgment = "WAIT"
        if f_score != "N/A" and f_score >= 3 and news_status == "なし":
            if row['RSI'] < 70: judgment = "GO"
            
        results.append({
            "日付": datetime.now().strftime('%Y-%m-%d'),
            "コード": row['コード'],
            "社名": row['社名'],
            "戦略": row['戦略'],
            "総合スコア": row['総合評価'],
            "Fスコア": f_score,
            "出来高変化率": vol_change,
            "ニュース警告": news_status,
            "最終判定": judgment,
            "備考": f"RSI:{row['RSI']}, 25日乖離:{row['25日乖離']}"
        })
        time.sleep(1) # API制限回避

    # 4. 結果の書き込み (ハイスコア深層分析)
    dest_ss = client.open(DEST_SS_NAME)
    new_sheet_name = datetime.now().strftime('%Y-%m-%d')
    
    # 同名のシートがあれば削除して作り直す（上書き）
    try:
        old_ws = dest_ss.worksheet(new_sheet_name)
        dest_ss.del_worksheet(old_ws)
    except:
        pass
    
    new_ws = dest_ss.add_worksheet(title=new_sheet_name, rows=100, cols=20)
    output_df = pd.DataFrame(results)
    new_ws.update([output_df.columns.values.tolist()] + output_df.values.tolist())

    print("深層分析が完了し、スプレッドシートを更新しました。")

if __name__ == "__main__":
    main()
