# (前略：インポート部分はそのまま)

# --- 中盤：メインロジック内の PER/PBR 計算部分を修正 ---
        # 基礎データ取得
        current_price = g('currentPrice', hist['Close'].iloc[-1])
        dps = g('dividendRate', 0)
        eps = g('trailingEps', 1)
        roe = g('returnOnEquity', 0) * 100
        per = g('trailingPE', 0)
        pbr = g('priceToBook', 0) # 0に変更

        # BPSの計算を安全に
        if pbr > 0:
            bps = current_price / pbr
        else:
            bps = current_price # PBRが取れない場合は現在値を代用（安全策）

        # ネットキャッシュ等の計算を安全に
        net_inc = g('netIncomeToCommon', 1)
        ocf = g('operatingCashflow', 0)
        
        # 0除算を徹底回避するPER計算
        safe_per = per if per > 0 else 15 # 不明な場合は標準的な15を仮定
        safe_eps = eps if eps != 0 else 1

        # --- 戦略判定 ---
        st_type = "王道" if roe > 10 and 0 < per < 25 else "お宝"

        # レンジ算出 (epsやdpsが0でも止まらないように修正)
        if st_type == "王道":
            buy_range_top = max(safe_eps * 12, (dps / 0.04) if dps > 0 else 0)
            buy_range_bottom = max(bps * 1.0, (dps / 0.05) if dps > 0 else 0)
        else:
            buy_range_top = max(safe_eps * 10, (dps / 0.045) if dps > 0 else 0)
            buy_range_bottom = max(bps * 0.8, (dps / 0.06) if dps > 0 else 0)

# (後略：スプレッドシート保存部分はそのまま)
