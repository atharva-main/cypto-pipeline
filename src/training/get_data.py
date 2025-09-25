import os
import time
import requests
import pandas as pd
from datetime import datetime

# -----------------------------
# Config
# -----------------------------
BASE_URL = "https://api.binance.com/api/v3/klines"
SYMBOLS = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "ADAUSDT"]
INTERVAL = "5m"            # 5-minute candles
MAX_CANDLES = 200_000       # Number of candles per symbol
LIMIT = 1000                # Binance API max per request
DATA_DIR = "./data"
os.makedirs(DATA_DIR, exist_ok=True)


# -----------------------------
# Fetch Klines
# -----------------------------
def fetch_klines(symbol, interval, end_time=None, limit=1000):
    """Fetch klines ending at end_time (ms timestamp)."""
    params = {
        "symbol": symbol,
        "interval": interval,
        "limit": limit
    }
    if end_time:
        params["endTime"] = int(end_time)

    resp = requests.get(BASE_URL, params=params)
    resp.raise_for_status()
    return resp.json()


# -----------------------------
# Download latest N candles
# -----------------------------
def download_latest(symbol, interval, max_candles=MAX_CANDLES, save_dir=DATA_DIR):
    all_klines = []
    end_time = int(datetime.now().timestamp() * 1000)  # Start from now

    while len(all_klines) < max_candles:
        fetch_limit = min(LIMIT, max_candles - len(all_klines))
        data = fetch_klines(symbol, interval, end_time=end_time, limit=fetch_limit)

        if not data:
            break

        all_klines = data + all_klines  # Prepend so newest candles stay last
        end_time = data[0][0] - 1        # Move end_time to earliest candle -1ms

        print(f"{symbol}: Collected {len(all_klines)} / {max_candles} candles...")
        time.sleep(0.25)

        if len(data) < fetch_limit:
            break

    # Convert to DataFrame
    cols = [
        "open_time", "open", "high", "low", "close", "volume",
        "close_time", "quote_asset_volume", "number_of_trades",
        "taker_buy_base", "taker_buy_quote", "ignore"
    ]
    df = pd.DataFrame(all_klines, columns=cols)
    df["symbol"] = symbol
    df["open_time"] = pd.to_datetime(df["open_time"], unit="ms")
    df["close_time"] = pd.to_datetime(df["close_time"], unit="ms")
    numeric_cols = ["open", "high", "low", "close", "volume",
                    "quote_asset_volume", "taker_buy_base", "taker_buy_quote"]
    df[numeric_cols] = df[numeric_cols].astype(float)
    df["number_of_trades"] = df["number_of_trades"].astype(int)

    # Save CSV
    filename = os.path.join(save_dir, f"{symbol}_{interval}.csv")
    df.to_csv(filename, index=False)
    print(f"✅ {symbol}: Saved {len(df)} rows to {filename}")


# -----------------------------
# Main
# -----------------------------
if __name__ == "__main__":
    for sym in SYMBOLS:
        download_latest(sym, INTERVAL, MAX_CANDLES)
