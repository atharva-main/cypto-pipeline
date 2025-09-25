import os
import joblib
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error

# -----------------------------
# Config
# -----------------------------
SYMBOLS = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "ADAUSDT"]
INTERVALS = ["5min", "15min", "1h"]   # train models for these intervals
DATA_DIR = "./data"           
MODEL_BASE_DIR = "./models"   

# -----------------------------
# Load data
# -----------------------------
def load_data(symbol, data_dir=DATA_DIR):
    csv_path = os.path.join(data_dir, f"{symbol}_5m.csv")  # always fetch 5m data
    if not os.path.exists(csv_path):
        print(f"⚠️ CSV not found for {symbol} at {csv_path}")
        return pd.DataFrame()
    
    df = pd.read_csv(csv_path, parse_dates=["open_time"])
    # Rename for consistency
    df = df.rename(columns={"open_time": "trade_time", "close": "price", "volume": "quantity"})
    df = df.sort_values("trade_time")
    return df

# -----------------------------
# Preprocess data
# -----------------------------
def preprocess(df, window):
    df = df.set_index("trade_time")

    agg = df.resample(window).agg({
        "price": ["mean", "min", "max"],
        "quantity": "sum"
    })
    agg.columns = ["avg_price", "min_price", "max_price", "total_volume"]

    # Drop instead of ffill to avoid stale values bias
    agg = agg.dropna()

    # Label = next window avg price
    agg["target"] = agg["avg_price"].shift(-1)
    agg = agg.dropna()

    return agg

# -----------------------------
# Train model
# -----------------------------
def train_model(df):
    X = df[["avg_price", "min_price", "max_price", "total_volume"]]
    y = df["target"]

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, shuffle=False
    )

    model = RandomForestRegressor(
        n_estimators=100,
        max_depth=10,
        random_state=42,
        n_jobs=-1
    )
    model.fit(X_train, y_train)

    preds = model.predict(X_test)
    mse = mean_squared_error(y_test, preds)
    rmse = mse ** 0.5
    return model, rmse

# -----------------------------
# Save model
# -----------------------------
def save_model(model, symbol, window, base_dir=MODEL_BASE_DIR):
    folder = os.path.join(base_dir, symbol)
    os.makedirs(folder, exist_ok=True)
    filename = os.path.join(folder, f"{symbol}_{window}_model.joblib")
    joblib.dump(model, filename)
    print(f"✅ {symbol} {window}: Model saved to {filename}")

# -----------------------------
# Main loop
# -----------------------------
if __name__ == "__main__":
    for symbol in SYMBOLS:
        print(f"\n=== Processing {symbol} ===")
        df = load_data(symbol)

        if df.empty:
            print(f"⚠️ No data found for {symbol}, skipping.")
            continue

        for window in INTERVALS:
            print(f"\n⏳ Training {symbol} at interval {window} ...")
            agg = preprocess(df, window)

            if agg.empty:
                print(f"⚠️ Preprocessing gave empty DataFrame for {symbol} {window}, skipping.")
                continue

            model, rmse = train_model(agg)
            print(f"{symbol} {window}: Validation RMSE = {rmse:.4f}")

            save_model(model, symbol, window)


def predict_next(symbol, window, model_dir="./models", data_dir="./data"):
    # Load model
    model_path = f"{model_dir}/{symbol}/{symbol}_{window}_model.joblib"
    model = joblib.load(model_path)

    # Load and preprocess data
    csv_path = f"{data_dir}/{symbol}_5m.csv"   # raw 5m data
    df = pd.read_csv(csv_path, parse_dates=["open_time"])
    df = df.rename(columns={"open_time": "trade_time", "close": "price", "volume": "quantity"})
    df = df.sort_values("trade_time").set_index("trade_time")

    agg = df.resample(window).agg({
        "price": ["mean", "min", "max"],
        "quantity": "sum"
    })
    agg.columns = ["avg_price", "min_price", "max_price", "total_volume"]

    # Last available features
    latest_features = agg.iloc[-1][["avg_price", "min_price", "max_price", "total_volume"]].values.reshape(1, -1)

    # Prediction
    return model.predict(latest_features)[0]
