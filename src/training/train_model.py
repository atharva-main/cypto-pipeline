import os
import joblib # type: ignore
import pandas as pd # type: ignore
from datetime import timedelta
from sklearn.ensemble import RandomForestRegressor # type: ignore
from sklearn.model_selection import train_test_split # type: ignore
from sklearn.metrics import mean_squared_error # type: ignore

MODEL_PATH = os.getenv("MODEL_PATH", "./models/latest_model.joblib")

# -----------------------------
# Load data from CSV
# -----------------------------
def load_data(csv_path, symbol="BTCUSDT"):
    df = pd.read_csv(csv_path, parse_dates=["trade_time"])
    df = df[df["symbol"] == symbol].copy()
    df = df.sort_values("trade_time")
    return df


# -----------------------------
# Feature Engineering
# -----------------------------
def preprocess(df, window="5min"):
    df.set_index("trade_time", inplace=True)

    agg = df.resample(window).agg({
        "price": ["mean", "min", "max"],
        "quantity": "sum"
    })
    agg.columns = ["avg_price", "min_price", "max_price", "total_volume"]

    agg = agg.fillna(method="ffill")

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
    rmse = mean_squared_error(y_test, preds, squared=False)
    print(f"Validation RMSE: {rmse:.4f}")

    return model


# -----------------------------
# Save model
# -----------------------------
def save_model(model, path=MODEL_PATH):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    joblib.dump(model, path)
    print(f"Model saved to {path}")


# -----------------------------
# Main
# -----------------------------
if __name__ == "__main__":
    csv_path = os.getenv("TRAINING_CSV", "btc_trades.csv")
    symbol = os.getenv("TRAIN_SYMBOL", "BTCUSDT")

    print(f"Loading data for {symbol} from {csv_path}...")
    df = load_data(csv_path, symbol)
    if df.empty:
        raise ValueError("No data found in CSV for training.")

    print("Preprocessing...")
    agg = preprocess(df)

    print("Training model...")
    model = train_model(agg)

    save_model(model, MODEL_PATH)
