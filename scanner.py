
import os
import json
import time
import requests
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed

BASE = "https://api.india.delta.exchange"
PRODUCT_URL = BASE + "/v2/products"
CANDLE_URL = BASE + "/v2/history/candles"

TIMEFRAMES = {
    "1h": 3600,
    "4h": 14400,
    "1d": 86400
}

BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]

STATE_FILE = "scanner_state.json"
WORKERS = 8


def telegram(message):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    try:
        r = requests.post(
            url,
            data={"chat_id": CHAT_ID, "text": message},
            timeout=20
        )
        r.raise_for_status()
        return r.json().get("ok", False)
    except Exception as e:
        print("Telegram error:", e)
        return False


def get_products():
    products = []
    after = None

    while True:
        params = {"page_size": 100}
        if after:
            params["after"] = after

        r = requests.get(PRODUCT_URL, params=params, timeout=30)
        r.raise_for_status()
        data = r.json()

        page = data.get("result", [])
        products.extend(page)

        after = data.get("meta", {}).get("after")
        if not after or not page:
            break

    selected = []
    for p in products:
        kind = str(p.get("product_type", "")).lower()
        status = str(p.get("state", "live")).lower()

        if kind in ("spot", "perpetual_futures") and status == "live":
            selected.append(p["symbol"])

    return sorted(set(selected))


def get_candles(symbol, timeframe):
    now = int(time.time())
    seconds = TIMEFRAMES[timeframe]

    params = {
        "resolution": timeframe,
        "symbol": symbol,
        "start": now - seconds * 120,
        "end": now
    }

    r = requests.get(CANDLE_URL, params=params, timeout=25)
    r.raise_for_status()

    rows = r.json().get("result", [])
    if not rows:
        return None

    df = pd.DataFrame(rows)
    needed = ["time", "open", "high", "low", "close"]

    if not all(c in df.columns for c in needed):
        return None

    for c in needed:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    df = df.dropna(subset=needed)
    df = df.sort_values("time").drop_duplicates("time")

    # Ignore candle that has not fully closed
    df = df[df["time"] + seconds <= now]

    if len(df) < 25:
        return None

    df["ema20"] = df["close"].ewm(
        span=20, adjust=False
    ).mean()

    return df.reset_index(drop=True)


def check_item(item):
    symbol, timeframe = item
    try:
        df = get_candles(symbol, timeframe)
        if df is None or len(df) < 25:
            return None

        previous = df.iloc[-2]
        latest = df.iloc[-1]

        return {
            "key": symbol + "|" + timeframe,
            "symbol": symbol,
            "timeframe": timeframe,
            "timestamp": int(latest["time"]),
            "previous": previous,
            "latest": latest
        }
    except Exception as e:
        print("Candle error:", symbol, timeframe, str(e))
        return None


def buy_candle(c):
    return c["low"] <= c["ema20"] and c["close"] > c["ema20"]


def sell_candle(c):
    return c["high"] >= c["ema20"] and c["close"] < c["ema20"]


def load_state():
    if not os.path.exists(STATE_FILE):
        return {}
    try:
        with open(STATE_FILE, "r") as f:
            return json.load(f)
    except Exception:
        return {}


def save_state(state):
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)


def main():
    state = load_state()

    print("Getting Delta products...")
    symbols = get_products()
    print("Spot + Perpetual products:", len(symbols))

    tasks = [
        (symbol, tf)
        for symbol in symbols
        for tf in TIMEFRAMES
    ]

    results = []
    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        futures = [pool.submit(check_item, item) for item in tasks]
        for future in as_completed(futures):
            result = future.result()
            if result:
                results.append(result)

    # First run: establish baseline, no old alerts
    if not state:
        for result in results:
            state[result["key"]] = result["timestamp"]
        save_state(state)
        print("First run complete. Baseline saved; no old alerts sent.")
        return

    for result in results:
        key = result["key"]
        timestamp = result["timestamp"]
        old_timestamp = int(state.get(key, 0))

        if timestamp <= old_timestamp:
            continue

        previous = result["previous"]
        latest = result["latest"]

        signal = None
        if buy_candle(previous) and buy_candle(latest):
            signal = "BUY"
        elif sell_candle(previous) and sell_candle(latest):
            signal = "SELL"

        if signal:
            message = (
                "🔔 DELTA EMA20 ALERT\n\n"
                f"Signal: {signal}\n"
                f"Symbol: {result['symbol']}\n"
                f"Timeframe: {result['timeframe']}\n"
                f"Close: {latest['close']}\n"
                f"EMA20: {latest['ema20']}\n\n"
                "Two consecutive closed candles touched EMA20 "
                "and closed on the correct side.\n"
                "Alert only — no automatic trade."
            )

            if telegram(message):
                print("Alert sent:", signal, key)
                state[key] = timestamp
        else:
            state[key] = timestamp

    save_state(state)
    print("Scan completed successfully.")


if __name__ == "__main__":
    main()
  
