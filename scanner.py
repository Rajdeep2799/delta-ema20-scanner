
import os
import json
import time
import requests
import pandas as pd
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

BASE_URL = "https://api.india.delta.exchange"
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

TIMEFRAMES = {
    "1h": 3600,
    "4h": 14400,
    "1d": 86400
}

STATE_FILE = "scanner_state.json"
MAX_CANDLES = 150
WORKERS = 6

session = requests.Session()
session.headers.update({"Accept": "application/json"})


def load_state():
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r") as f:
                return json.load(f)
        except Exception:
            pass
    return {}


def save_state(state):
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)


def telegram(message):
    if not BOT_TOKEN or not CHAT_ID:
        raise RuntimeError("Telegram secrets are missing")

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"

    response = requests.post(
        url,
        data={"chat_id": CHAT_ID, "text": message},
        timeout=20
    )
    response.raise_for_status()

    result = response.json()
    if not result.get("ok"):
        raise RuntimeError("Telegram message failed")


def get_products():
    print("Getting Delta products...")

    products = []
    after = None
    seen_cursors = set()

    while True:
        params = {
            "contract_types": "spot,perpetual_futures",
            "states": "live",
            "page_size": 100
        }

        if after:
            params["after"] = after

        response = session.get(
            f"{BASE_URL}/v2/products",
            params=params,
            timeout=25
        )
        response.raise_for_status()

        data = response.json()

        if not data.get("success"):
            raise RuntimeError(f"Products API error: {data}")

        page = data.get("result", [])
        products.extend(page)

        meta = data.get("meta") or {}
        next_cursor = meta.get("after")

        if not next_cursor or next_cursor in seen_cursors:
            break

        seen_cursors.add(next_cursor)
        after = next_cursor

    allowed = {"spot", "perpetual_futures"}
    live_products = []

    for p in products:
        contract = p.get("contract_type") or p.get("product_type")
        state = p.get("state", "live")

        if contract in allowed and state == "live":
            if p.get("symbol"):
                live_products.append(p)

    # Remove duplicate symbols
    unique = {}
    for p in live_products:
        unique[p["symbol"]] = p

    result = list(unique.values())

    print("Total products received:", len(products))
    print("Spot + Perpetual products:", len(result))

    if not result:
        sample = products[0] if products else {}
        raise RuntimeError(
            "No eligible products found. "
            f"First product data: {sample}"
        )

    return result


def get_candles(symbol, resolution, seconds):
    now = int(time.time())

    start = now - (MAX_CANDLES + 5) * seconds

    response = session.get(
        f"{BASE_URL}/v2/history/candles",
        params={
            "resolution": resolution,
            "symbol": symbol,
            "start": start,
            "end": now
        },
        timeout=25
    )
    response.raise_for_status()

    data = response.json()

    if not data.get("success"):
        raise RuntimeError(f"Candle API error for {symbol}: {data}")

    candles = data.get("result", [])

    if not candles:
        return None

    df = pd.DataFrame(candles)

    required = ["time", "open", "high", "low", "close"]
    if not all(col in df.columns for col in required):
        return None

    df = df[required].copy()

    for col in ["time", "open", "high", "low", "close"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.dropna()
    df = df.sort_values("time")
    df = df.drop_duplicates(subset=["time"])

    # Ignore candles that have not fully closed
    df = df[df["time"] + seconds <= now]

    if len(df) < 25:
        return None

    df["ema20"] = df["close"].ewm(
        span=20,
        adjust=False
    ).mean()

    return df.reset_index(drop=True)


def check_signal(df):
    if df is None or len(df) < 2:
        return None

    first = df.iloc[-2]
    second = df.iloc[-1]

    # BUY: wick touches EMA, full body remains above EMA
    buy_first = (
        first["low"] <= first["ema20"]
        and first["open"] > first["ema20"]
        and first["close"] > first["ema20"]
    )

    buy_second = (
        second["low"] <= second["ema20"]
        and second["open"] > second["ema20"]
        and second["close"] > second["ema20"]
    )

    if buy_first and buy_second:
        return "BUY"

    # SELL: wick touches EMA, full body remains below EMA
    sell_first = (
        first["high"] >= first["ema20"]
        and first["open"] < first["ema20"]
        and first["close"] < first["ema20"]
    )

    sell_second = (
        second["high"] >= second["ema20"]
        and second["open"] < second["ema20"]
        and second["close"] < second["ema20"]
    )

    if sell_first and sell_second:
        return "SELL"

    return None


def timeframe_due(tf, now):
    # Delta candle boundaries are checked in UTC
    if tf == "1h":
        return now.minute <= 10

    if tf == "4h":
        return now.hour % 4 == 0 and now.minute <= 10

    if tf == "1d":
        return now.hour == 0 and now.minute <= 10

    return False


def scan_one(product, tf, seconds):
    symbol = product["symbol"]

    df = get_candles(symbol, tf, seconds)

    if df is None:
        return None

    signal = check_signal(df)

    return {
        "symbol": symbol,
        "timeframe": tf,
        "signal": signal,
        "candle_time": int(df.iloc[-1]["time"])
    }


def main():
    print("Starting Delta EMA20 Scanner...")

    state = load_state()
    products = get_products()

    now = datetime.now(timezone.utc)

    jobs = []

    for tf, seconds in TIMEFRAMES.items():
        if timeframe_due(tf, now):
            jobs.append((tf, seconds))

    if not jobs:
        print("No timeframe candle close window right now.")
        return

    tasks = []

    for product in products:
        for tf, seconds in jobs:
            tasks.append((product, tf, seconds))

    print("Products:", len(products))
    print("Timeframes due:", [x[0] for x in jobs])
    print("Total candle checks:", len(tasks))

    if not state.get("initialized"):
        print("First run: saving baseline. No old alerts.")

        with ThreadPoolExecutor(max_workers=WORKERS) as executor:
            futures = {
                executor.submit(
                    scan_one, product, tf, seconds
                ): (product, tf)
                for product, tf, seconds in tasks
            }

            for future in as_completed(futures):
                product, tf = futures[future]
                key = f"{product['symbol']}:{tf}"

                try:
                    result = future.result()
                    if result:
                        state[key] = result["candle_time"]
                except Exception as e:
                    print("Baseline error:", key, str(e))

        state["initialized"] = True
        save_state(state)

        print("First run complete. Baseline saved; no old alerts sent.")
        return

    alerts = []

    with ThreadPoolExecutor(max_workers=WORKERS) as executor:
        futures = {
            executor.submit(
                scan_one, product, tf, seconds
            ): (product, tf)
            for product, tf, seconds in tasks
        }

        for future in as_completed(futures):
            product, tf = futures[future]
            key = f"{product['symbol']}:{tf}"

            try:
                result = future.result()

                if not result:
                    continue

                candle_time = result["candle_time"]
                previous_time = state.get(key)

                if previous_time is None:
                    state[key] = candle_time
                    continue

                if candle_time > previous_time:
                    state[key] = candle_time

                    if result["signal"]:
                        alerts.append(result)

            except Exception as e:
                print("Scan error:", key, str(e))

    for alert in alerts:
        message = (
            f"Delta EMA20 {alert['signal']} SIGNAL\n\n"
            f"Coin: {alert['symbol']}\n"
            f"Timeframe: {alert['timeframe']}\n"
            f"Rule: 2 consecutive closed candles touching EMA20\n"
            f"EMA20 full-body confirmation\n"
            f"Time: {datetime.fromtimestamp(alert['candle_time'], timezone.utc)} UTC\n\n"
            f"Alert only. No automatic trading."
        )

        try:
            telegram(message)
            print("Telegram alert sent:", alert["symbol"], alert["timeframe"])
        except Exception as e:
            print("Telegram error:", str(e))

    save_state(state)

    print("Scanner completed.")
    print("Alerts sent:", len(alerts))


if __name__ == "__main__":
    main()
            
