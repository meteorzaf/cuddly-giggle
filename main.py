import yfinance as yf
import pandas as pd
import requests
import json
import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

# =========================
# CONFIG
# =========================
BOT_TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")

CAPITAL = 1000
RISK_PER_TRADE = 0.02
MAX_WORKERS = 8

STOCK_FILE = "us_stocks.txt"
LOG_FILE = "trade_log.json"
SEEN_FILE = "seen_signals.json"

def load_seen():
    if os.path.exists(SEEN_FILE):
        try:
            with open(SEEN_FILE, "r") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}

def save_seen(data):
    with open(SEEN_FILE, "w") as f:
        json.dump(data, f)



# =========================
# TELEGRAM
# =========================
def send_telegram(msg):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    requests.post(url, data={"chat_id": CHAT_ID, "text": msg})

def should_run():
    now = datetime.now()

    # Singapore time assumed (your system time)
    hour = now.hour
    minute = now.minute

    # Run at 9:00 PM OR 10:00 PM
    if (hour >= 21 and minute == 0) or (hour <= 4 and minute == 0):
        return True

    return False

# =========================
# CLEAN SYMBOLS (YOUR FUNCTION)
# =========================
def clean_symbol_list(symbols):
    cleaned = []

    banned_suffixes = (
        "-U", "-W", "-R", "-WS", "-WT",
        "U", "W", "R"
    )

    for s in symbols:
        if not isinstance(s, str):
            continue

        s = s.strip().upper().replace(".", "-")

        # allow only letters + dash
        if not re.match(r"^[A-Z\-]{1,7}$", s):
            continue

        # remove junk suffixes
        if any(s.endswith(x) for x in banned_suffixes):
            continue

        # remove weird hyphen-heavy tickers
        if s.count("-") > 1:
            continue

        cleaned.append(s)

    return sorted(list(set(cleaned)))

# =========================
# LOAD UNIVERSE
# =========================
def get_all_us_stocks():
    with open(STOCK_FILE, "r") as f:
        stocks = [x.strip().upper() for x in f if x.strip()]

    print("RAW STOCKS:", len(stocks))
    stocks = clean_symbol_list(stocks)
    return stocks


# =========================
# MARKET FILTER
# =========================
import os
import time

def market_ok():
    import yfinance as yf
    import pandas as pd
    import json, os, time

    cache_file = "spy_cache.json"

    # USE CACHE (avoids rate limit)
    if os.path.exists(cache_file):
        with open(cache_file, "r") as f:
            data = json.load(f)

        if time.time() - data["time"] < 600:
            return data["close"] > data["ma50"]

    try:
        spy = yf.download("SPY", period="3mo", interval="1d", progress=False)

        if spy is None or spy.empty or len(spy) < 60:
            return True

        # 🔥 FORCE CLEAN STRUCTURE
        close = spy["Close"]

        if isinstance(close, pd.DataFrame):
            close = close.iloc[:, 0]  # take first column

        ma50 = close.rolling(50).mean()

        latest_close = close.iloc[-1]
        latest_ma50 = ma50.iloc[-1]

        # 🔥 FORCE SCALAR (this is the key)
        latest_close = float(latest_close if not hasattr(latest_close, "__iter__") else list(latest_close)[0])
        latest_ma50 = float(latest_ma50 if not hasattr(latest_ma50, "__iter__") else list(latest_ma50)[0])

        # cache result
        with open(cache_file, "w") as f:
            json.dump({
                "time": time.time(),
                "close": latest_close,
                "ma50": latest_ma50
            }, f)

        return latest_close > latest_ma50

    except:
        # fallback if Yahoo fails
        return True
# =========================
# FETCH DATA (YOUR VERSION)
# =========================
import time

def fetch_data(ticker):
    try:
        time.sleep(0.03)

        df = yf.download(
            ticker,
            period="1mo",
            interval="1h",
            progress=False,
            threads=False,
            auto_adjust=False
        )

        if df is None or df.empty or len(df) < 30:
            print(ticker, "-> empty or too short")
            return None

        # Flatten Yahoo columns if needed
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)

        required = {"Open", "High", "Low", "Close", "Volume"}
        if not required.issubset(set(df.columns)):
            print(ticker, "-> missing required columns:", df.columns)
            return None

        vol_mean = float(df["Volume"].astype(float).mean())
        close_std = float(df["Close"].astype(float).std())

        if vol_mean < 100000:
            print(ticker, "-> low volume")
            return None

        if close_std == 0:
            print(ticker, "-> flat price")
            return None

        return ticker, df

    except Exception as e:
        print(ticker, "fetch error:", e)
        return None
# =========================
# FAST UNIVERSE SCAN (YOUR FUNCTION INTEGRATED)
# =========================
def run_fast_universe_scan(stocks):
    results = []

    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = {executor.submit(fetch_data, s): s for s in stocks}
        data_map = {}

        count = 0
        success = 0

        for f in as_completed(futures):
            count += 1
            try:
                res = f.result()
                if res:
                    ticker, df = res
                    data_map[ticker] = df
                    success += 1
            except Exception as e:
                print("Future error:", e)

            if count % 50 == 0:
                print(f"Processed {count} stocks... successes: {success}")

    for ticker, df in data_map.items():
        results.append((ticker, df))

    print("Final universe_data size:", len(results))
    return results

# =========================
# STRATEGY ENGINE (kept minimal but functional)
# =========================
def analyze(ticker, df):
    df = df.copy()

    df["MA20"] = df["Close"].rolling(20).mean()
    df["MA50"] = df["Close"].rolling(50).mean()
    df["VolAvg20"] = df["Volume"].rolling(20).mean()

    latest = df.iloc[-1]
    prev = df.iloc[-2]

    close = float(latest["Close"])
    ma20 = float(latest["MA20"])
    ma50 = float(latest["MA50"])
    vol = float(latest["Volume"])
    volavg = float(latest["VolAvg20"])

    if pd.isna(ma20) or pd.isna(ma50) or pd.isna(volavg):
        return None

    score = 0
    reasons = []

    if close > ma50:
        score += 2
        reasons.append("above MA50")

    if close > ma20:
        score += 1
        reasons.append("above MA20")

    change_1bar = (close / float(prev["Close"]) - 1) * 100
    if change_1bar > 0.5:
        score += 2
        reasons.append(f"+{change_1bar:.1f}% momentum")

    if vol > volavg * 1.1:
        score += 2
        reasons.append("volume spike")

    high_10 = float(df["High"].rolling(10).max().iloc[-1])
    if close >= high_10 * 0.995:
        score += 2
        reasons.append("near breakout")

    if score < 3:
        return None

    sl = close * 0.97
    tp = close * 1.06

    return {
        "ticker": ticker,
        "entry": close,
        "sl": sl,
        "tp": tp,
        "score": score,
        "reasons": ", ".join(reasons)
    }


# =========================
# SCANNER ENGINE
# =========================
def run_scan():
    if not market_ok():
        send_telegram("⚠️ Market bearish — no trades.")
        return

    stocks = get_all_us_stocks()
    print("Clean universe size:", len(stocks))

    stocks = [
        s for s in stocks
        if len(s) <= 5
        and "-" not in s
    ]

    stocks = stocks[:1000]
    print("Scanning:", len(stocks))

    universe_data = run_fast_universe_scan(stocks)

    if not universe_data:
        print("No universe data returned.")
        return

    print("Universe candidates:", len(universe_data))

    results = []

    for ticker, df in universe_data:
        r = analyze(ticker, df)
        if r:
            results.append(r)

    if not results:
        print("No strong setups this cycle.")
        send_telegram("No strong setups this cycle.")
        return

    # Top 5 only
    results = sorted(results, key=lambda x: x["score"], reverse=True)[:5]

    seen = load_seen()
    new_results = []

    for r in results:
        key = f"{r['ticker']}_{round(r['entry'], 2)}"
        if key not in seen:
            new_results.append(r)
            seen[key] = time.time()

    save_seen(seen)

    if not new_results:
        print("No new top-5 alerts to send.")
        return
        msg = "No new top-5 alerts to send."

    msg = "📊 TOP 5 SCANNER ALERTS\n\n"

    for r in new_results:
        msg += (
            f"{r['ticker']}\n"
            f"Score: {r['score']}\n"
            f"Entry: {r['entry']:.2f}\n"
            f"SL: {r['sl']:.2f}\n"
            f"TP: {r['tp']:.2f}\n"
            f"Why: {r.get('reasons', 'N/A')}\n\n"
        )

    send_telegram(msg)


# =========================
# RUN
# =========================
last_run_hour = None

while True:
    now = datetime.now()
    current_hour = now.strftime("%Y-%m-%d %H")

    if should_run_now():
        if last_run_hour != current_hour:
            print(f"Running scan at {now}")
            
            try:
                run_scan()
            except Exception as e:
                print("ERROR:", e)

            last_run_hour = current_hour

    else:
        print(f"⏸ Sleeping (outside trading hours): {now}")

    time.sleep(60)  # check every minute
