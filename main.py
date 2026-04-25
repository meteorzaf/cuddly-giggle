import yfinance as yf
import pandas as pd
import requests
import json
import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from zoneinfo import ZoneInfo

# =========================
# CONFIG
# =========================
BOT_TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")

CAPITAL = 1000
RISK_PER_TRADE = 0.02
MAX_WORKERS = 8

STOCK_FILE = "liquid_stocks.txt"
LOG_FILE = "trade_log.json"
SEEN_FILE = "seen_signals.json"

PAPER_TRADE_FILE = "paper_trades.csv"
MAX_ALERTS = 3

RUN_INTERVAL_MINUTES = 60
MAX_SCAN_STOCKS = 3872

def save_paper_trade(signal):
    file_exists = os.path.exists(PAPER_TRADE_FILE)

    with open(PAPER_TRADE_FILE, "a", newline="") as f:
        writer = csv.writer(f)

        if not file_exists:
            writer.writerow([
                "date", "ticker", "entry", "sl", "tp",
                "size", "status", "result", "close_date"
            ])

        writer.writerow([
            datetime.now().strftime("%Y-%m-%d %H:%M"),
            signal["ticker"],
            round(signal["entry"], 2),
            round(signal["sl"], 2),
            round(signal["tp"], 2),
            round(signal["size"], 2),
            "OPEN",
            "",
            ""
        ])

def count_open_trades():
    if not os.path.exists(PAPER_TRADE_FILE):
        return 0

    df = pd.read_csv(PAPER_TRADE_FILE)

    if df.empty or "status" not in df.columns:
        return 0

    return len(df[df["status"] == "OPEN"])

def update_open_paper_trades():
    if not os.path.exists(PAPER_TRADE_FILE):
        return

    df_trades = pd.read_csv(PAPER_TRADE_FILE)

    if df_trades.empty:
        return

    for i, trade in df_trades.iterrows():
        if trade["status"] != "OPEN":
            continue

        ticker = trade["ticker"]
        sl = float(trade["sl"])
        tp = float(trade["tp"])

        try:
            data = yf.download(
                ticker,
                period="10d",
                interval="1d",
                progress=False,
                threads=False
            )

            if data.empty:
                continue

            latest_high = float(data["High"].iloc[-1])
            latest_low = float(data["Low"].iloc[-1])

            if latest_low <= sl:
                df_trades.at[i, "status"] = "CLOSED"
                df_trades.at[i, "result"] = "LOSS"
                df_trades.at[i, "close_date"] = datetime.now().strftime("%Y-%m-%d %H:%M")

            elif latest_high >= tp:
                df_trades.at[i, "status"] = "CLOSED"
                df_trades.at[i, "result"] = "WIN"
                df_trades.at[i, "close_date"] = datetime.now().strftime("%Y-%m-%d %H:%M")

        except Exception as e:
            print(f"Could not update {ticker}: {e}")

    df_trades.to_csv(PAPER_TRADE_FILE, index=False)

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

    try:
        response = requests.post(
            url,
            data={"chat_id": CHAT_ID, "text": msg},
            timeout=10
        )

        print("Telegram status:", response.status_code)
        print("Telegram response:", response.text)

    except Exception as e:
        print("Telegram error:", e)

def market_is_open_now():
    now_ny = datetime.now(ZoneInfo("America/New_York"))

    # Monday=0, Sunday=6
    if now_ny.weekday() >= 5:
        return False

    market_open = now_ny.replace(hour=9, minute=30, second=0, microsecond=0)
    market_close = now_ny.replace(hour=16, minute=0, second=0, microsecond=0)

    return market_open <= now_ny <= market_close

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
            return None

        if close_std == 0:
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

    risk_per_share = close - sl
    risk_amount = CAPITAL * RISK_PER_TRADE
    size = risk_amount / risk_per_share if risk_per_share > 0 else 0

    return {
        "ticker": ticker,
        "entry": close,
        "sl": sl,
        "tp": tp,
        "score": score,
        "reasons": ", ".join(reasons),
        "size": size
    }


# =========================
# SCANNER ENGINE
# =========================
def run_scan():
    print("Bot started...")
    print(f"Run time: {datetime.now()}")

    send_telegram("✅ Bot started scanning.")
    print("Telegram test sent")

    with open("debug_log.txt", "a") as f:
        f.write("Bot started\n")

    update_open_paper_trades()
    print("Paper trades updated")

    print("Bot started...")

    update_open_paper_trades()
    print("Paper trades updated")

    if count_open_trades() >= 5:
        print("Max open trades reached")
        send_telegram("⚠️ Max open trades reached. Skipping new signals.")
        return

    if not market_ok():
        print("Market bearish")
        send_telegram("⚠️ Market bearish — no trades.")
        return

    stocks = get_all_us_stocks()
    print("RAW / CLEAN STOCKS LOADED:", len(stocks))
    stocks = stocks[:MAX_SCAN_STOCKS]
    print("CAPPED STOCKS:", len(stocks))

    universe_data = run_fast_universe_scan(stocks)
    print("Fetched valid data:", len(universe_data))

    results = []

    for ticker, df in universe_data:
        r = analyze(ticker, df)
        if r:
            results.append(r)

    print("Signals found:", len(results))

    results = [r for r in results if r["score"] >= 5]
    print("High conviction signals:", len(results))

    results = sorted(results, key=lambda x: x["score"], reverse=True)[:MAX_ALERTS]
    if not results:
        print("No signals found")
        send_telegram("No strong setups today.")
        return

    msg = "📊 PAPER TRADE SIGNALS\n\n"

    for r in results:
        if r["score"] >= 6:
            tag = "🔥 HIGH PRIORITY"
        elif r["score"] >= 5:
            tag = "✅ GOOD"
        else:
            tag = "⚠️ WEAK"

        msg += (
            f"{r['ticker']} {tag}\n"
            f"Score: {r['score']}\n"
            f"Reason: {r['reasons']}\n"
            f"Entry: {r['entry']:.2f}\n"
            f"SL: {r['sl']:.2f}\n"
            f"TP: {r['tp']:.2f}\n"
            f"Size: {r['size']:.2f} shares\n\n"
        )
        save_paper_trade(r)

    print("Sending Telegram message...")
    send_telegram(msg)
    print("Done.")



# =========================
# RUN
# =========================
if __name__ == "__main__":
    last_run_time = None

    while True:
        if market_is_open_now():
            now = datetime.now()

            if (
                last_run_time is None
                or (now - last_run_time).total_seconds() >= RUN_INTERVAL_MINUTES * 60
            ):
                print("Market open. Running scan...")
                run_scan()
                last_run_time = now
            else:
                print("Market open. Waiting for next interval...")
        else:
            print("Market closed. Sleeping...")

        time.sleep(60)
