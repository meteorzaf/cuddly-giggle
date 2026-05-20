import yfinance as yf
import pandas as pd
import requests
import json
import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from zoneinfo import ZoneInfo
import csv

# =========================
# CONFIG
# =========================
BOT_TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")

CAPITAL = 150
RISK_PER_TRADE = 0.02
MAX_WORKERS = 12

STOCK_FILE = "liquid_stocks.txt"
LOG_FILE = "trade_log.json"
SEEN_FILE = "seen_signals.json"

PAPER_TRADE_FILE = "paper_trades.csv"
MAX_ALERTS = 3

RUN_INTERVAL_MINUTES = 60
MAX_SCAN_STOCKS = 2400

ENABLE_MAX_OPEN_TRADES = False
MAX_OPEN_TRADES = 20

MIN_PRICE = 5
MIN_AVG_VOLUME = 150000
MIN_SCORE = 8

MAX_RISK_PER_TRADE = CAPITAL * RISK_PER_TRADE
SKIP_IF_ONE_SHARE_RISK_TOO_HIGH = True

DAILY_TICKERS_FILE = "seen_today.txt"
FAILED_TICKERS = set()

BLOCK_REPEAT_LOSERS_SAME_DAY = True
REPEAT_LOSS_BLOCK_DAYS = 10

MAX_ATR_PCT = 0.035          # skip stocks moving too wildly
MAX_LOSS_PCT = 0.045         # max SL distance from entry
BLOCK_REPEAT_LOSERS = True

BUY_FEE = 0.60
SELL_FEE = 0.60
SLIPPAGE_PCT = 0.001

SEND_MARKET_STATUS = True

VERBOSE_LOGS = True
PROGRESS_EVERY = 100

BANNED_KEYWORDS = [
    "2X", "3X",
    "ULTRA", "BEAR", "BULL"
]
BANNED_TICKERS = [
    "RIOT", "MARA", "BITX", "MSTX", "AMDL", "TSLL",
    "HOOD", "UPST",    "DKNG", "S", "RUN", "PUMP", "NASA", "NTNX",
    "PTEN", "BKSY", "CLBT", "DRVN", "FIVN","KRMN", "FROG"
]

LEVERAGED_MODE = False

LEVERAGED_MULTIPLIERS = {
    "TSLL": 2,
    "NVDL": 2,
    "NVDX": 2,
    "AMDL": 2,
    "BITX": 2,
    "MSTX": 2,

    "TQQQ": 3,
    "SQQQ": 3,
    "SOXL": 3,
    "SOXS": 3,
    "SPXL": 3,
    "SPXS": 3,
    "UPRO": 3,
    "TNA": 3,
    "TZA": 3,
}

def save_paper_trade(signal):
    file_exists = os.path.exists(PAPER_TRADE_FILE)

    with open(PAPER_TRADE_FILE, "a", newline="") as f:
        writer = csv.writer(f)

        if not file_exists:
            writer.writerow([
                "date", "ticker", "entry", "sl", "tp",
                "size", "status", "result", "close_date",
                "closed_at", "net_pnl"
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
            "",
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

    if os.path.getsize(PAPER_TRADE_FILE) == 0:
        print("paper_trades.csv is empty. Skipping update.")
        return

    try:
        df_trades = pd.read_csv(PAPER_TRADE_FILE)
    except pd.errors.EmptyDataError:
        print("paper_trades.csv has no columns. Skipping update.")
        return

    if df_trades.empty:
        return

    required_cols = {"status", "ticker", "entry", "sl", "tp", "size"}
    if not required_cols.issubset(set(df_trades.columns)):
        print("paper_trades.csv missing required columns. Skipping update.")
        return

    for i, trade in df_trades.iterrows():
        if trade["status"] != "OPEN":
            continue

        ticker = trade["ticker"]
        entry = float(trade["entry"])
        sl = float(trade["sl"])
        tp = float(trade["tp"])
        size = float(trade["size"])

        try:
            data = yf.download(
                ticker,
                period="10d",
                interval="1d",
                progress=False,
                threads=False
            )

            if data is None or data.empty:
                continue

            if isinstance(data.columns, pd.MultiIndex):
                data.columns = data.columns.get_level_values(0)

            latest_high = float(data["High"].iloc[-1])
            latest_low = float(data["Low"].iloc[-1])

            closed_price = None
            result_label = None

            if latest_low <= sl:
                closed_price = sl
                result_label = "LOSS"

            elif latest_high >= tp:
                closed_price = tp
                result_label = "WIN"

            if closed_price is None:
                continue
            
            gross_pnl = (closed_price - entry) * size

            buy_fee = BUY_FEE
            sell_fee = SELL_FEE

            slippage_cost = (
                entry * size * SLIPPAGE_PCT
                + closed_price * size * SLIPPAGE_PCT
            )

            net_pnl = gross_pnl - buy_fee - sell_fee - slippage_cost
            net_pnl = round(net_pnl, 2)

            df_trades.at[i, "status"] = "CLOSED"
            df_trades.at[i, "result"] = result_label
            df_trades.at[i, "close_date"] = datetime.now().strftime("%Y-%m-%d %H:%M")
            df_trades.at[i, "closed_at"] = round(closed_price, 2)
            df_trades.at[i, "net_pnl"] = net_pnl

        except Exception as e:
            print(f"Could not update {ticker}: {e}")

    df_trades.to_csv(PAPER_TRADE_FILE, index=False)
def ensure_paper_trade_file():
    if not os.path.exists(PAPER_TRADE_FILE) or os.path.getsize(PAPER_TRADE_FILE) == 0:
        with open(PAPER_TRADE_FILE, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([
                "date", "ticker", "entry", "sl", "tp",
                "size", "status", "result", "close_date",
                "closed_at", "net_pnl"
            ])

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

        if response.status_code != 200:
            print("Telegram error response:", response.text)

    except Exception as e:
        print("Telegram error:", e)

def market_is_open_now():
    now_ny = datetime.now(ZoneInfo("America/New_York"))

    # Monday=0, Sunday=6
    if now_ny.weekday() >= 5:
        return False

    market_open = now_ny.replace(hour=9, minute=25, second=0, microsecond=0)
    market_close = now_ny.replace(hour=15, minute=45, second=0, microsecond=0)

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

def lost_today(ticker):
    if not os.path.exists(PAPER_TRADE_FILE):
        return False

    df = pd.read_csv(PAPER_TRADE_FILE)

    if df.empty:
        return False

    today = datetime.now().strftime("%Y-%m-%d")

    losses_today = df[
        (df["ticker"] == ticker) &
        (df["result"] == "LOSS") &
        (df["close_date"].astype(str).str.startswith(today))
    ]

    return len(losses_today) > 0
    
def lost_recently(ticker, days=3):
    if not os.path.exists(PAPER_TRADE_FILE):
        return False

    try:
        df = pd.read_csv(PAPER_TRADE_FILE)
    except:
        return False

    if df.empty or "ticker" not in df.columns or "result" not in df.columns:
        return False

    if "close_date" not in df.columns:
        return False

    df["close_date"] = pd.to_datetime(df["close_date"], errors="coerce")

    cutoff = datetime.now() - pd.Timedelta(days=days)

    recent_losses = df[
        (df["ticker"] == ticker) &
        (df["result"] == "LOSS") &
        (df["close_date"] >= cutoff)
    ]

    return len(recent_losses) > 0

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

def get_scalar(value):
    if isinstance(value, pd.DataFrame):
        value = value.iloc[:, 0]

    if isinstance(value, pd.Series):
        return float(value.iloc[-1])

    return float(value)


def to_float(value):
    try:
        return float(value)
    except:
        return float(value.squeeze().iloc[-1] if hasattr(value.squeeze(), "iloc") else value.squeeze())


def market_ok():
    try:
        spy = yf.download(
            "SPY",
            period="3mo",
            interval="1d",
            progress=False,
            threads=False,
            auto_adjust=False
        )

        if spy is None or spy.empty or len(spy) < 50:
            return True, "⚠️ Market data unavailable"

        if isinstance(spy.columns, pd.MultiIndex):
            spy.columns = spy.columns.get_level_values(0)

        close = spy["Close"]

        if isinstance(close, pd.DataFrame):
            close = close.iloc[:, 0]

        close = pd.to_numeric(close, errors="coerce").dropna()

        ma20 = close.rolling(20).mean()
        ma50 = close.rolling(50).mean()

        latest_close = float(close.iloc[-1])
        latest_ma20 = float(ma20.iloc[-1])
        latest_ma50 = float(ma50.iloc[-1])

        market_good = latest_close > latest_ma20 and latest_close > latest_ma50

        if market_good:
            msg = (
                f"✅ Market OK\n"
                f"SPY: {latest_close:.2f}\n"
                f"MA20: {latest_ma20:.2f}\n"
                f"MA50: {latest_ma50:.2f}"
            )
        else:
            msg = (
                f"⚠️ Market Weak — skipping trades\n"
                f"SPY: {latest_close:.2f}\n"
                f"MA20: {latest_ma20:.2f}\n"
                f"MA50: {latest_ma50:.2f}"
            )

        return market_good, msg

    except Exception as e:
        return True, f"⚠️ Market error: {e}"
# =========================
# FETCH DATA (YOUR VERSION)
# =========================
import time

def fetch_data(ticker):
    try:
        if ticker in FAILED_TICKERS:
            return None

        time.sleep(0.005)

        df = yf.download(
            ticker,
            period="3mo",
            interval="1h",
            progress=False,
            threads=False,
            auto_adjust=False
        )

        if df is None or df.empty or len(df) < 60:
            if VERBOSE_LOGS:
                print(f"{ticker} insufficient candles", flush=True)
            return None

        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)

        required = {"Open", "High", "Low", "Close", "Volume"}
        if not required.issubset(set(df.columns)):
            return None

        close_series = df["Close"]
        volume_series = df["Volume"]

        if isinstance(close_series, pd.DataFrame):
            close_series = close_series.iloc[:, 0]

        if isinstance(volume_series, pd.DataFrame):
            volume_series = volume_series.iloc[:, 0]

        latest_close = float(close_series.iloc[-1])
        avg_volume = float(volume_series.mean())
        close_std = float(close_series.std())

        if latest_close < MIN_PRICE:
            return None

        if avg_volume < MIN_AVG_VOLUME:
            return None

        if close_std == 0:
            return None

        return ticker, df

    except Exception as e:
        FAILED_TICKERS.add(ticker)
        if VERBOSE_LOGS:
            print(ticker, "fetch error:", e, flush=True)
        return None
# =========================
# FAST UNIVERSE SCAN (YOUR FUNCTION INTEGRATED)
# =========================
def run_fast_universe_scan(stocks):
    data_map = {}

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(fetch_data, s): s for s in stocks}

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
                if VERBOSE_LOGS:
                    print("Future error:", e)

            if VERBOSE_LOGS and count % PROGRESS_EVERY == 0:
                print(f"Processed {count}/{len(stocks)} stocks... valid: {success}", flush=True)

    results = list(data_map.items())
    
    print(f"Final universe data size: {len(results)}", flush=True)
    print(f"Scan completed. Checked: {count}, valid: {success}")
    return results
    
# =========================
# STRATEGY ENGINE (kept minimal but functional)
# =========================
def analyze(ticker, df):
    df = df.copy()

    
    if not LEVERAGED_MODE:
        for word in BANNED_KEYWORDS:
            if word in ticker.upper():
                return None

    if ticker.upper() in BANNED_TICKERS:
        return None
                
    df["MA20"] = df["Close"].rolling(20).mean()
    df["MA50"] = df["Close"].rolling(50).mean()
    df["VolAvg20"] = df["Volume"].rolling(20).mean()
    high_low = df["High"] - df["Low"]
    high_close = abs(df["High"] - df["Close"].shift())
    low_close = abs(df["Low"] - df["Close"].shift())
    
    true_range = pd.concat(
        [high_low, high_close, low_close],
        axis=1
    ).max(axis=1)
    
    df["ATR14"] = true_range.rolling(14).mean()

    latest = df.iloc[-2]
    prev = df.iloc[-3]

    close = float(latest["Close"])
    open_price = float(latest["Open"])
    prev_close = float(prev["Close"])

    ma20 = float(latest["MA20"])
    ma50 = float(latest["MA50"])
    vol = float(latest["Volume"])
    volavg = float(latest["VolAvg20"])
    atr14 = float(latest["ATR14"])

    if pd.isna(ma20) or pd.isna(ma50) or pd.isna(volavg) or pd.isna(atr14):
        return None

    # Gap risk filter
    gap = abs(open_price - prev_close) / prev_close
    if gap > 0.03:
        return None

    # Must close green
    if close < open_price:
        return None

    # Volatility filter
    atr_pct = atr14 / close
    if atr_pct > MAX_ATR_PCT:
        return None

    # Avoid chasing extended moves
    distance_from_ma20 = (close - ma20) / ma20
    if distance_from_ma20 > 0.05:
        return None

    # Momentum check — controlled, not too extended
    change_1bar = (close / prev_close - 1) * 100

    if close < 20:
        if change_1bar < 1.0:
            return None
    else:
        if change_1bar < 0.7:
            return None

    avg_volume = float(df["Volume"].mean())

    score = 0
    reasons = []

    if close > ma50:
        score += 2
        reasons.append("above MA50")

    if close > ma20:
        score += 1
        reasons.append("above MA20")

    if ma20 > ma50:
        score += 1
        reasons.append("MA20 above MA50")

    if change_1bar >= 0.7:
        score += 2
        reasons.append(f"+{change_1bar:.1f}% momentum")

    if vol < volavg * 1.15:
        return None
    
    score += 2
    reasons.append("strong volume spike")

    previous_high_10 = float(df["High"].shift(1).rolling(10).max().iloc[-1])
    
    recent_breakout = (
        float(df["Close"].iloc[-5:-1].max()) <= previous_high_10
    )
    
    if close > previous_high_10 * 1.003 and recent_breakout:
        score += 2
        reasons.append("fresh breakout")

    # Stricter rules for low-priced stocks
    if close < 10:
        if avg_volume < 1000000:
            return None

        if score < MIN_SCORE + 1:
            return None

    ticker_upper = ticker.upper()
    
    leveraged_multiplier = LEVERAGED_MULTIPLIERS.get(ticker_upper, 1)
    
    if leveraged_multiplier == 1:
        if "3X" in ticker_upper:
            leveraged_multiplier = 3
        elif "2X" in ticker_upper:
            leveraged_multiplier = 2
    
    is_leveraged = leveraged_multiplier > 1

    if is_leveraged and score < MIN_SCORE + 1:
        return None
    
    if score < MIN_SCORE:
        return None

    # SL/TP
    if close >= 100:
        base_sl_pct = 0.0125
        base_tp_pct = 0.03
    else:
        base_sl_pct = 0.025
        base_tp_pct = 0.05
    

    
    if leveraged_multiplier > 1:
        base_sl_pct *= leveraged_multiplier
        base_tp_pct *= leveraged_multiplier

    total_fees = BUY_FEE + SELL_FEE
    fixed_fee_pct = total_fees / close
    slippage_pct = SLIPPAGE_PCT * 2
    cost_buffer_pct = fixed_fee_pct + slippage_pct

    sl = close * (1 - base_sl_pct)
    tp = close * (1 + base_tp_pct + cost_buffer_pct)

    if sl >= close:
        return None

    loss_pct = (close - sl) / close

    if is_leveraged:
        max_allowed_loss_pct = MAX_LOSS_PCT * leveraged_multiplier
    else:
        max_allowed_loss_pct = MAX_LOSS_PCT
    
    if loss_pct > max_allowed_loss_pct:
        return None

    risk_per_share = close - sl
    if risk_per_share <= 0.5:
        return None

    if SKIP_IF_ONE_SHARE_RISK_TOO_HIGH and risk_per_share > MAX_RISK_PER_TRADE:
        return None

    risk_amount = CAPITAL * RISK_PER_TRADE
    if is_leveraged:
        risk_amount *= 0.5
    size_by_risk = risk_amount / risk_per_share
    size_by_capital = CAPITAL / close

    size = int(min(size_by_risk, size_by_capital))

    if size < 1:
        return None

    return {
        "ticker": ticker,
        "entry": close,
        "sl": sl,
        "tp": tp,
        "score": score,
        "reasons": ", ".join(reasons),
        "size": size
    }


def load_seen_today():
    if not os.path.exists(DAILY_TICKERS_FILE):
        return set()

    today = datetime.now().strftime("%Y-%m-%d")

    seen = set()

    with open(DAILY_TICKERS_FILE, "r") as f:
        for line in f:
            date, ticker = line.strip().split(",")
            if date == today:
                seen.add(ticker)

    return seen

def save_seen_today(ticker):
    today = datetime.now().strftime("%Y-%m-%d")

    with open(DAILY_TICKERS_FILE, "a") as f:
        f.write(f"{today},{ticker}\n")
        
def cleanup_seen_file():
    today = datetime.now().strftime("%Y-%m-%d")

    if not os.path.exists(DAILY_TICKERS_FILE):
        return

    lines = []

    with open(DAILY_TICKERS_FILE, "r") as f:
        for line in f:
            if line.startswith(today):
                lines.append(line)

    with open(DAILY_TICKERS_FILE, "w") as f:
        f.writelines(lines)


# =========================
# SCANNER ENGINE
# =========================
def run_scan():
    print("Bot started...")
    print(f"Run time: {datetime.now()}")
    cleanup_seen_file()

    print("Telegram test sent")


    with open("debug_log.txt", "a") as f:
        f.write("Bot started\n")

    seen_today = load_seen_today()

    ensure_paper_trade_file()
    update_open_paper_trades()
    print("Paper trades updated")

    if ENABLE_MAX_OPEN_TRADES and count_open_trades() >= MAX_OPEN_TRADES:
        print("Max open trades reached")
        send_telegram("⚠️ Max open trades reached. Skipping new signals.")
        return

    market_good, market_msg = market_ok()
    
    print(market_msg)
    
    if SEND_MARKET_STATUS:
        send_telegram(market_msg)
    
    if not market_good:
        return

    stocks = get_all_us_stocks()
    print("RAW / CLEAN STOCKS LOADED:", len(stocks))
    stocks = [s for s in stocks if len(s) <= 5]   # remove weird symbols
    stocks = stocks[:MAX_SCAN_STOCKS]  # hard cap
    print("CAPPED STOCKS:", len(stocks))

    universe_data = run_fast_universe_scan(stocks)
    print("Fetched valid data:", len(universe_data))

    results = []
    
    for ticker, df in universe_data:
        if ticker in seen_today:
            continue
    
        if BLOCK_REPEAT_LOSERS and lost_recently(ticker, days=REPEAT_LOSS_BLOCK_DAYS):
            continue
    
        r = analyze(ticker, df)
    
        if r:
            results.append(r)
    
    print("Signals found:", len(results))
    
    filtered_results = [
        r for r in results
        if r["score"] >= MIN_SCORE
    ]
    
    print("High conviction:", len(filtered_results))
    
    results = filtered_results

    results = sorted(results, key=lambda x: x["score"], reverse=True)[:MAX_ALERTS]
    if not results:
        print("No signals found")
        send_telegram("No strong setups today.")
        return

    msg = "📊 PAPER TRADE SIGNALS\n\n"

    for r in results:
        rr = (r["tp"] - r["entry"]) / (r["entry"] - r["sl"])
    
        if r["score"] >= 10:
            tag = "🚀 ELITE"
        elif r["score"] >= 8:
            tag = "🔥 HIGH"
        elif r["score"] >= 6:
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
            f"Risk/Share: {(r['entry'] - r['sl']):.2f}\n"
            f"RR: {rr:.2f}\n"
            f"Size: {r['size']:.2f} shares\n\n"
        )
    
        save_paper_trade(r)
        save_seen_today(r["ticker"])
    
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
