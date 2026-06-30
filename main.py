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
from collections import defaultdict

# =========================
# CONFIG
# =========================
BOT_TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")

CAPITAL = 10000
RISK_PER_TRADE = 0.02
MAX_WORKERS = 12

STOCK_FILE = "liquid_stocks.txt"
LOG_FILE = "trade_log.json"
SEEN_FILE = "seen_signals.json"

PAPER_TRADE_FILE = "paper_trades.csv"
MAX_ALERTS = 10

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
MAX_POSITION_PCT = 0.1
MAX_PORTFOLIO_RISK = 0.10

BUY_FEE = 0.60
SELL_FEE = 0.60
SLIPPAGE_PCT = 0.001

SEND_MARKET_STATUS = True

VERBOSE_LOGS = True
PROGRESS_EVERY = 100

ENABLE_EARNINGS_FILTER = True
EARNINGS_BLACKOUT_DAYS = 3

MIN_MARKET_CAP = 5_000_000_000

SEND_MARKET_STATUS_ON_CHANGE = True
MARKET_STATUS_FILE = "market_status.json"

RETEST_BUFFER = 0.002
MAX_PULLBACK_PCT = 0.015

REJECT_REASONS = defaultdict(int)
CANDLE_STRENGTH_BUCKETS = defaultdict(int)

WEAK_CANDLE_DEBUG = []

ENABLE_SECTOR_FILTER = True
BANNED_SECTORS = [
    "Biotechnology",
    "Pharmaceuticals"
]

BANNED_KEYWORDS = [
    "2X",
    "3X",
    "ULTRA",
    "BEAR",
    "BULL"
]
BANNED_TICKERS = []

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

        close = pd.to_numeric(spy["Close"], errors="coerce").dropna()

        ma20 = close.rolling(20).mean()
        ma50 = close.rolling(50).mean()

        latest_close = float(close.iloc[-1])
        latest_ma20 = float(ma20.iloc[-1])
        latest_ma50 = float(ma50.iloc[-1])

        above_ma20 = latest_close > latest_ma20
        above_ma50 = latest_close > latest_ma50

        dist20 = ((latest_close / latest_ma20) - 1) * 100
        dist50 = ((latest_close / latest_ma50) - 1) * 100

        if above_ma20 and above_ma50:
            market_state = "STRONG"
        elif above_ma50:
            market_state = "NEUTRAL"
        else:
            market_state = "WEAK"
        
        market_good = market_state != "WEAK"
        
        if market_state == "STRONG":
            strength = "🟢 Strong"
        elif market_state == "NEUTRAL":
            strength = "🟡 Neutral"
        else:
            strength = "🔴 Weak"

        ma20_icon = "✅" if above_ma20 else "❌"
        ma50_icon = "✅" if above_ma50 else "❌"


        msg = (
            f"{strength} Market\n\n"
            f"SPY: {latest_close:.2f}\n\n"
            f"MA20: {latest_ma20:.2f} {ma20_icon} ({dist20:+.2f}%)\n"
            f"MA50: {latest_ma50:.2f} {ma50_icon} ({dist50:+.2f}%)"
        )

        return market_good, msg

    except Exception as e:
        return True, f"⚠️ Market filter error:\n{e}"
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

def earnings_nearby(ticker, days=3):
    try:
        stock = yf.Ticker(ticker)
        cal = stock.calendar

        if cal is None or cal.empty:
            return False

        earnings_date = None

        if "Earnings Date" in cal.index:
            earnings_date = cal.loc["Earnings Date"][0]

        if earnings_date is None:
            return False

        earnings_date = pd.to_datetime(earnings_date).tz_localize(None)
        today = pd.Timestamp.now().normalize()

        diff = abs((earnings_date.normalize() - today).days)

        return diff <= days

    except Exception:
        return False


def passes_fundamental_safety_filter(ticker):
    try:
        info = yf.Ticker(ticker).info

        market_cap = info.get("marketCap", 0)
        sector = str(info.get("sector") or "")
        industry = str(info.get("industry") or "")

        if market_cap and market_cap < MIN_MARKET_CAP:
            return False

        if ENABLE_SECTOR_FILTER:
            sector_lower = sector.lower()
            industry_lower = industry.lower()

            for banned in BANNED_SECTORS:
                banned_lower = banned.lower()

                if (
                    banned_lower in sector_lower
                    or banned_lower in industry_lower
                ):
                    return False

        return True

    except Exception:
        # Don't reject a stock just because Yahoo Finance failed
        return True

def reject(reason):
    REJECT_REASONS[reason] += 1
    return None

def reject_weak_candle(candle_strength):
    log_candle_strength(candle_strength)
    return reject("weak_candle")
# =========================
# STRATEGY ENGINE (kept minimal but functional)
# =========================
def analyze(ticker, df):
    df = df.copy()

    
    if not LEVERAGED_MODE:
        for word in BANNED_KEYWORDS:
            if word in ticker.upper():
                return reject("banned_keyword")

    if ticker.upper() in BANNED_TICKERS:
        return reject("banned_ticker")
              
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
        return reject("missing_indicators")

    # Gap risk filter
    gap = abs(open_price - prev_close) / prev_close
    if gap > 0.03:
        return reject("gap_risk")
        
    score = 0
    reasons = []
    candle_range = float(latest["High"] - latest["Low"])
    
    if candle_range <= 0:
        return reject("zero_candle_range")
    
    candle_strength = (close - open_price) / candle_range
    
    
    if candle_strength < 0.5:

    log_candle_strength(candle_strength)

    WEAK_CANDLE_DEBUG.append({
        "ticker": ticker,
        "strength": candle_strength,
        "open": open_price,
        "high": float(latest["High"]),
        "low": float(latest["Low"]),
        "close": close,
        "volume": vol,
        "avg_volume": volavg,
        "score": score,
    })

    WEAK_CANDLE_DEBUG[:] = sorted(
        WEAK_CANDLE_DEBUG,
        key=lambda x: x["strength"],
        reverse=True
    )[:10]

    return reject("weak_candle")
    
    if candle_strength > 0.7:
        score += 1
        reasons.append("strong candle close")

    previous_high_10 = float(df["High"].shift(1).rolling(10).max().iloc[-1])
    
    recent_breakout = (float(df["Close"].iloc[-5:-1].max()) <= previous_high_10)

    breakout_pct = (close / previous_high_10 - 1) * 100
    
    if breakout_pct > 1.0:
        score += 1
        reasons.append("strong breakout")

    if vol > volavg * 1.5:
        score += 1
        reasons.append("heavy volume expansion")

    # Volatility filter
    atr_pct = atr14 / close
    if atr_pct > MAX_ATR_PCT:
        return reject("atr_too_high")
        
    # Avoid chasing extended moves
    distance_from_ma20 = (close - ma20) / ma20
    if distance_from_ma20 > 0.05:
        return reject("too_extended")

    # Momentum check — controlled, not too extended
    change_1bar = (close / prev_close - 1) * 100

    if close < 20:
        if change_1bar < 1.0:
            return reject("weak_low_price_momentum")
    else:
        if change_1bar < 0.7:
            return reject("weak_momentum")

    avg_volume = float(df["Volume"].mean())


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

    if vol < volavg * 1.05:
        return reject("weak_volume")
    
    score += 2
    reasons.append("strong volume spike")
    
    if breakout_pct >= 0.3 and recent_breakout:
        score += 3
        reasons.append("fresh breakout")
    
    entry = previous_high_10 * (1 + RETEST_BUFFER)
    
    pullback_pct = (close - entry) / close
    
    if pullback_pct > MAX_PULLBACK_PCT:
        return reject("retest_too_far")

    if close < entry * 0.999:
        return reject("entry_above_close")

    # Stricter rules for low-priced stocks
    if close < 10:
        if avg_volume < 1000000:
            return reject("low_price_low_volume")

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
        return reject("low_score")

    # Run slow Yahoo fundamental/earnings checks only after technical filters pass
    if not passes_fundamental_safety_filter(ticker):
        return reject("fundamental_filter")
    
    if ENABLE_EARNINGS_FILTER and earnings_nearby(ticker, EARNINGS_BLACKOUT_DAYS):
        return reject("earnings_nearby")

    # SL/TP
    if entry >= 100:
        base_sl_pct = 0.0125
        base_tp_pct = 0.03
    else:
        base_sl_pct = 0.025
        base_tp_pct = 0.05
    

    
    if leveraged_multiplier > 1:
        base_sl_pct *= leveraged_multiplier
        base_tp_pct *= leveraged_multiplier

    total_fees = BUY_FEE + SELL_FEE
    fixed_fee_pct = total_fees / entry
    slippage_pct = SLIPPAGE_PCT * 2
    cost_buffer_pct = fixed_fee_pct + slippage_pct

    sl = entry * (1 - base_sl_pct)
    tp = entry * (1 + base_tp_pct + cost_buffer_pct)

    if sl >= entry:
        return None

    loss_pct = (entry - sl) / entry

    if is_leveraged:
        max_allowed_loss_pct = MAX_LOSS_PCT * leveraged_multiplier
    else:
        max_allowed_loss_pct = MAX_LOSS_PCT
    
    if loss_pct > max_allowed_loss_pct:
        return None

    risk_per_share = entry - sl
    
    if risk_per_share <= 0:
        return None

    if SKIP_IF_ONE_SHARE_RISK_TOO_HIGH and risk_per_share > MAX_RISK_PER_TRADE:
        return None

    risk_amount = CAPITAL * RISK_PER_TRADE
    if is_leveraged:
        risk_amount *= 0.5
    size_by_risk = risk_amount / risk_per_share
    
    size_by_capital = CAPITAL / entry
    
    max_position_value = CAPITAL * MAX_POSITION_PCT
    size_by_position_cap = max_position_value / entry
    
    size = int(
        min(
            size_by_risk,
            size_by_capital,
            size_by_position_cap
        )
    )

    if size < 1:
        return reject("position_too_small")

    quality = 0

    volume_ratio = vol / volavg
    volume_score = max(0, min((volume_ratio - 1) * 25, 25))
    quality += volume_score
    
    breakout_strength = (close - previous_high_10) / previous_high_10
    quality += min(max(breakout_strength * 1000, 0), 25)
    
    momentum_strength = max(change_1bar, 0)
    quality += min(momentum_strength * 8, 25)
    
    trend_strength = (close / ma20 - 1) * 100
    quality += min(max(trend_strength * 3, 0), 15)
    
    ma_strength = (ma20 / ma50 - 1) * 100
    quality += min(max(ma_strength * 2, 0), 15)
    
    atr_penalty = atr_pct * 100
    quality -= min(atr_penalty * 3, 20)
    
    quality = round(quality, 2)

    return {
        "ticker": ticker,
        "entry": entry,
        "sl": sl,
        "tp": tp,
        "score": score,
        "quality": quality,
        "reasons": ", ".join(reasons),
        "size": size
    }
def log_candle_strength(value):
    if value < 0:
        bucket = "<0.00"
    elif value < 0.10:
        bucket = "0.00-0.10"
    elif value < 0.20:
        bucket = "0.10-0.20"
    elif value < 0.30:
        bucket = "0.20-0.30"
    elif value < 0.40:
        bucket = "0.30-0.40"
    elif value < 0.45:
        bucket = "0.40-0.45"
    elif value < 0.50:
        bucket = "0.45-0.50"
    elif value < 0.60:
        bucket = "0.50-0.60"
    elif value < 0.70:
        bucket = "0.60-0.70"
    elif value < 0.80:
        bucket = "0.70-0.80"
    elif value < 0.90:
        bucket = "0.80-0.90"
    else:
        bucket = "0.90-1.00"

    CANDLE_STRENGTH_BUCKETS[bucket] += 1

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
        
def get_open_portfolio_risk():
    try:
        if not os.path.exists(PAPER_TRADE_FILE):
            return 0

        df = pd.read_csv(PAPER_TRADE_FILE)

        if df.empty:
            return 0

        open_trades = df[df["status"] == "OPEN"]

        total_risk = 0

        for _, row in open_trades.iterrows():
            risk = (
                float(row["entry"])
                - float(row["sl"])
            ) * float(row["size"])

            total_risk += risk

        return total_risk

    except Exception:
        return 0

def load_last_market_status():
    if not os.path.exists(MARKET_STATUS_FILE):
        return None

    try:
        with open(MARKET_STATUS_FILE, "r") as f:
            data = json.load(f)
            return data.get("market_good")
    except Exception:
        return None


def save_market_status(market_good):
    with open(MARKET_STATUS_FILE, "w") as f:
        json.dump({"market_good": market_good}, f)


def should_send_market_status(market_good):
    if not SEND_MARKET_STATUS_ON_CHANGE:
        return SEND_MARKET_STATUS

    last_status = load_last_market_status()

    if last_status is None:
        save_market_status(market_good)
        return True

    if last_status != market_good:
        save_market_status(market_good)
        return True

    return False


# =========================
# SCANNER ENGINE
# =========================
def run_scan():
    global REJECT_REASONS, CANDLE_STRENGTH_BUCKETS
    global WEAK_CANDLE_DEBUG

    WEAK_CANDLE_DEBUG = []
    
    REJECT_REASONS = defaultdict(int)
    CANDLE_STRENGTH_BUCKETS = defaultdict(int)

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
    
    if should_send_market_status(market_good):
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
    
    total_rejections = sum(REJECT_REASONS.values())
    
    print("Reject summary:")
    for reason, count in sorted(REJECT_REASONS.items(), key=lambda x: x[1], reverse=True):
        pct = (count / total_rejections) * 100 if total_rejections else 0
        print(f"{reason}: {count} ({pct:.1f}%)")

    print(f"\nRejected: {total_rejections}")
    print(f"Passed: {len(results)}")
    total_checked_by_analyze = len(results) + total_rejections
    pass_rate = (len(results) / total_checked_by_analyze) * 100 if total_checked_by_analyze else 0
    
    print(f"Pass rate: {pass_rate:.2f}%")    
    
    bucket_order = [
        "<0.00",
        "0.00-0.10",
        "0.10-0.20",
        "0.20-0.30",
        "0.30-0.40",
        "0.40-0.45",
        "0.45-0.50",
    ]
    
    print("\nWeak candle distribution:")

    print("\nFirst 10 weak candle rejects:")
    
    for stock in WEAK_CANDLE_DEBUG:
    
        print(
            f"{stock['ticker']:<6} "
            f"Score={stock['score']} "
            f"S={stock['strength']:.2f} "
            f"O={stock['open']:.2f} "
            f"H={stock['high']:.2f} "
            f"L={stock['low']:.2f} "
            f"C={stock['close']:.2f} "
            f"Vol={stock['volume']:,} "
            f"Avg={stock['avg_volume']:,}"
        )
        
    total_weak_candles = REJECT_REASONS.get("weak_candle", 0)
    
    for bucket in bucket_order:
        count = CANDLE_STRENGTH_BUCKETS.get(bucket, 0)
    
        if count:
            pct = (count / total_weak_candles) * 100 if total_weak_candles else 0
            print(f"{bucket}: {count} ({pct:.1f}%)")

    if results:
        print(f"Top quality: {max(r['quality'] for r in results):.2f}")
    else:
        print("Highest quality: N/A")
    
    filtered_results = [
        r for r in results
        if r["score"] >= MIN_SCORE
    ]
    
    print("High conviction:", len(filtered_results))
    
    results = sorted(
        filtered_results,
        key=lambda x: x["quality"],
        reverse=True
    )[:MAX_ALERTS]
    
    if not results:
        print("No signals found")
    
        top_rejections = sorted(
            REJECT_REASONS.items(),
            key=lambda x: x[1],
            reverse=True
        )[:5]
            
        total_rejections = sum(REJECT_REASONS.values())
        
        reject_msg = "\n".join(
            [
                f"{reason}: {count} ({(count / total_rejections) * 100:.1f}%)"
                for reason, count in top_rejections
            ]
        ) if total_rejections else "No rejection data available"
    
        if not reject_msg:
            reject_msg = "No rejection data available"
    
        send_telegram(
            f"{market_msg}\n\n"
            f"Valid universe: {len(universe_data)}\n\n"
            f"No signals passed all filters.\n\n"
            f"Top rejection reasons:\n{reject_msg}"
        )
    
        return

    msg = "📊 PAPER TRADE SIGNALS\n\n"

    current_portfolio_risk = get_open_portfolio_risk()

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
        
        new_trade_risk = (
            r["entry"]
            - r["sl"]
        ) * r["size"]
        
        if (
            current_portfolio_risk
            + new_trade_risk
            > CAPITAL * MAX_PORTFOLIO_RISK
        ):
            print(
                f"Portfolio risk limit reached. "
                f"Skipping {r['ticker']}"
            )
            continue
    
        msg += (
            f"{r['ticker']} {tag}\n"
            f"Score: {r['score']}\n"
            f"Quality: {r['quality']:.2f}\n"
            f"Reason: {r['reasons']}\n"
            f"Entry: {r['entry']:.2f}\n"
            f"SL: {r['sl']:.2f}\n"
            f"TP: {r['tp']:.2f}\n"
            f"Risk/Share: {(r['entry'] - r['sl']):.2f}\n"
            f"RR: {rr:.2f}\n"
            f"Size: {r['size']} shares\n\n"
        )
    
        save_paper_trade(r)
        save_seen_today(r["ticker"])
        current_portfolio_risk += new_trade_risk
    
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
