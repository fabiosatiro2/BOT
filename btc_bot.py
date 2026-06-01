# ULTIMATE SCALPING BOT v6.0
# Alle Coinbase Advanced Signale: Orderbook, Market Trades, Candles, BTC Filter
# Techniken: RSI, EMA, MACD, Bollinger, Divergenz, Orderflow, Taker-Ratio

import os, re, time, uuid, sqlite3, requests, threading, json, concurrent.futures
from datetime import datetime
from dotenv import load_dotenv
from flask import Flask, jsonify, Response
from cryptography.hazmat.primitives.serialization import (
    load_pem_private_key, Encoding, PrivateFormat, NoEncryption)
from cryptography.hazmat.backends import default_backend
from coinbase.rest import RESTClient

load_dotenv()
API_KEY     = os.getenv("CDP_API_KEY_NAME") or os.getenv("COINBASE_API_KEY")
PRIVATE_KEY = os.getenv("CDP_PRIVATE_KEY")  or os.getenv("COINBASE_SECRET")

# ?? SETTINGS ??????????????????????????????????????????????????????????????????
DB_NAME          = "trader.db"
TARGETS          = [5, 25, 100, 250, 500, 1000, 2500, 5000]
CHECK_INTERVAL   = 10
MAX_POSITIONS    = 5      # v6.8: €700 Portfolio
MIN_ORDER_USDC   = 30.0   # v6.8: €700 Portfolio
TAKER_FEE_PCT    = 0.006
MAKER_FEE_PCT    = 0.0        # Maker Orders = 0% Fee
TOTAL_FEE_PCT    = TAKER_FEE_PCT * 2   # Realistisch: Taker Fees fuer Break-Even Berechnung

# Long/Short Parameter
LONG_STOP_PCT    = 0.015   # v6.6: etwas weiter fuer weniger false stops
LONG_TP_PCT      = 0.070   # v6.6: höherer TP für mehr Netto-Gewinn
LONG_TRAIL_PCT   = 0.012   # v6.7: Kompromiss Sicherung/Luft
MIN_SCORE        = 12.0   # v6.8: Grok-Empfehlung, nur sehr starke Signale

# Position Sizing
BASE_RISK_PCT    = 0.26   # 77% WR v6.2
MAX_RISK_PCT     = 0.25   # v6.8: max 25% pro Position
MIN_RISK_PCT     = 0.08   # v6.8: min 8% = ~$60 pro Trade

# ── PERP SETTINGS ─────────────────────────────────────────────
PERP_ENABLED     = os.getenv("PERP_ENABLED", "true").lower() == "true"
PERP_LEVERAGE    = int(os.getenv("PERP_LEVERAGE", "3"))
PERP_MAX_POS     = 3
PERP_MIN_MARGIN  = 30.0
PERP_MAX_MARGIN  = 100.0
PERP_ALLOC       = 0.30
PERP_STOP_PCT    = 0.015
PERP_TP_PCT      = 0.060
PERP_TRAIL_PCT   = 0.012
PERP_MIN_SCORE_L = 11.0
PERP_MIN_SCORE_S = 11.0

# ── TIMING ──────────────────────────────────────────────────────
COOLDOWN         = 60
ERROR_COOLDOWN   = 600
PHANTOM_GUARD    = 180     # v6.8: Grok-Empfehlung, längerer Schutz

# Candle Settings
CANDLE_GRAN      = "FIVE_MINUTE"
CANDLE_GRAN_H1   = "ONE_HOUR"
CANDLE_HOURS     = 8
CANDLE_TTL       = 60
PRICE_TTL        = 3
BOOK_TTL         = 5
TRADES_TTL       = 10

# Rate Limiter
RATE_LIMIT_RPS   = 6
_rate_lock       = threading.Lock()
_rate_times      = []

BLACKLIST = {
    "EURC-USDC","CBETH-USDC","WBTC-USDC","USDT-USDC","DAI-USDC",
    "TUSD-USDC","USDP-USDC","WETH-USDC","WSTETH-USDC","RETH-USDC",
    "SBTC-USDC","LBTC-USDC","EBTC-USDC","TBTC-USDC","WLFI-USDC",
}

# ?? GLOBALS ???????????????????????????????????????????????????????????????????
positions            = {}
recently_bought      = {}
last_trade_time      = {}
pair_error_time      = {}
trade_log            = []
milestones_reached   = []
current_target_index = 0
start_total          = None
last_known_total     = None   # Auszahlungs-Erkennung
wins = losses        = 0
consecutive_losses   = 0
circuit_breaker_until = 0
daily_start_total    = None
daily_loss_limit_hit = False
_candle_cache        = {}
_price_cache         = {}
_volume_cache        = {}
_book_cache          = {}
_trades_cache        = {}
_whale_cache         = {}
_corr_cache          = {}   # Korrelations-Cache
_corr_data           = {}   # Preisdaten fuer Korrelation
_btc_cache           = [0, None]
scored_signals       = []

dashboard_state = {
    "total":0.0,"usdc_free":0.0,"in_positions":0.0,"today_pnl":0.0,
    "cycle":0,"phase":"unknown","fear_greed":50,"positions":[],
    "trades":[],"wins":0,"losses":0,"milestones_done":[],
    "current_target_idx":0,"top_signals":[],"history":[],"daily_chart":[],"daily_chart_labels":[],"daily_start_date":"","week_chart":[],"week_chart_labels":[],"month_chart":[],"month_chart_labels":[],
    "total_fees":0.0,"total_profit":0.0,
}

# ── DAILY CHART ────────────────────────────────────────────────
def load_daily_chart():
    today = datetime.now().strftime("%Y-%m-%d")
    conn = sqlite3.connect(DB_NAME)
    rows = conn.execute(
        "SELECT ts, value FROM daily_chart WHERE date=? ORDER BY ts",
        (today,)).fetchall()
    conn.close()
    return today, [r[0] for r in rows], [r[1] for r in rows]

def save_chart_point(ts, value):
    today = datetime.now().strftime("%Y-%m-%d")
    conn = sqlite3.connect(DB_NAME)
    conn.execute("INSERT OR REPLACE INTO daily_chart (ts,value,date) VALUES (?,?,?)",
                 (ts, value, today))
    conn.commit(); conn.close()

def cleanup_old_chart():
    # Behalte nur die letzten 31 Tage
    conn = sqlite3.connect(DB_NAME)
    conn.execute("DELETE FROM daily_chart WHERE date < date('now','-31 days')")
    conn.commit(); conn.close()

def load_week_chart():
    """Ein Punkt pro Stunde, letzte 7 Tage"""
    conn = sqlite3.connect(DB_NAME)
    rows = conn.execute("""
        SELECT substr(ts,1,13) as hour, AVG(value) as avg_val
        FROM daily_chart
        WHERE date >= date('now','-7 days')
        GROUP BY hour ORDER BY hour
    """).fetchall()
    conn.close()
    labels = [r[0][5:] for r in rows]  # MM-DD HH
    values = [round(r[1],4) for r in rows]
    return labels, values

def load_month_chart():
    """Ein Punkt pro Tag, letzte 31 Tage — letzter Wert des Tages"""
    conn = sqlite3.connect(DB_NAME)
    rows = conn.execute("""
        SELECT date, value FROM daily_chart
        WHERE ts IN (
            SELECT MAX(ts) FROM daily_chart
            WHERE date >= date('now','-31 days')
            GROUP BY date
        ) ORDER BY date
    """).fetchall()
    conn.close()
    labels = [r[0][5:] for r in rows]  # MM-DD
    values = [round(r[1],4) for r in rows]
    return labels, values

# ── RATE LIMITER ────────────────────────────────────────────────
def rate_limit():
    with _rate_lock:
        now = time.time()
        while _rate_times and now - _rate_times[0] > 1.0:
            _rate_times.pop(0)
        if len(_rate_times) >= RATE_LIMIT_RPS:
            st = 1.0 - (now - _rate_times[0])
            if st > 0: time.sleep(st)
        _rate_times.append(time.time())

# ?? LOGGING ???????????????????????????????????????????????????????????????????
def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")

def log_trade(action, pair, price, pnl=None, usdc_amount=None,
              fee_usdc=None, profit_usdc=None):
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    entry = {"time": datetime.now().strftime("%H:%M:%S"),
             "action": action, "pair": pair, "price": price,
             "pnl": pnl, "usdc_amount": usdc_amount,
             "fee_usdc": fee_usdc, "profit_usdc": profit_usdc}
    trade_log.append(entry)
    try:
        conn = sqlite3.connect(DB_NAME)
        conn.execute(
            "INSERT INTO trades (pair,action,price,pnl,usdc_amount,"
            "fee_usdc,profit_usdc,created_at) VALUES (?,?,?,?,?,?,?,?)",
            (pair,action,price,pnl,usdc_amount,fee_usdc,profit_usdc,now_str))
        conn.commit(); conn.close()
    except Exception as e:
        log(f"DB ERR: {e}")
    fee_s = f" Fee:${fee_usdc:.3f}" if fee_usdc else ""
    net_s = f" Netto:${profit_usdc:+.3f}" if profit_usdc is not None else ""
    pnl_s = f" PnL:{pnl:+.2f}%" if pnl is not None else ""
    log(f"TRADE: {action} {pair} @{price:.6f}{pnl_s}{fee_s}{net_s}")

# ?? DATABASE ??????????????????????????????????????????????????????????????????
def init_db():
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute("""CREATE TABLE IF NOT EXISTS positions(
        pair TEXT PRIMARY KEY, entry REAL, peak REAL,
        amount REAL, usdc_spent REAL, opened_at TEXT, manual INTEGER DEFAULT 0)""")
    try: c.execute("ALTER TABLE positions ADD COLUMN manual INTEGER DEFAULT 0")
    except: pass
    c.execute("""CREATE TABLE IF NOT EXISTS daily_chart(
        ts TEXT PRIMARY KEY, value REAL, date TEXT)""")
    c.execute("""CREATE TABLE IF NOT EXISTS trades(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        pair TEXT, action TEXT, price REAL, pnl REAL,
        usdc_amount REAL, fee_usdc REAL, profit_usdc REAL, created_at TEXT)""")
    c.execute("""CREATE TABLE IF NOT EXISTS milestones(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        target REAL UNIQUE, reached_at TEXT, total REAL)""")
    for col in ["usdc_amount REAL","fee_usdc REAL","profit_usdc REAL"]:
        try: c.execute(f"ALTER TABLE trades ADD COLUMN {col}")
        except: pass
    conn.commit(); conn.close()

def save_position(pair, entry, peak, amount, usdc_spent, manual=0):
    conn = sqlite3.connect(DB_NAME)
    conn.execute("INSERT OR REPLACE INTO positions "
                 "(pair,entry,peak,amount,usdc_spent,opened_at,manual) VALUES (?,?,?,?,?,?,?)",
                 (pair,entry,peak,amount,usdc_spent,
                  datetime.now().strftime("%Y-%m-%d %H:%M:%S"), manual))
    conn.commit(); conn.close()

def update_peak(pair, peak):
    conn = sqlite3.connect(DB_NAME)
    conn.execute("UPDATE positions SET peak=? WHERE pair=?", (peak, pair))
    conn.commit(); conn.close()

def delete_position(pair):
    conn = sqlite3.connect(DB_NAME)
    conn.execute("DELETE FROM positions WHERE pair=?", (pair,))
    conn.commit(); conn.close()

def load_positions():
    conn = sqlite3.connect(DB_NAME)
    rows = conn.execute(
        "SELECT pair,entry,peak,amount,usdc_spent,manual FROM positions").fetchall()
    conn.close()
    return {r[0]: {"buy":r[1],"peak":r[2],"amount":r[3],
                   "usdc_spent":r[4] or r[3]*r[1],"manual":r[5] or 0} for r in rows}

def load_trades():
    try:
        conn = sqlite3.connect(DB_NAME)
        rows = conn.execute(
            "SELECT pair,action,price,pnl,usdc_amount,fee_usdc,"
            "profit_usdc,created_at FROM trades ORDER BY id DESC LIMIT 200").fetchall()
        conn.close()
        result = []
        for r in reversed(rows):
            ts = r[7][11:19] if r[7] else "--"
            result.append({"time":ts,"action":r[1],"pair":r[0],"price":r[2],
                           "pnl":r[3],"usdc_amount":r[4],
                           "fee_usdc":r[5],"profit_usdc":r[6]})
        return result
    except: return []

def load_milestones():
    try:
        conn = sqlite3.connect(DB_NAME)
        rows = conn.execute(
            "SELECT target,reached_at,total FROM milestones ORDER BY id").fetchall()
        conn.close()
        return [{"target":r[0],"time":r[1],"total":r[2]} for r in rows]
    except: return []

def save_milestone(target, total):
    try:
        conn = sqlite3.connect(DB_NAME)
        conn.execute("INSERT OR IGNORE INTO milestones (target,reached_at,total) VALUES (?,?,?)",
                     (target, datetime.now().strftime("%Y-%m-%d %H:%M:%S"), total))
        conn.commit(); conn.close()
    except: pass

# ?? AUTH ??????????????????????????????????????????????????????????????????????
def parse_ec_key(raw):
    raw = raw.replace("\\n", "\n").strip()
    if "BEGIN EC PRIVATE KEY" in raw or "BEGIN PRIVATE KEY" in raw:
        try:
            load_pem_private_key(raw.encode(), password=None, backend=default_backend())
            return raw
        except: pass
    body = re.sub(r"[^A-Za-z0-9+/=\n]", "", raw).replace("\n", "")
    def wrap(h, f):
        return (f"-----{h}-----\n" +
                "\n".join(body[i:i+64] for i in range(0,len(body),64)) +
                f"\n-----{f}-----")
    pem = wrap("BEGIN PRIVATE KEY", "END PRIVATE KEY")
    try:
        load_pem_private_key(pem.encode(), password=None, backend=default_backend())
        return pem
    except: pass
    pem2 = wrap("BEGIN EC PRIVATE KEY", "END EC PRIVATE KEY")
    key = load_pem_private_key(pem2.encode(), password=None, backend=default_backend())
    return key.private_bytes(Encoding.PEM, PrivateFormat.PKCS8, NoEncryption()).decode()

def create_client():
    return RESTClient(api_key=API_KEY, api_secret=parse_ec_key(PRIVATE_KEY))

# ?? MARKET DATA ???????????????????????????????????????????????????????????????
def get_pairs(client):
    pairs = []
    try:
        for p in client.get_products().products:
            try:
                if (p.quote_currency_id == "USDC" and
                        p.status == "online" and
                        not getattr(p, "trading_disabled", True)):
                    pairs.append(p.product_id)
            except: continue
    except Exception as e:
        log(f"PAIRS ERR: {e}")
    return pairs[:120]

def get_balance(client, currency):
    try:
        cursor = None
        while True:
            accs = client.get_accounts(cursor=cursor) if cursor else client.get_accounts()
            for a in accs.accounts:
                if a.currency == currency:
                    ab = a.available_balance
                    return float(ab["value"] if isinstance(ab, dict) else ab.value)
            if getattr(accs, "has_next", False) and getattr(accs, "cursor", None):
                cursor = accs.cursor
            else: break
    except Exception as e:
        log(f"BALANCE ERR ({currency}): {e}")
    return 0.0

def get_pricebook(client, pair):
    now = time.time()
    cached = _price_cache.get(pair)
    if cached and now - cached[0] < PRICE_TTL:
        return cached[1], cached[2]
    rate_limit()
    try:
        pb = client.get_best_bid_ask(product_ids=[pair]).pricebooks[0]
        bid, ask = float(pb.bids[0].price), float(pb.asks[0].price)
        _price_cache[pair] = (now, bid, ask)
        return bid, ask
    except:
        return (cached[1], cached[2]) if cached else (0.0, 0.0)

def get_orderbook_signal(client, pair):
    """
    Orderbook Imbalance: Verhaeltnis Kauf- zu Verkaufs-Druck
    Wert > 1.0 = mehr Kaeufer = bullish
    Wert < 1.0 = mehr Verkaeufer = bearish
    """
    now = time.time()
    cached = _book_cache.get(pair)
    if cached and now - cached[0] < BOOK_TTL:
        return cached[1]
    rate_limit()
    try:
        book = client.get_product_book(product_id=pair, limit=10)
        bids = book.pricebook.bids
        asks = book.pricebook.asks
        if not bids or not asks:
            return 1.0
        bid_vol = sum(float(b.size) * float(b.price) for b in bids[:5])
        ask_vol = sum(float(a.size) * float(a.price) for a in asks[:5])
        ratio = bid_vol / ask_vol if ask_vol > 0 else 1.0
        _book_cache[pair] = (now, ratio)
        return ratio
    except:
        return cached[1] if cached else 1.0

def get_market_trades_signal(client, pair):
    """
    Taker Buy Ratio: Anteil der Kaeufer unter den letzten Trades
    > 0.6 = mehr Kaeufer (bullish momentum)
    < 0.4 = mehr Verkaeufer (bearish)
    """
    now = time.time()
    cached = _trades_cache.get(pair)
    if cached and now - cached[0] < TRADES_TTL:
        return cached[1]
    rate_limit()
    try:
        result = client.get_market_trades(product_id=pair, limit=30)
        trades = result.trades
        if not trades:
            return 0.5
        buys  = sum(1 for t in trades if getattr(t, "side", "") == "BUY")
        ratio = buys / len(trades)
        _trades_cache[pair] = (now, ratio)
        return ratio
    except:
        return cached[1] if cached else 0.5

def get_candles(client, pair):
    now = time.time()
    cached = _candle_cache.get(pair)
    if cached and now - cached[0] < CANDLE_TTL:
        return cached[1]
    rate_limit()
    for attempt in range(2):
        try:
            resp = client.get_candles(
                product_id=pair,
                start=str(int(now) - 3600 * CANDLE_HOURS),
                end=str(int(now)),
                granularity=CANDLE_GRAN)
            candles = list(reversed(resp.candles))
            data = {
                "close":  [float(c.close)  for c in candles],
                "high":   [float(c.high)   for c in candles],
                "low":    [float(c.low)    for c in candles],
                "open":   [float(c.open)   for c in candles],
                "volume": [float(c.volume) for c in candles],
            }
            _candle_cache[pair] = (now, data)
            return data
        except Exception as e:
            if "429" in str(e) and attempt == 0:
                time.sleep(1.5); continue
            return cached[1] if cached else None
    return cached[1] if cached else None

def get_volume_24h(client, pair):
    now = time.time()
    cached = _volume_cache.get(pair)
    if cached and now - cached[0] < 300:
        return cached[1]
    rate_limit()
    try:
        vol = float(getattr(client.get_product(product_id=pair), "volume_24h", 0))
        _volume_cache[pair] = (now, vol)
        return vol
    except:
        return cached[1] if cached else 0.0

def get_1h_trend(client, pair):
    """1h Candle Trend als Konfirmation fuer 5min Signal"""
    now = time.time()
    rate_limit()
    try:
        resp = client.get_candles(
            product_id=pair,
            start=str(int(now) - 3600 * 24),
            end=str(int(now)),
            granularity=CANDLE_GRAN_H1)
        candles = list(reversed(resp.candles))
        if len(candles) < 10: return 0
        closes = [float(c.close) for c in candles]
        e9  = ema(closes, 9)
        e21 = ema(closes, 21) if len(closes) >= 21 else None
        r   = rsi(closes)
        if not e9 or not r: return 0
        if e21 and e9 > e21 and r > 50: return 1   # bullish
        if e21 and e9 < e21 and r < 50: return -1  # bearish
        return 0
    except: return 0

def get_btc_data(client):
    now = time.time()
    if _btc_cache[1] and now - _btc_cache[0] < 60:
        return _btc_cache[1]
    data = get_candles(client, "BTC-USDC")
    _btc_cache[0] = now; _btc_cache[1] = data
    return data

# ?? INDIKATOREN ???????????????????????????????????????????????????????????????
def rsi(closes, period=9):
    if len(closes) < period + 1: return None
    gains = losses = 0.0
    for i in range(1, period + 1):
        d = closes[-i] - closes[-i-1]
        if d > 0: gains += d
        else: losses -= d
    if losses == 0: return 100.0
    return 100 - (100 / (1 + gains / losses))

def ema(data, n):
    if len(data) < n: return None
    k = 2 / (n + 1); val = data[0]
    for p in data[1:]: val = p * k + val * (1 - k)
    return val

def sma(data, n):
    return sum(data[-n:]) / n if len(data) >= n else None

def macd_bullish(closes):
    if len(closes) < 26: return False
    e12 = ema(closes, 12); e26 = ema(closes, 26)
    if not e12 or not e26: return False
    line = e12 - e26
    sig  = ema(closes[-9:], 9) if len(closes) >= 35 else line
    return line > sig

def bollinger(closes, period=20, mult=2.0):
    if len(closes) < period: return None, None, None, None
    basis = sum(closes[-period:]) / period
    dev   = (sum((c-basis)**2 for c in closes[-period:]) / period) ** 0.5
    upper = basis + mult * dev
    lower = basis - mult * dev
    width = (upper - lower) / basis if basis > 0 else 0
    return upper, lower, basis, width

def volume_spike(volumes, threshold=1.5):
    if len(volumes) < 10: return False
    avg = sum(volumes[-10:-1]) / 9
    return avg > 0 and volumes[-1] > avg * threshold

def rsi_bullish_divergence(closes):
    if len(closes) < 20: return False
    r_now  = rsi(closes[-10:])
    r_prev = rsi(closes[-20:-10])
    if not r_now or not r_prev: return False
    return closes[-1] < closes[-10] and r_now > r_prev

def stochastic_rsi(closes, period=14, smooth=3):
    """StochRSI: RSI des RSI - sehr fruehes Signal"""
    if len(closes) < period * 2: return None, None
    rsi_vals = []
    for i in range(period):
        r = rsi(closes[-(period*2)+i:-(period)+i] if i > 0 else closes[-(period*2):-(period)])
        if r: rsi_vals.append(r)
    if len(rsi_vals) < period: return None, None
    rsi_min = min(rsi_vals[-period:])
    rsi_max = max(rsi_vals[-period:])
    if rsi_max == rsi_min: return None, None
    r_now = rsi(closes)
    if not r_now: return None, None
    k = (r_now - rsi_min) / (rsi_max - rsi_min) * 100
    d = k  # vereinfacht
    return k, d

def vwap_signal(closes, highs, lows, volumes):
    """VWAP: Volume Weighted Average Price - wichtigste Referenz"""
    if len(closes) < 10: return None
    typical = [(highs[i] + lows[i] + closes[i]) / 3 for i in range(-10, 0)]
    vols    = volumes[-10:]
    tot_vol = sum(vols)
    if tot_vol == 0: return None
    vwap = sum(typical[i] * vols[i] for i in range(10)) / tot_vol
    return closes[-1] > vwap  # True = Preis ueber VWAP = bullish

# ?? BTC KONTEXT ???????????????????????????????????????????????????????????????
def btc_context(client):
    """BTC Trend: 1=bullish, -1=bearish, 0=neutral"""
    data = get_btc_data(client)
    if not data or len(data["close"]) < 10: return 0
    closes = data["close"]
    r  = rsi(closes)
    e9 = ema(closes, 9); e21 = ema(closes, 21)
    if not r or not e9 or not e21: return 0
    if r > 52 and e9 > e21 and closes[-1] > closes[-3]: return 1
    if r < 48 and e9 < e21 and closes[-1] < closes[-3]: return -1
    return 0

def get_market_phase(client):
    data = get_btc_data(client)
    if not data or len(data["close"]) < 50: return "sideways"
    closes = data["close"]
    s50 = sma(closes, 50)
    if not s50: return "sideways"
    diff = (closes[-1] - s50) / s50 * 100
    return "bull" if diff > 1.5 else "bear" if diff < -1.5 else "sideways"

def get_fear_greed():
    try:
        r = requests.get("https://api.alternative.me/fng/?limit=1", timeout=6)
        d = r.json()
        if "data" in d: return int(d["data"][0]["value"])
    except: pass
    return 50

# ?? MASTER SIGNAL SCORE ???????????????????????????????????????????????????????
def compute_score(client, pair, data, btc_trend=0):
    """
    Kombiniert ALLE verfuegbaren Signale:
    - Technische Indikatoren (RSI, EMA, MACD, Bollinger, VWAP, StochRSI)
    - Orderbook Imbalance (Coinbase API)
    - Market Trades Taker Ratio (Coinbase API)
    - Volumen Analyse
    - BTC Kontext
    Max Score: ~30 Punkte
    """
    closes  = data["close"]
    highs   = data.get("high", closes)
    lows    = data.get("low",  closes)
    volumes = data["volume"]

    if len(closes) < 20: return -999

    r = rsi(closes)
    if not r or r > 68 or r < 42: return -999  # 77% WR: enger RSI Bereich

    score = 0.0
    signals = []

    # ?? GRUPPE 1: RSI (0-4 Punkte) ??????????????????????????????????????
    if 40 <= r <= 70:
        score += 3.0; signals.append(f"RSI:{r:.0f}")
    elif 30 <= r < 40:
        score += 1.0; signals.append(f"RSI-weak:{r:.0f}")

    # StochRSI - sehr fruehes Signal
    k, d = stochastic_rsi(closes)
    if k is not None and k < 30:
        score += 2.0; signals.append(f"StochRSI-OS:{k:.0f}")
    elif k is not None and 30 <= k < 50:
        score += 1.0

    # ?? GRUPPE 2: TREND (0-7 Punkte) ????????????????????????????????????
    if closes[-1] > closes[-3]:
        score += 3.0; signals.append("3C-up")
    elif len(closes) >= 5 and closes[-1] > closes[-5]:
        score += 1.0; signals.append("5C-up")

    e9  = ema(closes, 9)
    e21 = ema(closes, 21)
    e50 = ema(closes, 50) if len(closes) >= 50 else None
    if e9 and e21:
        if e9 > e21:
            score += 2.0; signals.append("EMA-bull")
        elif e9 > e21 * 0.999:
            score += 0.5
    if e9 and e50 and e9 > e50:
        score += 1.0; signals.append("EMA-trend")

    if macd_bullish(closes):
        score += 2.0; signals.append("MACD")

    # ?? GRUPPE 3: BOLLINGER (0-5 Punkte) ????????????????????????????????
    upper, lower, basis, width = bollinger(closes)
    if width is not None:
        if width < 0.03 and closes[-1] > basis:
            score += 3.0; signals.append("BB-squeeze")
        elif width < 0.03:
            score += 1.0
        if lower and closes[-1] < lower * 1.01:
            score += 2.0; signals.append("BB-low")
        elif upper and closes[-1] > upper * 0.99:
            score -= 2.0  # ueberkauft

    # ?? GRUPPE 4: VWAP (0-2 Punkte) ?????????????????????????????????????
    vwap_bull = vwap_signal(closes, highs, lows, volumes)
    if vwap_bull is True:
        score += 2.0; signals.append("VWAP-bull")
    elif vwap_bull is False:
        score -= 1.0

    # ?? GRUPPE 5: VOLUMEN (0-4 Punkte) ??????????????????????????????????
    if volume_spike(volumes, 1.5):
        score += 2.0; signals.append("Vol-spike")
    if volume_spike(volumes, 2.5):
        score += 1.0; signals.append("Vol-huge")
    if closes[-1] > closes[-2]:
        score += 1.0

    # ?? GRUPPE 6: DIVERGENZ (0-3 Punkte) ????????????????????????????????
    if rsi_bullish_divergence(closes):
        score += 3.0; signals.append("RSI-div")

    # ?? GRUPPE 7: ORDERBOOK (0-3 Punkte) - Coinbase API ?????????????????
    try:
        ob_ratio = get_orderbook_signal(client, pair)
        if ob_ratio > 1.5:
            score += 3.0; signals.append(f"OB:{ob_ratio:.1f}x")
        elif ob_ratio > 1.4:
            score += 2.0; signals.append(f"OB:{ob_ratio:.1f}x")
        elif ob_ratio > 1.2:
            score += 1.0
        elif ob_ratio < 0.8:
            score -= 3.0  # Verkaufsdruck: disqualifiziert
        # Mindestfilter: OB < 1.4 = schwaches Signal
        if ob_ratio < 1.6:
            score -= 2.0  # 77% WR: strengerer OB Filter
    except: pass

    # ?? GRUPPE 8: MARKET TRADES (0-3 Punkte) - Coinbase API ?????????????
    try:
        taker_ratio = get_market_trades_signal(client, pair)
        if taker_ratio > 0.68:
            score += 3.0; signals.append(f"Taker:{taker_ratio:.0%}")
        elif taker_ratio > 0.63:
            score += 2.0; signals.append(f"Taker:{taker_ratio:.0%}")
        elif taker_ratio > 0.58:
            score += 1.0
        elif taker_ratio < 0.40:
            score -= 3.0  # Verkaeufer dominieren stark
        # Mindestfilter
        if taker_ratio < 0.62:
            score -= 2.0  # 77% WR: strengerer Taker Filter
    except: pass

    # ?? GRUPPE 9: BTC KONTEXT (0-3 Punkte) ??????????????????????????????
    if btc_trend == 1:
        score += 3.0; signals.append("BTC-up")
    elif btc_trend == -1:
        score -= 4.0  # BTC faellt = kein Kauf

    # ?? GRUPPE 11: WHALE DETECTION (0-5 Punkte) ??????????????????????????????
    try:
        wh = get_whale_signals(client, pair)
        if wh:
            if wh["bid_wall"]:
                score += 5.0
                signals.append(f"WHALE:{wh['bid_whale']:.0f}x@{wh['support']:.4f}")
            elif wh["ask_wall"]:
                score -= 4.0
                signals.append(f"ASK-WALL:{wh['ask_whale']:.0f}x")
            if wh["depth_ratio"] > 1.5:
                score += 2.0; signals.append(f"DEPTH:{wh['depth_ratio']:.1f}x")
            elif wh["depth_ratio"] > 1.2:
                score += 1.0
            elif wh["depth_ratio"] < 0.7:
                score -= 2.0
            if 3.0 <= wh["bid_whale"] < 5.0:
                score += 1.5; signals.append(f"BIG-BID:{wh['bid_whale']:.0f}x")
    except: pass

    if signals:
        log(f"  {pair} Score={score:.1f} [{', '.join(signals[:5])}]")

    return score

# ?? ADAPTIVE POSITION SIZE ????????????????????????????????????????????????????
def calc_position_size(usdc, score):
    """
    Half-Kelly Criterion: maximiert geometrisches Wachstum
    f* = (WR - (1-WR)/RR) * 0.5 (Half-Kelly = sicherer)
    """
    # Schaetze WR und RR aus aktuellem Score
    max_score = 30.0
    strength  = min(max(score / max_score, 0), 1.0)
    est_wr    = 0.55 + strength * 0.25   # 55-80% je nach Score
    rr        = LONG_TP_PCT / LONG_STOP_PCT  # z.B. 5.2/1.2 = 4.33
    kelly     = est_wr - (1 - est_wr) / rr
    half_kelly = max(MIN_RISK_PCT, min(MAX_RISK_PCT, kelly * 0.5))
    pos = usdc * half_kelly
    return max(min(pos, usdc - MIN_ORDER_USDC), MIN_ORDER_USDC)

# ?? ORDERS ????????????????????????????????????????????????????????????????????
def market_buy(client, pair, usdc):
    """Versucht zuerst Limit-Order (Maker=0% Gebuehr), Fallback Market"""
    try:
        bid, ask = get_pricebook(client, pair)
        if bid > 0:
            # Limit-Order 0.05% unter Ask -> wird als Maker ausgefuehrt
            limit_price = round(ask * 0.9995, 8)
            size = round(usdc / limit_price, 8)
            client.limit_order_gtc_buy(
                client_order_id=str(uuid.uuid4()),
                product_id=pair,
                base_size=str(size),
                limit_price=str(limit_price))
            log(f"LIMIT BUY {pair} @ {limit_price:.6f} (Maker 0%)")
            time.sleep(3)  # Warte auf Ausfuehrung
            # Pruefe ob Order ausgefuehrt
            base = pair.split("-")[0]
            bal = get_balance(client, base)
            if bal * limit_price >= usdc * 0.95:
                return True
            log(f"Limit nicht ausgefuehrt, versuche Market...")
    except Exception as e:
        log(f"Limit ERR {pair}: {e}")
    # Fallback: Market Order
    try:
        client.market_order_buy(
            client_order_id=str(uuid.uuid4()),
            product_id=pair, quote_size=str(round(usdc, 4)))
        return True
    except Exception as e:
        log(f"BUY ERR {pair}: {e}")
        ts = time.time() + (86400 if "account is not available" in str(e) else 0)
        pair_error_time[pair] = max(time.time(), ts)
        return False

def market_sell(client, pair, size):
    try:
        client.market_order_sell(
            client_order_id=str(uuid.uuid4()),
            product_id=pair, base_size=str(round(size, 8)))
        return True
    except Exception as e:
        log(f"SELL ERR {pair}: {e}")
        pair_error_time[pair] = time.time()
        return False

# ?? VALIDATION ????????????????????????????????????????????????????????????????
def validate_pair(client, pair):
    now = time.time()
    if pair in pair_error_time and now - pair_error_time[pair] < ERROR_COOLDOWN:
        return None
    bid, ask = get_pricebook(client, pair)
    if bid <= 0 or ask <= 0: return None
    spread = (ask - bid) / ask * 100
    if spread > 0.15: return None
    return bid, ask

def verify_position(client, pair):
    base = pair.split("-")[0]
    amount = get_balance(client, base)
    if amount < 1e-8:
        log(f"PHANTOM: {pair}")
        delete_position(pair)
        return 0.0
    return amount

# ?? SYNC ??????????????????????????????????????????????????????????????????????
def sync_positions(client):
    pos = load_positions()
    for pair in list(pos.keys()):
        base = pair.split("-")[0]
        if get_balance(client, base) < 1e-8:
            log(f"SYNC CLEANUP: {pair}")
            delete_position(pair); del pos[pair]
    for pair in get_pairs(client):
        if pair in BLACKLIST: continue
        base   = pair.split("-")[0]
        amount = get_balance(client, base)
        if amount <= 0: continue
        bid, _ = get_pricebook(client, pair)
        if bid <= 0 or amount * bid < 0.50: continue
        if pair not in pos:
            pos[pair] = {"buy":bid,"peak":bid,"amount":amount,"usdc_spent":amount*bid,"manual":1}
            save_position(pair, bid, bid, amount, amount*bid, manual=1)
            log(f"SYNC: {pair} @ {bid:.6f} ${amount*bid:.2f} [MANUELL]")
    return pos

# ?? MILESTONE ?????????????????????????????????????????????????????????????????
def check_milestone(total):
    global current_target_index
    if current_target_index >= len(TARGETS): return
    target = TARGETS[current_target_index]
    if total < target: return
    log("=" * 55)
    log(f"*** MEILENSTEIN: ${target} USDC ***")
    milestones_reached.append({"target":target,"total":total,
        "time":datetime.now().strftime("%Y-%m-%d %H:%M:%S")})
    save_milestone(target, total)
    current_target_index += 1
    if current_target_index < len(TARGETS):
        log(f"Naechstes Ziel: ${TARGETS[current_target_index]} USDC")
    log("=" * 55)

# ?? MAIN LOOP ?????????????????????????????????????????????????????????????????

# ?? KI SIGNAL GEWICHTUNGEN (lernbar) ?????????????????????????????????????????
_signal_weights = {
    "rsi":1.0,"ema":1.0,"macd":1.0,"bollinger":1.0,"vwap":1.0,
    "volume":1.0,"orderbook":1.0,"taker":1.0,"btc":1.0,"multi_tf":1.0,
}
_learned_blacklist = set()
_trade_memory      = []
_last_ki_analysis  = 0
_consensus_cache   = {}
KI_INTERVAL        = 3600  # 1h
CONSENSUS_TTL      = 120   # 2min

def save_ki_weights():
    try:
        conn = sqlite3.connect(DB_NAME)
        conn.execute("""CREATE TABLE IF NOT EXISTS ki_weights(
            key TEXT PRIMARY KEY, value REAL, updated_at TEXT)""")
        for k,v in _signal_weights.items():
            conn.execute("INSERT OR REPLACE INTO ki_weights VALUES (?,?,?)",
                (k, v, datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
        for pair in _learned_blacklist:
            conn.execute("INSERT OR REPLACE INTO ki_weights VALUES (?,?,?)",
                (f"bl_{pair}", -1.0, datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
        conn.commit(); conn.close()
    except Exception as e: log(f"KI SAVE ERR: {e}")

def load_ki_weights():
    global _signal_weights, _learned_blacklist
    try:
        conn = sqlite3.connect(DB_NAME)
        conn.execute("""CREATE TABLE IF NOT EXISTS ki_weights(
            key TEXT PRIMARY KEY, value REAL, updated_at TEXT)""")
        rows = conn.execute("SELECT key,value FROM ki_weights").fetchall()
        conn.close()
        for k,v in rows:
            if k.startswith("bl_"): _learned_blacklist.add(k[3:])
            elif k in _signal_weights: _signal_weights[k] = v
        log(f"KI: Gewichtungen geladen | Blacklist: {len(_learned_blacklist)}")
    except: pass

def record_trade_ki(pair, score, pnl, profit, ob, taker, btc):
    _trade_memory.append({
        "pair":pair,"score":score,"pnl":pnl,"profit":profit,
        "ob":ob,"taker":taker,"btc":btc,
        "win": profit > 0 if profit is not None else pnl > 0,
        "time": datetime.now().strftime("%H:%M"),
    })
    if len(_trade_memory) > 50: _trade_memory.pop(0)

def ki_lern_analyse():
    """Jede Stunde: Lerne aus abgeschlossenen Trades"""
    global _last_ki_analysis, _signal_weights, _learned_blacklist
    now = time.time()
    if now - _last_ki_analysis < KI_INTERVAL: return
    if len(_trade_memory) < 5: return
    _last_ki_analysis = now

    wins   = [t for t in _trade_memory if t["win"]]
    losses = [t for t in _trade_memory if not t["win"]]
    if not _trade_memory: return
    wr = len(wins) / len(_trade_memory)

    # Regelbasiertes Lernen
    def adjust(key, good_signal):
        if good_signal:
            _signal_weights[key] = min(2.0, _signal_weights[key] + 0.05)
        else:
            _signal_weights[key] = max(0.5, _signal_weights[key] - 0.05)

    # OB und Taker Analyse
    if wins:
        avg_ob_win  = sum(t["ob"] for t in wins) / len(wins)
        avg_ob_loss = sum(t["ob"] for t in losses) / len(losses) if losses else avg_ob_win
        adjust("orderbook", avg_ob_win > avg_ob_loss)

        avg_tk_win  = sum(t["taker"] for t in wins) / len(wins)
        avg_tk_loss = sum(t["taker"] for t in losses) / len(losses) if losses else avg_tk_win
        adjust("taker", avg_tk_win > avg_tk_loss)

    # BTC Analyse
    btc_wins  = [t for t in wins   if t["btc"] == 1]
    btc_neut  = [t for t in wins   if t["btc"] == 0]
    adjust("btc", len(btc_wins) > len(btc_neut))

    # Score Analyse
    if wins and losses:
        avg_score_win  = sum(t["score"] for t in wins) / len(wins)
        avg_score_loss = sum(t["score"] for t in losses) / len(losses)
        adjust("rsi", avg_score_win > avg_score_loss)

    # Schlechte Pairs merken
    pair_stats = {}
    for t in _trade_memory:
        p = t["pair"]
        if p not in pair_stats: pair_stats[p] = [0,0]
        if t["win"]: pair_stats[p][0] += 1
        else:        pair_stats[p][1] += 1
    for p, (w, l) in pair_stats.items():
        if l >= 2 and w == 0:
            _learned_blacklist.add(p)
            log(f"KI BLACKLIST: {p} (zu viele Verluste)")

    # Claude API fuer tiefere Analyse
    try:
        api_key = os.getenv("ANTHROPIC_API_KEY","")
        if api_key and len(_trade_memory) >= 10:
            import json as _j
            weights_str = _j.dumps(_signal_weights)
            trades_str  = _j.dumps(_trade_memory[-5:])
            prompt = (
                f"Crypto Bot Optimierung. {len(_trade_memory)} Trades "
                f"WR={wr:.0%}. Gewichtungen: {weights_str}. "
                f"Letzte 5: {trades_str}. "
                "Gib NUR JSON: weights rsi 1.0 reasoning 1 Satz"
            )
            resp = requests.post(
                "https://api.anthropic.com/v1/messages",
                headers={"x-api-key":api_key,
                         "anthropic-version":"2023-06-01",
                         "content-type":"application/json"},
                json={"model":"claude-sonnet-4-20250514","max_tokens":200,
                      "messages":[{"role":"user","content":prompt}]},
                timeout=12)
            if resp.status_code == 200:
                raw = resp.json()["content"][0]["text"]
                s = raw.find("{"); e = raw.rfind("}")+1
                if s >= 0:
                    data = _j.loads(raw[s:e])
                    for k,v in data.get("weights",{}).items():
                        if k in _signal_weights:
                            delta = max(-0.10, min(0.10, float(v) - _signal_weights[k]))
                            _signal_weights[k] = round(_signal_weights[k]+delta, 3)
                    log(f"KI Claude: {data.get('reasoning','')}")
    except Exception as e: log(f"KI API: {e}")

    log(f"KI LERN: WR={wr:.0%} | Weights={_signal_weights}")
    save_ki_weights()

# ?? KI KONSENSUS: 4 AGENTEN STIMMEN AB ????????????????????????????????????????
def _agent_vote_rules(agent_id, score, ob, taker, btc, rsi_v, fg, phase):
    """Regelbasierter Fallback wenn keine API"""
    if agent_id == 0:  # Trend-Agent
        return "BUY" if (score>=8 and btc>=0 and 42<=rsi_v<=68) else "SKIP"
    if agent_id == 1:  # Risiko-Agent
        return "BUY" if (ob>=1.5 and taker>=0.62 and fg<75) else "SKIP"
    if agent_id == 2:  # Sentiment-Agent
        return "BUY" if (taker>=0.60 and phase!="bear" and fg>=35) else "SKIP"
    if agent_id == 3:  # Contrarian-Agent (strenger)
        return "BUY" if (score>=11 and ob>=1.7 and taker>=0.65) else "SKIP"
    return "SKIP"

def _ask_claude_agent(name, system_p, context, api_key):
    """Fragt einen Claude-Agenten"""
    try:
        resp = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={"x-api-key":api_key,
                     "anthropic-version":"2023-06-01",
                     "content-type":"application/json"},
            json={"model":"claude-sonnet-4-20250514","max_tokens":10,
                  "system":system_p,
                  "messages":[{"role":"user","content":context}]},
            timeout=8)
        if resp.status_code == 200:
            t = resp.json()["content"][0]["text"].strip().upper()
            return "BUY" if "BUY" in t else "SKIP"
    except: pass
    return None

def ki_konsensus(pair, score, ob, taker, btc, rsi_v, fg, phase):
    """
    4 KI-Agenten stimmen ab.
    Braucht 3/4 Stimmen fuer BUY.
    Trend-KI | Risiko-KI | Sentiment-KI | Contrarian-KI
    """
    now = time.time()
    cached = _consensus_cache.get(pair)
    if cached and now - cached[0] < CONSENSUS_TTL:
        return cached[1]

    api_key = os.getenv("ANTHROPIC_API_KEY","")
    # Whale Daten fuer Kontext
    whale_str = ""
    w_data = _whale_cache.get(pair)
    if w_data and w_data[1]:
        wd = w_data[1]
        if wd["bid_wall"]:   whale_str = f" WHALE-BID-WALL:{wd['bid_whale']:.0f}x"
        elif wd["ask_wall"]: whale_str = f" ASK-WALL:{wd['ask_whale']:.0f}x"

    context = (
        f"Pair:{pair} Score:{score:.1f}/30 RSI:{rsi_v:.0f} "
        f"Phase:{phase} FG:{fg} OB:{ob:.2f}x Taker:{taker:.0%} "
        f"BTC:{'bull' if btc==1 else 'bear' if btc==-1 else 'neutral'}"
        f"{whale_str} Stop:1.2% TP:5.2% - Antworte NUR: BUY oder SKIP"
    )

    agent_defs = [
        ("TREND",
         "Du bist Trend-Spezialist. BUY wenn Score>8, RSI 42-68, BTC neutral/bullish, OB>1.4x. Sonst SKIP."),
        ("RISIKO",
         "Du bist Risikomanager. BUY wenn OB>1.5x, Taker>62%, Score>9, FG<75. Sonst SKIP."),
        ("SENTIMENT",
         "Du bist Sentiment-Analyst. BUY wenn Taker>60%, FG 35-70, Phase nicht bear. Sonst SKIP."),
        ("CONTRARIAN",
         "Du bist kritischer Contrarian. Hinterfrage JEDEN Trade. BUY NUR wenn Score>11 UND OB>1.7x UND Taker>65%. Sonst immer SKIP."),
    ]

    votes      = []
    vote_log   = []

    if api_key:
        # Alle 4 Agenten parallel fragen
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as ex:
            futs = {ex.submit(_ask_claude_agent, n, s, context, api_key): (i,n)
                    for i,(n,s) in enumerate(agent_defs)}
            for fut in concurrent.futures.as_completed(futs, timeout=12):
                i, name = futs[fut]
                try:
                    v = fut.result()
                    if v is None:  # API Fehler -> Regelbasierter Fallback
                        v = _agent_vote_rules(i, score, ob, taker, btc, rsi_v, fg, phase)
                except:
                    v = _agent_vote_rules(i, score, ob, taker, btc, rsi_v, fg, phase)
                votes.append(v)
                vote_log.append(f"{name}:{v}")
    else:
        # Vollstaendig regelbasierter Konsensus
        for i, (name, _) in enumerate(agent_defs):
            v = _agent_vote_rules(i, score, ob, taker, btc, rsi_v, fg, phase)
            votes.append(v)
            vote_log.append(f"{name}:{v}")

    buy_count = votes.count("BUY")
    decision  = "BUY" if buy_count >= 3 else "SKIP"

    log(f"KONSENSUS {pair}: {' | '.join(vote_log)} => {decision} ({buy_count}/4)")
    result = (decision, buy_count, vote_log)
    _consensus_cache[pair] = (now, result)
    return result


# ?? WHALE DETECTION (DOM / ORDER FLOW) ???????????????????????????????????????
# Erkennt grosse Orders im Orderbook - "Spotting the Whales"
# Bid Wall = starke Unterstuetzung = Kaufsignal
# Ask Wall = starker Widerstand    = kein Kauf

_whale_cache = {}
WHALE_TTL    = 15  # 15s Cache


def calc_correlation(pair1, pair2):
    d1 = _corr_data.get(pair1, [])
    d2 = _corr_data.get(pair2, [])
    n  = min(len(d1), len(d2))
    if n < 5: return 0.0
    d1 = d1[-n:]; d2 = d2[-n:]
    r1 = [(d1[i]-d1[i-1])/d1[i-1] for i in range(1,n)]
    r2 = [(d2[i]-d2[i-1])/d2[i-1] for i in range(1,n)]
    if not r1: return 0.0
    m1 = sum(r1)/len(r1); m2 = sum(r2)/len(r2)
    cov  = sum((r1[i]-m1)*(r2[i]-m2) for i in range(len(r1)))
    std1 = (sum((x-m1)**2 for x in r1))**0.5
    std2 = (sum((x-m2)**2 for x in r2))**0.5
    return cov/(std1*std2) if std1*std2 > 0 else 0.0

def update_corr_data(pair, price):
    if pair not in _corr_data: _corr_data[pair] = []
    _corr_data[pair].append(price)
    if len(_corr_data[pair]) > 20: _corr_data[pair].pop(0)

def is_too_correlated(pair, open_positions):
    high_corr = 0
    for op in open_positions:
        if abs(calc_correlation(pair, op)) > 0.7:
            high_corr += 1
    return high_corr >= 2

def get_whale_signals(client, pair):
    """
    Analysiert das Orderbook nach Whale-Orders.
    Gibt zurueck:
      bid_wall      - starke Kaufmauer gefunden (bullish)
      ask_wall      - starke Verkaufsmauer gefunden (bearish)
      whale_ratio   - Groesste Order vs Durchschnitt (>5x = Whale)
      support_level - Preisniveau der Bid Wall
      resist_level  - Preisniveau der Ask Wall
    """
    now = time.time()
    cached = _whale_cache.get(pair)
    if cached and now - cached[0] < WHALE_TTL:
        return cached[1]

    rate_limit()
    try:
        book  = client.get_product_book(product_id=pair, limit=20)
        bids  = book.pricebook.bids
        asks  = book.pricebook.asks

        if not bids or not asks:
            return None

        # Bid Analyse
        bid_sizes  = [float(b.size) * float(b.price) for b in bids[:15]]
        ask_sizes  = [float(a.size) * float(a.price) for a in asks[:15]]

        if not bid_sizes or not ask_sizes:
            return None

        avg_bid = sum(bid_sizes) / len(bid_sizes)
        avg_ask = sum(ask_sizes) / len(ask_sizes)

        # Groesste einzelne Order finden
        max_bid_idx  = bid_sizes.index(max(bid_sizes))
        max_ask_idx  = ask_sizes.index(max(ask_sizes))

        max_bid_size = bid_sizes[max_bid_idx]
        max_ask_size = ask_sizes[max_ask_idx]

        bid_whale_ratio = max_bid_size / avg_bid if avg_bid > 0 else 1.0
        ask_whale_ratio = max_ask_size / avg_ask if avg_ask > 0 else 1.0

        support_level = float(bids[max_bid_idx].price)
        resist_level  = float(asks[max_ask_idx].price)
        current_bid   = float(bids[0].price)

        # Bid Wall: grosse Kauforder nahe aktuellem Preis (innerhalb 1%)
        bid_wall = (bid_whale_ratio >= 5.0 and
                    abs(support_level - current_bid) / current_bid < 0.01)

        # Ask Wall: grosse Verkaufsorder nahe aktuellem Preis (innerhalb 1%)
        ask_wall = (ask_whale_ratio >= 5.0 and
                    abs(resist_level - current_bid) / current_bid < 0.01)

        # Gesamtes Orderbook Tiefe
        total_bid_depth = sum(bid_sizes[:10])
        total_ask_depth = sum(ask_sizes[:10])
        depth_ratio     = total_bid_depth / total_ask_depth if total_ask_depth > 0 else 1.0

        result = {
            "bid_wall":      bid_wall,
            "ask_wall":      ask_wall,
            "bid_whale":     round(bid_whale_ratio, 1),
            "ask_whale":     round(ask_whale_ratio, 1),
            "support":       support_level,
            "resistance":    resist_level,
            "depth_ratio":   round(depth_ratio, 2),
            "current":       current_bid,
        }

        _whale_cache[pair] = (now, result)
        return result

    except Exception as e:
        return cached[1] if cached else None

def run():
    global current_target_index, start_total, last_known_total, wins, losses, positions, scored_signals
    global perp_positions, perp_wins, perp_losses, perp_trade_log
    global consecutive_losses, circuit_breaker_until, daily_start_total, daily_loss_limit_hit

    init_db()
    client    = create_client()
    all_pairs = get_pairs(client)
    positions = sync_positions(client)

    global trade_log, milestones_reached
    trade_log          = load_trades()
    milestones_reached = load_milestones()
    if milestones_reached:
        done = {m["target"] for m in milestones_reached}
        for i, t in enumerate(TARGETS):
            if t not in done:
                current_target_index = i; break
        else:
            current_target_index = len(TARGETS)
    wins   = sum(1 for t in trade_log if t.get("pnl") and t["pnl"] > 0)
    losses = sum(1 for t in trade_log if t.get("pnl") and t["pnl"] < 0)
    log(f"DB: {len(trade_log)} Trades | {len(milestones_reached)} Meilensteine")

    load_ki_weights()
    load_ki_weights()
    log("ULTIMATE SCALPING BOT v6.2 + KI KONSENSUS")
    log(f"Signale: RSI+EMA+MACD+BB+VWAP+StochRSI+Orderbook+MarketTrades+BTC")
    log(f"Pairs:{len(all_pairs)} | Interval:{CHECK_INTERVAL}s | "
        f"Stop:{LONG_STOP_PCT*100:.1f}% | TP:{LONG_TP_PCT*100:.1f}%")

    cycle    = 0
    fg       = 50
    phase    = "sideways"
    fg_timer = ph_timer = 0
    best_scored = []

    while True:
        try:
            cycle += 1
            now_ts = time.time()

            if now_ts - fg_timer > 300:
                fg = get_fear_greed(); fg_timer = now_ts
            if now_ts - ph_timer > 300:
                phase = get_market_phase(client); ph_timer = now_ts

            if cycle % 20 == 0:
                positions = sync_positions(client)

            btc_t = btc_context(client)
            usdc  = get_balance(client, "USDC")
            total = usdc

            for pair in list(positions.keys()):
                bid, _ = get_pricebook(client, pair)
                if bid > 0: update_corr_data(pair, bid)  # Korrelation
                if now_ts - recently_bought.get(pair, 0) < PHANTOM_GUARD:
                    total += positions[pair]["amount"] * bid; continue
                real = verify_position(client, pair)
                if real == 0.0:
                    positions.pop(pair, None); continue
                positions[pair]["amount"] = real
                total += real * bid

            if start_total is None: start_total = total

            # ── AUSZAHLUNGS-ERKENNUNG ────────────────────────────────
            # Wenn Balance sinkt OHNE dass der Bot verkauft hat
            # → manuelle Auszahlung → daily_start und start_total anpassen
            if last_known_total is not None:
                drop = last_known_total - total
                # Mehr als $2 Rückgang ohne Bot-Verkauf in diesem Cycle?
                if drop > 2.0 and len(positions) == len([p for p in positions]):
                    # Prüfe ob Bot in diesem Cycle verkauft hat
                    # (trade_log letzte Einträge)
                    recent_sells = [t for t in trade_log[-3:]
                                    if t.get("action") == "SELL"]
                    if not recent_sells:
                        log(f"AUSZAHLUNG ERKANNT: -${drop:.2f} → "
                            f"P&L-Basis angepasst")
                        if daily_start_total is not None:
                            daily_start_total -= drop
                        start_total -= drop
            last_known_total = total

            btc_str = "UP" if btc_t==1 else "DN" if btc_t==-1 else "--"
            log(f"Cycle={cycle} | ${total:.4f} | USDC=${usdc:.2f} | "
                f"Pos={len(positions)} | {phase} | FG={fg} | BTC={btc_str}")

            check_milestone(total)

            # KI Analyse jede Stunde
            if cycle % 360 == 0:
                threading.Thread(target=ki_lern_analyse, daemon=True).start()

            # ?? SELL: Stop / TP / Trailing ????????????????????????????????
            for pair in list(positions.keys()):
                pos  = positions[pair]
                bid, _ = get_pricebook(client, pair)
                if bid <= 0: continue
                entry = pos["buy"]; peak = pos["peak"]

                if bid > peak:
                    peak = bid; pos["peak"] = peak
                    update_peak(pair, peak)

                pnl      = (bid - entry) / entry * 100
                is_manual = pos.get("manual", 0) == 1
                # Manuelle Trades: erst bei +15% verkaufen, kein Stop
                if is_manual:
                    stop     = entry * 0.0   # kein Stop fuer manuelle Trades
                    tp       = entry * 1.15  # +15% TP
                    trail    = entry * 0.85  # Trail ab +15% mit 1.2%
                    eff_stop = peak * (1 - LONG_TRAIL_PCT) if peak >= entry * 1.15 else 0.0
                else:
                    # Nacht-Positionen: engerer Stop (1.0% statt 1.5%)
                    night_pos = pos.get("night_mode", False)
                    stop_pct  = 0.010 if night_pos else LONG_STOP_PCT
                    stop     = entry * (1 - stop_pct)
                    tp       = entry * (1 + LONG_TP_PCT)
                    trail    = peak  * (1 - LONG_TRAIL_PCT)
                    break_even = entry * (1 + TOTAL_FEE_PCT + 0.002)
                    # Trailing nur aktiv wenn ueber Break-Even
                    if peak > break_even:
                        eff_stop = max(stop, trail, break_even)
                    else:
                        eff_stop = stop

                if now_ts - recently_bought.get(pair, 0) < PHANTOM_GUARD:
                    size = pos.get("amount", 0)
                else:
                    size = verify_position(client, pair)
                    if size == 0.0:
                        positions.pop(pair, None); continue

                if is_manual:
                    log(f"  {pair} [MANUELL] PnL={pnl:+.2f}% | TP@+15%={tp:.5f} | Trail={eff_stop:.5f}")
                else:
                    log(f"  {pair} PnL={pnl:+.2f}% | Stop={eff_stop:.5f} | TP={tp:.5f}")

                reason = None
                if is_manual:
                    # Kein Stop fuer manuelle Trades
                    # Erst bei +15% verkaufen, danach Trailing
                    if bid >= tp:
                        reason = "TAKE PROFIT +15%"
                    elif eff_stop > 0 and bid <= eff_stop:
                        reason = "TRAILING (nach +15%)"
                else:
                    if bid <= eff_stop: reason = "STOP"
                    elif bid >= tp:     reason = "TAKE PROFIT"

                if reason:
                    log(f"SELL {pair} | {reason} | PnL={pnl:+.2f}%")
                    sell_size = size
                    if reason == "PARTIAL_TP":
                        sell_size = round(size * 0.5, 8)  # 50% verkaufen
                        pos["partial_done"] = True
                        pos["amount"] = size - sell_size
                        # Stop auf Break-Even anheben
                        pos["stop_raised"] = True
                        log(f"PARTIAL TP {pair}: 50% verkauft, Stop -> Break-Even")
                    if market_sell(client, pair, sell_size if reason=="PARTIAL_TP" else size):
                        actual_size = sell_size if reason == "PARTIAL_TP" else size
                        sell_value  = actual_size * bid
                        usdc_spent  = pos.get("usdc_spent", size * entry)
                        buy_fee     = usdc_spent * MAKER_FEE_PCT
                        sell_fee    = sell_value * MAKER_FEE_PCT
                        total_fee   = buy_fee + sell_fee
                        profit_usdc = sell_value - usdc_spent - total_fee
                        log_trade("SELL", pair, bid, pnl,
                                  round(sell_value,4), round(total_fee,4),
                                  round(profit_usdc,4))
                        record_trade_ki(
                            pair, score=0, pnl=pnl,
                            profit=profit_usdc,
                            ob=_book_cache.get(pair,(0,1.0))[1],
                            taker=_trades_cache.get(pair,(0,0.5))[1],
                            btc=btc_t)
                        if pnl > 0:
                            wins += 1
                            consecutive_losses = 0
                        else:
                            losses += 1
                            consecutive_losses += 1
                            if consecutive_losses >= 3:
                                circuit_breaker_until = time.time() + 7200
                                log(f"CIRCUIT BREAKER: 3 Verluste - Pause 2h")
                                consecutive_losses = 0
                        if reason != "PARTIAL_TP":
                            delete_position(pair)
                            positions.pop(pair, None)
                        else:
                            save_position(pair, entry, peak,
                                         pos["amount"], pos["usdc_spent"]*0.5)
                        usdc = get_balance(client, "USDC")

            # ?? SCAN & BUY ????????????????????????????????????????????????
            # Tages-Reset um Mitternacht
            now_hour = datetime.now().hour
            if now_hour == 0 and daily_start_total is not None:
                if datetime.now().minute < 1:
                    daily_start_total = None
                    daily_loss_limit_hit = False
                    log("Neuer Tag: Tages-Verlust-Limit zurueckgesetzt")
            if daily_start_total is None:
                daily_start_total = total
            daily_pct = (total - daily_start_total) / daily_start_total * 100
            if daily_pct <= -3.0 and not daily_loss_limit_hit:
                daily_loss_limit_hit = True
                log(f"TAGES-VERLUST LIMIT: -{abs(daily_pct):.1f}% - kein Kauf bis Mitternacht")
            cb_active = time.time() < circuit_breaker_until
            if cb_active:
                remaining = int((circuit_breaker_until - time.time()) / 60)
                if cycle % 10 == 0:
                    log(f"CIRCUIT BREAKER aktiv: noch {remaining}min Pause")
            # Time-of-Day Filter
            utc_hour   = datetime.now().hour  # UTC+0 Railway Server
            good_hours = list(range(8,13)) + list(range(13,18)) + list(range(20,24))
            night_hours = list(range(0,7))   # 00-06 UTC = Nacht-Modus
            time_ok    = utc_hour in good_hours or utc_hour in night_hours
            night_mode = utc_hour in night_hours  # Halbe Position, engerer Stop

            can_buy = (fg > 25 and fg < 78 and  # v6.8: etwas weiter
                       len(positions) < MAX_POSITIONS and
                       usdc >= MIN_ORDER_USDC and
                       btc_t >= 0 and
                       not cb_active and
                       not daily_loss_limit_hit and
                       time_ok)

            if not time_ok and cycle % 30 == 0:
                log(f"TIME FILTER: {utc_hour}:xx UTC - keine neuen Kaeufe")

            scan_now = (len(positions) == 0) or (cycle % 3 == 1)

            if can_buy and scan_now:
                scan_pairs = [p for p in all_pairs
                              if p not in BLACKLIST
                              and p not in _learned_blacklist
                              and p not in positions]
                new_scored = []

                for pair in scan_pairs:
                    if pair in pair_error_time:
                        if now_ts - pair_error_time[pair] < ERROR_COOLDOWN:
                            continue
                    if pair in last_trade_time:
                        if now_ts - last_trade_time[pair] < COOLDOWN:
                            continue

                    result = validate_pair(client, pair)
                    if result is None: continue
                    bid, ask = result

                    # Mindest-Volumen $500k/Tag (v6.6: erhöht für Liquidität)
                    vol24 = get_volume_24h(client, pair)
                    vol_usd = vol24 * bid
                    if vol_usd < 500_000:
                        continue  # Zu illiquide
                    # Bonus-Score für sehr liquide Coins (>$5M/Tag)
                    if vol_usd > 5_000_000:
                        pass  # wird im Score berücksichtigt

                    data = get_candles(client, pair)
                    if not data or len(data["close"]) < 20: continue

                    score = compute_score(client, pair, data, btc_t)
                    if score >= MIN_SCORE:
                        new_scored.append((pair, score, ask, data))

                new_scored.sort(key=lambda x: x[1], reverse=True)
                best_scored = new_scored
                scored_signals = new_scored

                if new_scored:
                    log(f"Scan: {len(scan_pairs)} Pairs | "
                        f"{len(new_scored)} Signale | "
                        f"Top3: {[(p,round(s,1)) for p,s,_,_ in new_scored[:3]]}")
                else:
                    log(f"Scan: {len(scan_pairs)} Pairs | 0 Signale")

            # Kaufe Top Signale
            if can_buy:
                buys = 0
                for pair, score, price, data in best_scored[:5]:
                    if buys >= 2: break
                    if len(positions) >= MAX_POSITIONS: break
                    if pair in positions or usdc < MIN_ORDER_USDC: continue
                    if pair in last_trade_time:
                        if now_ts - last_trade_time[pair] < COOLDOWN:
                            continue

                    pos_usdc = calc_position_size(usdc, score)
                    # Nacht-Modus: halbe Position (00-07 UTC)
                    if night_mode:
                        pos_usdc = pos_usdc * 0.5
                        pos_usdc = max(pos_usdc, MIN_ORDER_USDC)
                    if pos_usdc < MIN_ORDER_USDC: continue

                    # KI KONSENSUS: Alle 4 Agenten muessen zustimmen
                if not data or not data.get("close"):
                    continue
                rsi_now = rsi(data["close"]) if data else 50
                _ki_result = ki_konsensus(
                    pair=pair, score=score,
                    ob=_book_cache.get(pair, (0,1.0))[1],
                    taker=_trades_cache.get(pair, (0,0.5))[1],
                    btc=btc_t, rsi_v=rsi_now,
                    phase=phase, fg=fg)
                consensus, buy_v, total_v = _ki_result[0], _ki_result[1], 4
                details = _ki_result[2]

                if consensus != "BUY":
                    log(f"SKIP {pair}: KI Konsensus {buy_v}/{total_v} (brauche 3/4)")
                    continue

                # Korrelations-Filter: max 2 korrelierende Pairs
                if is_too_correlated(pair, positions):
                    log(f"SKIP {pair}: zu stark korreliert mit offenen Positionen")
                    continue

                # Dynamischer Stop: knapp unter Whale Bid Wall
                dyn_stop = LONG_STOP_PCT
                wh_d = _whale_cache.get(pair)
                if wh_d and wh_d[1] and wh_d[1].get("bid_wall"):
                    sp = wh_d[1]["support"]
                    if sp > 0 and price > sp:
                        dist = (price - sp) / price
                        if 0 < dist < LONG_STOP_PCT:
                            dyn_stop = dist + 0.003
                            log(f"WHALE-STOP {pair}: {dyn_stop*100:.2f}%")

                log(f"BUY {pair} Score={score:.1f} Konsensus={buy_v}/4 "
                    f"${pos_usdc:.2f} Stop={dyn_stop*100:.1f}%")

                if market_buy(client, pair, pos_usdc):
                        amount  = pos_usdc / price
                        buy_fee = round(pos_usdc * MAKER_FEE_PCT, 6)
                        positions[pair] = {"buy":price,"peak":price,
                                           "amount":amount,"usdc_spent":pos_usdc}
                        save_position(pair, price, price, amount, pos_usdc)
                        last_trade_time[pair]  = now_ts
                        recently_bought[pair]  = now_ts
                        log_trade("BUY", pair, price,
                                  usdc_amount=round(pos_usdc,4),
                                  fee_usdc=round(buy_fee,4))
                        usdc = get_balance(client, "USDC")
                        buys += 1
                        best_scored = [(p,s,pr,d) for p,s,pr,d in best_scored
                                       if p != pair]

            # ?? DASHBOARD UPDATE ?????????????????????????????????????????
            total_fees   = round(sum(t.get("fee_usdc") or 0 for t in trade_log), 4)
            total_profit = round(sum(t.get("profit_usdc") or 0 for t in trade_log
                                     if t.get("profit_usdc") is not None), 4)
            pos_list = []
            for pair, pos in positions.items():
                bid, _ = get_pricebook(client, pair)
                pp = (bid - pos["buy"]) / pos["buy"] * 100 if pos["buy"] else 0
                pd = (bid - pos["peak"]) / pos["peak"] * 100 if pos["peak"] else 0
                pos_list.append({"pair":pair,"buy":pos["buy"],"now":bid,
                                 "pnl":round(pp,2),"drop":round(pd,2)})

            # ── PERP CYCLE ──────────────────────────────────────
            if PERP_ENABLED and cycle % 3 == 0:
                # Aktuelle Preise für Perp-Pairs sammeln
                perp_bids = {}
                for pp in PERP_PAIRS:
                    try:
                        bid, _ = get_pricebook(client, pp)
                        if bid: perp_bids[pp] = bid
                    except: pass
                perp_cycle(client, perp_bids, _candle_cache, fg, btc_t, usdc)

            dashboard_state.update({
                "whale_signals": {p: _whale_cache[p][1]
                                  for p in list(_whale_cache.keys())[:5]
                                  if _whale_cache[p][1]},
                "ki_weights": dict(_signal_weights),
                "ki_blacklist": list(_learned_blacklist),
                "ki_trades_analyzed": len(_trade_memory),
                "circuit_breaker": cb_active,
                "daily_loss_hit": daily_loss_limit_hit,
                "daily_pct": round(daily_pct if daily_start_total else 0, 2),
                "consec_losses": consecutive_losses,
                "total":round(total,4), "usdc_free":round(usdc,4),
                "in_positions":round(total-usdc,4),
                "today_pnl":round(total-start_total,4),
                "cycle":cycle, "phase":phase, "fear_greed":fg,
                "wins":wins, "losses":losses,
                "total_fees":total_fees, "total_profit":total_profit,
                "milestones_done":[m["target"] for m in milestones_reached],
                "current_target_idx":current_target_index,
                "trades":list(reversed(trade_log[-30:])),
                "positions":pos_list,
                "top_signals":[{"pair":p,"score":round(s,1)}
                               for p,s,_,_ in (scored_signals or [])[:5]],
                "perp_positions":[{
                    "pair":p,
                    "side":v["side"],
                    "entry":v["entry"],
                    "now":perp_bids.get(p, v["entry"]) if "perp_bids" in dir() else v["entry"],
                    "pnl":round(((v["entry"]-perp_bids.get(p,v["entry"]))/v["entry"] if v["side"]=="SHORT"
                                 else (perp_bids.get(p,v["entry"])-v["entry"])/v["entry"])
                                * PERP_LEVERAGE * 100, 2) if perp_positions else 0,
                    "margin":v["margin"],
                    "stop":v["stop"],
                    "tp":v["tp"],
                } for p,v in perp_positions.items()],
                "perp_wins":perp_wins,
                "perp_losses":perp_losses,
                "perp_trades":list(reversed(perp_trade_log[-20:])),
                "perp_enabled":PERP_ENABLED,
                "perp_leverage":PERP_LEVERAGE,
            })
            dashboard_state["history"].append(round(total,4))
            if len(dashboard_state["history"]) > 120:
                dashboard_state["history"].pop(0)

            # Tageskurve: alle 60s einen Punkt speichern
            now_dt = datetime.now()
            if now_dt.second < 12:  # nur einmal pro Minute (~10s cycle)
                ts_label = now_dt.strftime("%H:%M")
                save_chart_point(now_dt.strftime("%Y-%m-%d %H:%M"), round(total,4))
                # Tageskurve aus DB laden
                today_date, labels, values = load_daily_chart()
                dashboard_state["daily_chart"] = values
                dashboard_state["daily_chart_labels"] = labels
                dashboard_state["daily_start_date"] = today_date
                # Wochen/Monatskurve alle 5 Minuten laden
                if now_dt.minute % 5 == 0:
                    wl, wv = load_week_chart()
                    dashboard_state["week_chart"] = wv
                    dashboard_state["week_chart_labels"] = wl
                    ml, mv = load_month_chart()
                    dashboard_state["month_chart"] = mv
                    dashboard_state["month_chart_labels"] = ml
                # Aufräumen alter Daten (täglich)
                if now_dt.hour == 0 and now_dt.minute == 0 and now_dt.second < 12:
                    cleanup_old_chart()

        except Exception as e:
            log(f"MAIN ERR: {e}")
            import traceback; traceback.print_exc()

        time.sleep(CHECK_INTERVAL)

# ?? FLASK ?????????????????????????????????????????????????????????????????????
app = Flask(__name__)

DASH_HTML = ""
if os.path.exists("dashboard.html"):
    with open("dashboard.html") as f:
        DASH_HTML = f.read()
else:
    DASH_HTML = ("<html><body style='background:#050709;color:#00ff88;"
                 "font-family:monospace;padding:20px'>"
                 "<h2>SCALPING BOT v6.0</h2>"
                 "<a href='/api/status' style='color:#00e5ff'>/api/status</a>"
                 "</body></html>")

@app.route("/")
def index():
    return Response(DASH_HTML, mimetype="text/html")

@app.route("/api/status")
def api_status():
    return jsonify(dashboard_state)

def start_flask():
    port = int(os.getenv("PORT", 8080))
    log(f"Dashboard auf Port {port}")
    app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)

if __name__ == "__main__":
    if not API_KEY or not PRIVATE_KEY:
        log("FEHLER: API Keys fehlen!")
        exit(1)
    threading.Thread(target=start_flask, daemon=True).start()
    run()
