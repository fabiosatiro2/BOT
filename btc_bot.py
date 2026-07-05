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
import pathlib

def _resolve_db_path():
    """
    Ermittelt den DB-Pfad und prueft OB das Volume wirklich persistent ist.
    Problem bisher: mkdir('/data') gelingt auch OHNE gemountetes Volume
    (Railway legt dann einen fluechtigen Ordner an -> DB weg nach Neustart).
    Dieser Check schreibt eine Marker-Datei und prueft beim naechsten Start
    ob sie noch da ist. So sehen wir im Log eindeutig ob das Volume haelt.
    """
    target = os.getenv("DB_PATH", "/data/trader.db")
    data_dir = os.path.dirname(target) or "."

    # 1) Versuche das Verzeichnis anzulegen / zu nutzen
    try:
        pathlib.Path(data_dir).mkdir(parents=True, exist_ok=True)
    except Exception as e:
        print(f"[DB] WARN: kann {data_dir} nicht anlegen ({e}) -> lokale DB")
        return "trader.db", False

    # 2) Schreibtest: ist das Verzeichnis ueberhaupt beschreibbar?
    test_file = os.path.join(data_dir, ".write_test")
    try:
        with open(test_file, "w") as f:
            f.write("ok")
        os.remove(test_file)
    except Exception as e:
        print(f"[DB] WARN: {data_dir} nicht beschreibbar ({e}) -> lokale DB")
        return "trader.db", False

    # 3) Persistenz-Marker: zeigt im Log ob Volume ueber Neustarts haelt
    marker = os.path.join(data_dir, ".volume_marker")
    if os.path.exists(marker):
        try:
            with open(marker) as f:
                first_seen = f.read().strip()
            print(f"[DB] Volume PERSISTENT — Marker seit {first_seen} vorhanden")
        except: pass
    else:
        stamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        try:
            with open(marker, "w") as f:
                f.write(stamp)
            print(f"[DB] Volume-Marker NEU angelegt ({stamp}). "
                  f"Wenn diese Zeile bei JEDEM Neustart kommt, ist KEIN "
                  f"echtes Volume gemountet!")
        except: pass

    print(f"[DB] Nutze Datenbank: {target}")
    return target, True

DB_NAME, DB_PERSISTENT = _resolve_db_path()
TARGETS          = [5, 25, 100, 250, 500, 1000, 2500, 5000]
CHECK_INTERVAL   = 10
MAX_POSITIONS    = int(os.getenv("MAX_POSITIONS", "3"))   # QUALITY: 4->3
MIN_ORDER_USDC   = 20.0   # v6.9: €200 Portfolio
# ── GEBUEHREN (ECHT statt geschoent) ──────────────────────────
# BUGFIX: Vorher rechnete die Profit-Anzeige mit MAKER_FEE_PCT=0.0
# -> Dashboard zeigte "Fees €0.00" und Trades mit +1.4% PnL waren
# real Verluste. Jetzt: konservative Defaults, beim Start werden
# die ECHTEN Account-Gebuehren von der Coinbase-API geholt.
# Override per Railway-Variablen FEE_MAKER / FEE_TAKER moeglich.
TAKER_FEE_PCT    = float(os.getenv("FEE_TAKER", "0.012"))
MAKER_FEE_PCT    = float(os.getenv("FEE_MAKER", "0.006"))
TOTAL_FEE_PCT    = TAKER_FEE_PCT * 2   # Round-Trip, wird per API aktualisiert
# Trailing wird erst scharf wenn der Peak ueber Break-Even PLUS
# diesem Mindest-Nettogewinn liegt -> kein Trailing-Exit im Minus mehr
MIN_NET_PROFIT_PCT = float(os.getenv("MIN_NET_PROFIT_PCT", "0.005"))

# Long/Short Parameter
LONG_STOP_PCT    = float(os.getenv("LONG_STOP_PCT", "0.015"))
# QUALITY: TP 7% -> 10%. Bei ~2.4% Round-Trip-Fees sinkt der
# Fee-Anteil am Bruttogewinn von 34% auf 24%.
LONG_TP_PCT      = float(os.getenv("LONG_TP_PCT", "0.10"))
LONG_TRAIL_PCT   = 0.012   # v6.7: Kompromiss Sicherung/Luft
MIN_SCORE        = 10.5   # v6.9: €200 Portfolio

# ── LEARNING MODE (Sideways/Recovery kleine Lern-Trades) ──────
# Bot tradet auch in ruhigen Phasen mit kleinen Orders ($10-15),
# damit die KI auch diese Marktphasen kennenlernt. Wenig Risiko,
# aber echte Trade-Daten fuers KI-Lernmodul (record_trade_ki).
LEARN_MODE       = os.getenv("LEARN_MODE", "true").lower() == "true"
LEARN_MIN_ORDER  = 10.0   # kleinste Lern-Order USDC
LEARN_MAX_ORDER  = 15.0   # groesste Lern-Order USDC
LEARN_MIN_SCORE  = 7.5    # niedrigere Schwelle zum Lernen
LEARN_MAX_POS    = int(os.getenv("LEARN_MAX_POS", "2"))  # Lehrgeld-Deckel: 3->2

# QUALITY-MODUS: normale Trades brauchen diesen Bonus AUF die
# Regime-Schwelle -> weniger, aber bessere Signale. Lern-Trades
# sind davon ausgenommen (die sollen ja bewusst breiter lernen).
QUALITY_SCORE_BONUS = float(os.getenv("QUALITY_SCORE_BONUS", "1.5"))
MAX_BUYS_PER_CYCLE  = int(os.getenv("MAX_BUYS_PER_CYCLE", "1"))  # war 2
LEARN_REGIMES    = ("SIDEWAYS", "RECOVERY")  # wann gelernt wird

# ── TAGESZIELE (EUR-basiert) ──────────────────────────────────
# Profit-Ziel: bei Erreichen wird der Bot konservativer (hoehere
# Schwelle, halbe Positionsgroesse) um den Tagesgewinn zu sichern.
# Verlust-Limit: absoluter EUR-Betrag statt Prozent.
DAILY_PROFIT_TARGET = float(os.getenv("DAILY_PROFIT_TARGET", "5.0"))
DAILY_LOSS_LIMIT    = float(os.getenv("DAILY_LOSS_LIMIT", "6.50"))

# Position Sizing
BASE_RISK_PCT    = 0.26   # 77% WR v6.2
MAX_RISK_PCT     = float(os.getenv("MAX_RISK_PCT", "0.25"))  # QUALITY: ~€57 max
MIN_RISK_PCT     = float(os.getenv("MIN_RISK_PCT", "0.15"))  # QUALITY: ~€34 min

# ── PERP SETTINGS ─────────────────────────────────────────────
PERP_ENABLED     = os.getenv("PERP_ENABLED", "true").lower() == "true"
INTX_API_KEY     = os.getenv("INTX_API_KEY", "")
INTX_PRIVATE_KEY = os.getenv("INTX_PRIVATE_KEY", "")
INTX_BASE        = "https://api.international.coinbase.com"
PERP_LEVERAGE    = int(os.getenv("PERP_LEVERAGE", "5"))
PERP_MAX_POS     = 3
PERP_MIN_MARGIN  = 10.0   # angepasst für kleines Perp-Konto
PERP_MAX_MARGIN  = 50.0   # max 50$ pro Perp-Trade
PERP_ALLOC       = 0.90   # 90% des Perp-Guthabens nutzen
PERP_STOP_PCT    = 0.010   # 1% Stop = 5% mit 5x Leverage
PERP_TP_PCT      = 0.050   # 5% TP = 25% mit 5x Leverage
PERP_TRAIL_PCT   = 0.012
PERP_MIN_SCORE_L = 10.5   # Long Signal Schwelle
PERP_MIN_SCORE_S = 10.5   # Short Signal Schwelle


# ══════════════════════════════════════════════════════════════════
# TELEGRAM ALERTS
# ══════════════════════════════════════════════════════════════════
TELEGRAM_TOKEN   = os.getenv("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

def tg(msg):
    """Telegram Nachricht senden"""
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": msg, "parse_mode": "HTML"},
            timeout=5)
    except: pass

def tg_buy(pair, price, usdc, score, strategy):
    tg(f"🟢 <b>KAUF</b> {pair}\n"
       f"💰 Einsatz: €{usdc:.2f}\n"
       f"📈 Preis: ${price:.4f}\n"
       f"⭐ Score: {score:.1f}\n"
       f"🎯 Strategie: {strategy}\n"
       f"🛑 Stop: -{LONG_STOP_PCT*100:.1f}% | TP: +{LONG_TP_PCT*100:.1f}%")

def tg_sell(pair, price, pnl, profit, reason, net_pct=None):
    icon = "✅" if profit > 0 else "❌"
    np = f" | netto {net_pct:+.2f}%" if net_pct is not None else ""
    tg(f"{icon} <b>VERKAUF</b> {pair}\n"
       f"📊 PnL: {pnl:+.2f}%{np}\n"
       f"💵 Profit: €{profit:+.2f} (nach Fees)\n"
       f"📌 Grund: {reason}\n"
       f"💲 Preis: ${price:.4f}")

def tg_perp_open(pair, side, price, margin, leverage):
    icon = "🔵" if side == "LONG" else "🔴"
    tg(f"{icon} <b>PERP {side}</b> {pair}\n"
       f"💰 Margin: ${margin:.0f} | {leverage}x\n"
       f"📈 Entry: ${price:.4f}\n"
       f"🎯 TP: +{PERP_TP_PCT*100:.1f}% | Stop: -{PERP_STOP_PCT*100:.1f}%")

def tg_perp_close(pair, side, pnl_pct, profit, reason):
    icon = "✅" if profit > 0 else "❌"
    tg(f"{icon} <b>PERP CLOSE</b> {pair} ({side})\n"
       f"📊 PnL: {pnl_pct:+.1f}% (mit Leverage)\n"
       f"💵 Profit: ${profit:+.2f}\n"
       f"📌 Grund: {reason}")

def tg_learn(pair, price, usdc, score, regime):
    tg(f"📚 <b>LERN-TRADE</b> {pair}\n"
       f"💰 Einsatz: €{usdc:.2f} (klein)\n"
       f"📈 Preis: ${price:.4f}\n"
       f"⭐ Score: {score:.1f}\n"
       f"🧪 Regime: {regime}\n"
       f"💡 Bot lernt diese Marktphase kennen")

def tg_alert(msg):
    tg(f"⚠️ <b>ALERT</b>\n{msg}")

def tg_daily_summary(total, pnl, wins, losses, fees):
    wr = wins/(wins+losses)*100 if (wins+losses) > 0 else 0
    icon = "📈" if pnl > 0 else "📉"
    tg(f"{icon} <b>TAGES-ZUSAMMENFASSUNG</b>\n"
       f"💼 Portfolio: €{total:.2f}\n"
       f"📊 Heute P&L: €{pnl:+.2f}\n"
       f"✅ Wins: {wins} | ❌ Losses: {losses}\n"
       f"🎯 Win Rate: {wr:.0f}%\n"
       f"💸 Fees: €{fees:.2f}")

# ── TIMING ──────────────────────────────────────────────────────
COOLDOWN         = int(os.getenv("PAIR_COOLDOWN", "900"))  # QUALITY: 60s->15min pro Pair
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
daily_target_hit     = False
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

# ── PERP GLOBALS ────────────────────────────────────────────────
perp_positions   = {}
perp_wins        = 0
perp_losses      = 0
perp_trade_log   = []
PERP_PAIRS = [
    "BTC-USDC","ETH-USDC","SOL-USDC","XRP-USDC",
    "AVAX-USDC","LINK-USDC","DOGE-USDC","ADA-USDC",
    "DOT-USDC","LTC-USDC",
]

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
    try: c.execute("ALTER TABLE positions ADD COLUMN learn INTEGER DEFAULT 0")
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

def save_position(pair, entry, peak, amount, usdc_spent, manual=0, learn=0):
    conn = sqlite3.connect(DB_NAME)
    conn.execute("INSERT OR REPLACE INTO positions "
                 "(pair,entry,peak,amount,usdc_spent,opened_at,manual,learn) VALUES (?,?,?,?,?,?,?,?)",
                 (pair,entry,peak,amount,usdc_spent,
                  datetime.now().strftime("%Y-%m-%d %H:%M:%S"), manual, learn))
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
        "SELECT pair,entry,peak,amount,usdc_spent,manual,learn FROM positions").fetchall()
    conn.close()
    return {r[0]: {"buy":r[1],"peak":r[2],"amount":r[3],
                   "usdc_spent":r[4] or r[3]*r[1],"manual":r[5] or 0,
                   "learn":r[6] or 0} for r in rows}

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

def fetch_real_fees(client):
    """Holt die ECHTEN Gebuehren des Accounts von der Coinbase-API.
    Vorher rechnete der Bot mit 0% Maker -> Trades mit +1.4% Brutto-PnL
    waren real Verluste. Jetzt zeigt das Dashboard die Wahrheit."""
    global TAKER_FEE_PCT, MAKER_FEE_PCT, TOTAL_FEE_PCT
    try:
        ts = client.get_transaction_summary()
        ft = getattr(ts, "fee_tier", None)
        if ft is None and isinstance(ts, dict):
            ft = ts.get("fee_tier")
        def _get(obj, key, default):
            if obj is None: return default
            if isinstance(obj, dict): return obj.get(key, default)
            return getattr(obj, key, default)
        mk = float(_get(ft, "maker_fee_rate", MAKER_FEE_PCT))
        tk = float(_get(ft, "taker_fee_rate", TAKER_FEE_PCT))
        if 0 < mk < 0.05 and 0 < tk < 0.05:
            MAKER_FEE_PCT = mk
            TAKER_FEE_PCT = tk
            TOTAL_FEE_PCT = tk * 2
            log(f"FEES (Coinbase API): Maker {mk*100:.3f}% | "
                f"Taker {tk*100:.3f}% | Round-Trip {TOTAL_FEE_PCT*100:.2f}%")
            return True
    except Exception as e:
        log(f"FEE API ERR: {e}")
    log(f"FEES (Fallback/ENV): Maker {MAKER_FEE_PCT*100:.2f}% | "
        f"Taker {TAKER_FEE_PCT*100:.2f}% | Round-Trip {TOTAL_FEE_PCT*100:.2f}%")
    return False

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


# ── COINGLASS FUNDING RATE ──────────────────────────────────────
_funding_cache = {}
FUNDING_TTL = 300  # 5 Minuten Cache

def get_funding_rate(coin):
    """
    Coinglass Funding Rate API (public, keine Auth nötig)
    Positiv = Longs zahlen (teuer für Longs, gut für Shorts)
    Negativ = Shorts zahlen (gut für Longs)
    """
    now = time.time()
    cached = _funding_cache.get(coin)
    if cached and now - cached[0] < FUNDING_TTL:
        return cached[1]
    try:
        # Coinbase Perpetuals Funding (public API)
        symbol = coin.replace("-USDC", "").replace("-USD", "")
        url = f"https://api.coinglass.com/public/v2/funding?symbol={symbol}&exchangeName=Coinbase"
        r = requests.get(url, timeout=6,
                        headers={"User-Agent": "Mozilla/5.0"})
        if r.status_code == 200:
            data = r.json()
            rate = float(data.get("data", [{}])[0].get("fundingRate", 0))
            _funding_cache[coin] = (now, rate)
            return rate
    except: pass
    # Fallback: Binance Funding Rate (sehr ähnlich)
    try:
        symbol = coin.replace("-USDC", "USDT").replace("-USD", "USDT")
        url = f"https://fapi.binance.com/fapi/v1/premiumIndex?symbol={symbol}"
        r = requests.get(url, timeout=6)
        if r.status_code == 200:
            rate = float(r.json().get("lastFundingRate", 0))
            _funding_cache[coin] = (now, rate)
            return rate
    except: pass
    _funding_cache[coin] = (now, 0.0)
    return 0.0

def funding_score(pair):
    """
    Score Beitrag aus Funding Rate:
    Stark negativ (Shorts zahlen) → Long sehr attraktiv → +3
    Leicht negativ                → Long attraktiv      → +1.5
    Neutral                       → kein Einfluss       → 0
    Stark positiv (Longs zahlen)  → Long teuer          → -2
    """
    coin = pair.replace("-USDC", "")
    rate = get_funding_rate(coin)
    if rate < -0.001:   return 3.0, f"FR:{rate*100:.3f}%"
    elif rate < -0.0005: return 1.5, f"FR:{rate*100:.3f}%"
    elif rate > 0.002:  return -2.0, f"FR:{rate*100:.3f}%"
    elif rate > 0.001:  return -1.0, f"FR:{rate*100:.3f}%"
    return 0.0, ""

# ── CRYPTOPANIC NEWS SENTIMENT ──────────────────────────────────
_news_cache = {}
NEWS_TTL = 180  # 3 Minuten Cache
CRYPTOPANIC_KEY = os.getenv("CRYPTOPANIC_KEY", "")  # Optional

def get_news_sentiment(coin):
    """
    CryptoPanic News Sentiment für einen Coin.
    Gibt zurück: score (-3 bis +3), signal_str
    Positiv = bullishe News = Kaufsignal
    Negativ = bearishe News = kein Kauf
    """
    now = time.time()
    cached = _news_cache.get(coin)
    if cached and now - cached[0] < NEWS_TTL:
        return cached[1], cached[2]

    try:
        # CryptoPanic public API (kein Key nötig für basic)
        url = f"https://cryptopanic.com/api/v1/posts/?auth_token={CRYPTOPANIC_KEY}&currencies={coin}&filter=hot&public=true"
        if not CRYPTOPANIC_KEY:
            # Ohne Key: nur public posts
            url = f"https://cryptopanic.com/api/v1/posts/?currencies={coin}&filter=hot&public=true"
        r = requests.get(url, timeout=6)
        if r.status_code != 200:
            _news_cache[coin] = (now, 0.0, "")
            return 0.0, ""

        results = r.json().get("results", [])
        if not results:
            _news_cache[coin] = (now, 0.0, "")
            return 0.0, ""

        # Letzte 30 Minuten analysieren
        cutoff = now - 1800
        recent = []
        for post in results[:20]:
            try:
                from datetime import timezone
                pub = post.get("published_at", "")
                # Parse ISO timestamp
                import re as _re
                nums = _re.findall(r'\d+', pub)
                if len(nums) >= 6:
                    from datetime import datetime as _dt
                    dt = _dt(int(nums[0]),int(nums[1]),int(nums[2]),
                            int(nums[3]),int(nums[4]),int(nums[5]),
                            tzinfo=timezone.utc)
                    if dt.timestamp() > cutoff:
                        recent.append(post)
            except: continue

        if not recent:
            _news_cache[coin] = (now, 0.0, "")
            return 0.0, ""

        # Votes analysieren
        bullish = sum(p.get("votes", {}).get("positive", 0) for p in recent)
        bearish = sum(p.get("votes", {}).get("negative", 0) for p in recent)
        total   = bullish + bearish

        if total == 0:
            # Nur Anzahl News zählt
            news_count = len(recent)
            if news_count >= 5:
                score = 2.0
                sig = f"NEWS:{news_count}hot"
            elif news_count >= 3:
                score = 1.0
                sig = f"NEWS:{news_count}"
            else:
                score = 0.0
                sig = ""
        else:
            ratio = bullish / total
            if ratio > 0.75:
                score = 3.0
                sig = f"NEWS+:{int(ratio*100)}%"
            elif ratio > 0.60:
                score = 1.5
                sig = f"NEWS:{int(ratio*100)}%"
            elif ratio < 0.30:
                score = -2.0
                sig = f"NEWS-:{int((1-ratio)*100)}%"
            else:
                score = 0.0
                sig = ""

        _news_cache[coin] = (now, score, sig)
        return score, sig

    except Exception as e:
        _news_cache[coin] = (now, 0.0, "")
        return 0.0, ""


# ══════════════════════════════════════════════════════════════════
# SOCIAL INTELLIGENCE MODULE
# LunarCrush Galaxy Score + Whale Alert On-Chain Signals
# ══════════════════════════════════════════════════════════════════

LUNARCRUSH_KEY = os.getenv("LUNARCRUSH_KEY", "")
WHALE_ALERT_KEY = os.getenv("WHALE_ALERT_KEY", "")

_lunar_cache = {}
_whale_cache2 = {}
LUNAR_TTL = 300   # 5 Min Cache
WHALE_TTL2 = 120  # 2 Min Cache

# ── LUNARCRUSH: Galaxy Score + Sentiment ─────────────────────────
def get_lunar_data(coin):
    """
    LunarCrush API v4:
    - Galaxy Score: 0-100 (Gesamtgesundheit des Assets)
    - Sentiment: 0-100 (Social Stimmung)
    - AltRank: niedrig = stark (1 = bestes Asset)
    - Social Dominance: Anteil am Crypto Social Traffic
    """
    now = time.time()
    cached = _lunar_cache.get(coin)
    if cached and now - cached[0] < LUNAR_TTL:
        return cached[1]

    if not LUNARCRUSH_KEY:
        _lunar_cache[coin] = (now, None)
        return None

    try:
        symbol = coin.replace("-USDC","").replace("-USD","").lower()
        url = f"https://lunarcrush.com/api4/public/coins/{symbol}/v1"
        r = requests.get(url, timeout=8,
                        headers={"Authorization": f"Bearer {LUNARCRUSH_KEY}"})
        if r.status_code == 200:
            d = r.json().get("data", {})
            result = {
                "galaxy_score":    d.get("galaxy_score", 50),
                "sentiment":       d.get("sentiment", 50),
                "alt_rank":        d.get("alt_rank", 999),
                "social_dominance": d.get("social_dominance", 0),
                "interactions_24h": d.get("interactions_24h", 0),
            }
            _lunar_cache[coin] = (now, result)
            return result
    except Exception as e:
        log(f"LUNAR ERR {coin}: {e}")

    _lunar_cache[coin] = (now, None)
    return None

def lunar_score(pair):
    """
    Score-Beitrag aus LunarCrush:
    Galaxy Score > 70 + Sentiment > 65 → stark bullish → +3
    Galaxy Score > 55 + Sentiment > 55 → bullish → +1.5
    Galaxy Score < 30 oder Sentiment < 35 → bearish → -2
    AltRank < 20 → Top-Asset social momentum → +1
    """
    coin = pair.replace("-USDC","")
    d = get_lunar_data(coin)
    if not d: return 0.0, ""

    gs   = d["galaxy_score"]
    sent = d["sentiment"]
    rank = d["alt_rank"]
    score = 0.0
    sigs  = []

    if gs > 70 and sent > 65:
        score += 3.0; sigs.append(f"LC:{gs:.0f}↑")
    elif gs > 55 and sent > 55:
        score += 1.5; sigs.append(f"LC:{gs:.0f}")
    elif gs < 30 or sent < 35:
        score -= 2.0; sigs.append(f"LC:{gs:.0f}↓")

    if rank < 20:
        score += 1.0; sigs.append(f"Rank#{rank}")
    elif rank < 50:
        score += 0.5

    return score, ",".join(sigs)

# ── WHALE ALERT: Große On-Chain Transfers ────────────────────────
def get_whale_transfers(coin, min_usd=500_000):
    """
    Whale Alert API: Große Crypto-Transfers in letzten 5 Min
    Großer Exchange-Inflow → Verkaufsdruck → negativ
    Großer Exchange-Outflow → HODLing → positiv
    Große Wallet-zu-Wallet → neutrale Bewegung
    """
    now = time.time()
    cached = _whale_cache2.get(coin)
    if cached and now - cached[0] < WHALE_TTL2:
        return cached[1]

    if not WHALE_ALERT_KEY:
        _whale_cache2[coin] = (now, None)
        return None

    try:
        symbol = coin.replace("-USDC","").replace("-USD","").lower()
        url = "https://api.whale-alert.io/v1/transactions"
        params = {
            "api_key": WHALE_ALERT_KEY,
            "min_value": min_usd,
            "limit": 10,
            "start": int(now - 300),  # letzte 5 Minuten
            "currency": symbol,
        }
        r = requests.get(url, params=params, timeout=8)
        if r.status_code == 200:
            txs = r.json().get("transactions", [])
            result = {
                "total_txs": len(txs),
                "exchange_inflow":  sum(t["amount_usd"] for t in txs
                                       if t.get("to", {}).get("owner_type") == "exchange"),
                "exchange_outflow": sum(t["amount_usd"] for t in txs
                                       if t.get("from", {}).get("owner_type") == "exchange"),
                "largest_tx": max((t["amount_usd"] for t in txs), default=0),
            }
            _whale_cache2[coin] = (now, result)
            return result
    except Exception as e:
        log(f"WHALE ALERT ERR {coin}: {e}")

    _whale_cache2[coin] = (now, None)
    return None

def whale_alert_score(pair):
    """
    Score-Beitrag aus Whale Alert:
    Hoher Exchange-Outflow → HODLing → bullish → +2
    Hoher Exchange-Inflow → Verkauf droht → bearish → -2
    Sehr großer einzelner Transfer → Aufmerksamkeit → +1
    """
    coin = pair.replace("-USDC","")
    d = get_whale_transfers(coin)
    if not d: return 0.0, ""

    inflow  = d["exchange_inflow"]
    outflow = d["exchange_outflow"]
    largest = d["largest_tx"]
    score = 0.0
    sigs  = []

    net = outflow - inflow
    if net > 1_000_000:
        score += 2.0; sigs.append(f"WHALE-OUT:${net/1e6:.1f}M")
    elif net > 500_000:
        score += 1.0; sigs.append(f"WHALE-OUT:${net/1e6:.1f}M")
    elif net < -1_000_000:
        score -= 2.0; sigs.append(f"WHALE-IN:${abs(net)/1e6:.1f}M")
    elif net < -500_000:
        score -= 1.0

    if largest > 5_000_000:
        score += 1.0; sigs.append(f"BIG-TX:${largest/1e6:.1f}M")

    return score, ",".join(sigs)

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

    # ── FUNDING RATE ──────────────────────────────────────────────
    try:
        fr_score, fr_sig = funding_score(pair)
        score += fr_score
        if fr_sig: signals.append(fr_sig)
    except: pass

    # ── NEWS SENTIMENT ────────────────────────────────────────────
    try:
        coin = pair.replace("-USDC","")
        ns_score, ns_sig = get_news_sentiment(coin)
        score += ns_score
        if ns_sig: signals.append(ns_sig)
    except: pass

    # ── LUNARCRUSH SOCIAL INTELLIGENCE ───────────────────────────
    try:
        lc_score, lc_sig = lunar_score(pair)
        score += lc_score
        if lc_sig: signals.append(lc_sig)
    except: pass

    # ── WHALE ALERT ON-CHAIN ───────────────────────────────────────
    try:
        wa_score, wa_sig = whale_alert_score(pair)
        score += wa_score
        if wa_sig: signals.append(wa_sig)
    except: pass

    if signals:
        log(f"  {pair} Score={score:.1f} [{', '.join(signals[:8])}]")

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
            time.sleep(int(os.getenv("LIMIT_WAIT_SEC", "8")))  # laenger warten -> oefter Maker (halbe Fee)
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
KI_INTERVAL        = 900   # 15min (war 1h) - schnelleres Lernen
CONSENSUS_TTL      = 120   # 2min
# KI-Modell fuer Konsensus-Agenten + Lern-Analyse.
# Per Railway-Variable KI_MODEL aenderbar ohne Code-Push.
# claude-fable-5 = staerkstes Modell ($10/M in, $50/M out)
KI_MODEL           = os.getenv("KI_MODEL", "claude-fable-5")

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

def record_trade_ki(pair, score, pnl, profit, ob, taker, btc, learn=0, regime=""):
    entry = {
        "pair":pair,"score":score,"pnl":pnl,"profit":profit,
        "ob":ob,"taker":taker,"btc":btc,
        "learn":learn,"regime":regime,
        "win": profit > 0 if profit is not None else pnl > 0,
        "time": datetime.now().strftime("%H:%M"),
    }
    _trade_memory.append(entry)
    if len(_trade_memory) > 50: _trade_memory.pop(0)
    # In DB persistieren: Lern-Memory ueberlebt Neustarts (mit Volume)
    try:
        conn = sqlite3.connect(DB_NAME)
        conn.execute("""CREATE TABLE IF NOT EXISTS ki_memory(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            pair TEXT, score REAL, pnl REAL, profit REAL,
            ob REAL, taker REAL, btc INTEGER, learn INTEGER,
            regime TEXT, win INTEGER, created_at TEXT)""")
        conn.execute(
            "INSERT INTO ki_memory (pair,score,pnl,profit,ob,taker,btc,"
            "learn,regime,win,created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            (pair, score, pnl, profit, ob, taker, btc, learn, regime,
             1 if entry["win"] else 0,
             datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
        # Nur letzte 200 behalten
        conn.execute("""DELETE FROM ki_memory WHERE id NOT IN
            (SELECT id FROM ki_memory ORDER BY id DESC LIMIT 200)""")
        conn.commit(); conn.close()
    except Exception as e:
        log(f"KI MEMORY DB ERR: {e}")

def load_ki_memory():
    """Laedt das Lern-Memory aus der DB (letzte 50 Trades).
    Vorher war _trade_memory nur im RAM -> bei jedem Neustart
    fing die KI bei Null an."""
    global _trade_memory
    try:
        conn = sqlite3.connect(DB_NAME)
        conn.execute("""CREATE TABLE IF NOT EXISTS ki_memory(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            pair TEXT, score REAL, pnl REAL, profit REAL,
            ob REAL, taker REAL, btc INTEGER, learn INTEGER,
            regime TEXT, win INTEGER, created_at TEXT)""")
        rows = conn.execute(
            "SELECT pair,score,pnl,profit,ob,taker,btc,learn,regime,win,"
            "created_at FROM ki_memory ORDER BY id DESC LIMIT 50").fetchall()
        conn.close()
        for r in reversed(rows):
            _trade_memory.append({
                "pair":r[0],"score":r[1],"pnl":r[2],"profit":r[3],
                "ob":r[4],"taker":r[5],"btc":r[6],"learn":r[7],
                "regime":r[8] or "","win":bool(r[9]),
                "time":(r[10] or "")[11:16],
            })
        if rows:
            log(f"KI MEMORY: {len(rows)} Trades aus DB geladen")
    except Exception as e:
        log(f"KI MEMORY LOAD ERR: {e}")

def ki_lern_analyse():
    """Jede Stunde: Lerne aus abgeschlossenen Trades"""
    global _last_ki_analysis, _signal_weights, _learned_blacklist
    now = time.time()
    if now - _last_ki_analysis < KI_INTERVAL: return
    if len(_trade_memory) < 3: return  # war 5 - frueher lernen
    _last_ki_analysis = now

    wins   = [t for t in _trade_memory if t["win"]]
    losses = [t for t in _trade_memory if not t["win"]]
    if not _trade_memory: return
    wr = len(wins) / len(_trade_memory)
    # Getrennte Auswertung: Lern-Trades vs normale Trades
    learn_trades  = [t for t in _trade_memory if t.get("learn")]
    normal_trades = [t for t in _trade_memory if not t.get("learn")]
    lw = sum(1 for t in learn_trades if t["win"])
    nw = sum(1 for t in normal_trades if t["win"])
    if learn_trades or normal_trades:
        log(f"KI STATS: Normal {nw}/{len(normal_trades)} | "
            f"Lern {lw}/{len(learn_trades)}")

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
        if api_key and len(_trade_memory) >= 5:
            import json as _j
            weights_str = _j.dumps(_signal_weights)
            trades_str  = _j.dumps(_trade_memory[-10:])
            prompt = (
                f"Du bist der Lern-Analyst eines Krypto-Spot-Bots "
                f"(Coinbase, kleine Positionen, Stop ~1.5%, TP ~7%). "
                f"{len(_trade_memory)} Trades, WR={wr:.0%}. "
                f"Aktuelles Regime: {_current_regime}. "
                f"Signal-Gewichtungen: {weights_str}. "
                f"Letzte 10 Trades (learn=1 sind kleine Lern-Trades): "
                f"{trades_str}. "
                f"Analysiere: Welche Signale (ob/taker/score/btc) trennen "
                f"Wins von Losses? Welche Regimes laufen gut/schlecht? "
                f"Antworte NUR mit JSON: "
                f'{{"weights": {{"rsi": 1.0, ...}}, "reasoning": "1 Satz"}}'
            )
            resp = requests.post(
                "https://api.anthropic.com/v1/messages",
                headers={"x-api-key":api_key,
                         "anthropic-version":"2023-06-01",
                         "content-type":"application/json"},
                json={"model":KI_MODEL,"max_tokens":200,
                      "messages":[{"role":"user","content":prompt}]},
                timeout=20)
            if resp.status_code == 200:
                rj = resp.json()
                # Fable 5 kann mit stop_reason "refusal" ablehnen (HTTP 200)
                if rj.get("stop_reason") == "refusal" or not rj.get("content"):
                    log("KI API: Anfrage abgelehnt (refusal) - Regeln bleiben")
                else:
                    raw = rj["content"][0].get("text","")
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
            json={"model":KI_MODEL,"max_tokens":10,
                  "system":system_p,
                  "messages":[{"role":"user","content":context}]},
            timeout=15)
        if resp.status_code == 200:
            rj = resp.json()
            # Fable 5 Refusal -> None = regelbasierter Fallback greift
            if rj.get("stop_reason") == "refusal" or not rj.get("content"):
                return None
            t = rj["content"][0].get("text","").strip().upper()
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
            for fut in concurrent.futures.as_completed(futs, timeout=20):
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


# ══════════════════════════════════════════════════════════════════
# PERP FUNKTIONEN
# ══════════════════════════════════════════════════════════════════

def rsi_value(closes, period=9):
    if len(closes) < period+1: return 50
    gains = losses_r = 0
    for i in range(1, period+1):
        d = closes[-i] - closes[-i-1]
        if d > 0: gains += d
        else: losses_r -= d
    if losses_r == 0: return 100
    return 100 - (100/(1 + gains/losses_r))

def ema_value(closes, p):
    if len(closes) < p: return closes[-1] if closes else 0
    k = 2/(p+1); v = closes[0]
    for c in closes[1:]: v = c*k + v*(1-k)
    return v


# ══════════════════════════════════════════════════════════════════
# INTX ECHTE ORDER FUNKTIONEN
# ══════════════════════════════════════════════════════════════════
import hmac, hashlib, base64

def intx_sign(method, path, body=""):
    """INTX API Signatur"""
    ts = str(int(time.time()))
    msg = ts + method.upper() + path + (body or "")
    try:
        key = parse_ec_key(INTX_PRIVATE_KEY)
        from cryptography.hazmat.primitives.asymmetric import ec
        from cryptography.hazmat.primitives import hashes
        pk = load_pem_private_key(key.encode(), password=None, backend=default_backend())
        sig = pk.sign(msg.encode(), ec.ECDSA(hashes.SHA256()))
        return ts, base64.b64encode(sig).decode()
    except Exception as e:
        log(f"INTX SIGN ERR: {e}")
        return ts, ""

def intx_request(method, path, body=None):
    """INTX API Request"""
    if not INTX_API_KEY or not INTX_PRIVATE_KEY:
        return None
    body_str = json.dumps(body) if body else ""
    ts, sig = intx_sign(method, path, body_str)
    headers = {
        "CB-ACCESS-KEY": INTX_API_KEY,
        "CB-ACCESS-SIGN": sig,
        "CB-ACCESS-TIMESTAMP": ts,
        "Content-Type": "application/json"
    }
    try:
        url = INTX_BASE + path
        r = requests.request(method, url, headers=headers,
                           data=body_str if body_str else None, timeout=10)
        if r.status_code in [200, 201]:
            return r.json()
        log(f"INTX ERR {r.status_code}: {r.text[:100]}")
        return None
    except Exception as e:
        log(f"INTX REQ ERR: {e}")
        return None

def intx_get_portfolio():
    """INTX Portfolio ID holen"""
    data = intx_request("GET", "/api/v1/portfolios")
    if data and "results" in data:
        for p in data["results"]:
            if p.get("default"): return p["portfolio_uuid"]
        return data["results"][0]["portfolio_uuid"]
    return None

_intx_portfolio_id = None

def get_intx_portfolio():
    global _intx_portfolio_id
    if not _intx_portfolio_id:
        _intx_portfolio_id = intx_get_portfolio()
    return _intx_portfolio_id

def intx_get_balance():
    """USDC Balance auf INTX"""
    pid = get_intx_portfolio()
    if not pid: return 0.0
    data = intx_request("GET", f"/api/v1/portfolios/{pid}/balances")
    if not data: return 0.0
    for b in data.get("balances", []):
        if b.get("asset_id") == "USDC":
            return float(b.get("quantity", 0))
    return 0.0

def intx_place_order(side, pair, size, leverage=3):
    """Echte INTX Perp Order platzieren"""
    pid = get_intx_portfolio()
    if not pid:
        log("INTX: Kein Portfolio gefunden")
        return False
    # INTX Pair Format: BTC-PERP
    intx_pair = pair.replace("-USDC", "-PERP")
    body = {
        "portfolio": pid,
        "side": side,          # "BUY" oder "SELL"
        "client_order_id": str(uuid.uuid4()),
        "type": "MARKET",
        "product_id": intx_pair,
        "size": str(round(size, 6)),
    }
    data = intx_request("POST", "/api/v1/orders", body)
    if data:
        log(f"INTX ORDER OK: {side} {intx_pair} size={size}")
        return True
    log(f"INTX ORDER FAILED: {side} {intx_pair}")
    return False

def intx_close_position(pair):
    """INTX Position schließen"""
    pid = get_intx_portfolio()
    if not pid: return False
    intx_pair = pair.replace("-USDC", "-PERP")
    # Aktuelle Position holen
    data = intx_request("GET", f"/api/v1/portfolios/{pid}/positions")
    if not data: return False
    for pos in data.get("positions", []):
        if pos.get("product_id") == intx_pair:
            side = pos.get("side", "LONG")
            size = abs(float(pos.get("net_size", 0)))
            if size > 0:
                close_side = "SELL" if side == "LONG" else "BUY"
                return intx_place_order(close_side, pair, size)
    return False

def perp_score_long(data, fg=50, btc_trend=0):
    closes = data.get("close", [])
    if len(closes) < 20: return 0
    price = closes[-1]; score = 0.0
    r = rsi_value(closes)
    if r < 30:    score += 4.0
    elif r < 40:  score += 2.5
    elif r < 50:  score += 1.0
    elif r > 70:  score -= 2.5
    e9 = ema_value(closes, 9); e21 = ema_value(closes, 21)
    if e9 > e21 and price > e9:    score += 3.0
    elif e9 > e21:                  score += 1.5
    elif e9 < e21 and price < e9:  score -= 1.5
    fast = ema_value(closes, 12); slow = ema_value(closes, 26)
    if fast - slow > 0: score += 2.0
    elif fast - slow < 0: score -= 1.0
    bm = sum(closes[-20:])/20
    std = (sum((x-bm)**2 for x in closes[-20:])/20)**0.5
    bl = bm-2*std; bu = bm+2*std
    if price < bl:    score += 3.0
    elif price < bm:  score += 1.0
    elif price > bu:  score -= 2.0
    vols = data.get("volume", [])
    if len(vols) >= 5:
        avg = sum(vols[-5:-1])/4
        if avg > 0 and vols[-1] > avg*1.5: score += 2.0
    if btc_trend > 0:  score += 1.0
    elif btc_trend < 0: score -= 1.5
    if fg < 30: score -= 1.5
    return round(max(0, score), 2)

def perp_score_short(data, fg=50, btc_trend=0):
    closes = data.get("close", [])
    if len(closes) < 20: return 0
    price = closes[-1]; score = 0.0
    r = rsi_value(closes)
    if r > 70:    score += 4.0
    elif r > 60:  score += 2.5
    elif r > 50:  score += 1.0
    elif r < 30:  score -= 2.5
    e9 = ema_value(closes, 9); e21 = ema_value(closes, 21)
    if e9 < e21 and price < e9:    score += 3.0
    elif e9 < e21:                  score += 1.5
    elif e9 > e21 and price > e9:  score -= 1.5
    fast = ema_value(closes, 12); slow = ema_value(closes, 26)
    if fast - slow < 0: score += 2.0
    elif fast - slow > 0: score -= 1.0
    bm = sum(closes[-20:])/20
    std = (sum((x-bm)**2 for x in closes[-20:])/20)**0.5
    bu = bm+2*std; bl = bm-2*std
    if price > bu:    score += 3.0
    elif price > bm:  score += 1.0
    elif price < bl:  score -= 2.0
    vols = data.get("volume", [])
    if len(vols) >= 5:
        avg = sum(vols[-5:-1])/4
        if avg > 0 and vols[-1] > avg*1.5: score += 2.0
    if btc_trend < 0:  score += 1.0
    elif btc_trend > 0: score -= 1.5
    if fg > 75: score -= 1.5
    return round(max(0, score), 2)

def perp_save(pair, pos):
    conn = sqlite3.connect(DB_NAME)
    try:
        conn.execute("""CREATE TABLE IF NOT EXISTS perp_positions(
            pair TEXT PRIMARY KEY, side TEXT, entry REAL, peak REAL,
            trough REAL, size REAL, margin REAL, stop REAL, tp REAL,
            opened_at TEXT)""")
        conn.execute("INSERT OR REPLACE INTO perp_positions VALUES (?,?,?,?,?,?,?,?,?,?)",
            (pair, pos["side"], pos["entry"], pos["peak"], pos.get("trough", pos["entry"]),
             pos["size"], pos["margin"], pos["stop"], pos["tp"],
             pos.get("opened_at", datetime.now().strftime("%Y-%m-%d %H:%M"))))
        conn.commit()
    finally:
        conn.close()

def perp_delete(pair):
    conn = sqlite3.connect(DB_NAME)
    try:
        conn.execute("DELETE FROM perp_positions WHERE pair=?", (pair,))
        conn.commit()
    finally:
        conn.close()

def perp_load():
    conn = sqlite3.connect(DB_NAME)
    try:
        conn.execute("""CREATE TABLE IF NOT EXISTS perp_positions(
            pair TEXT PRIMARY KEY, side TEXT, entry REAL, peak REAL,
            trough REAL, size REAL, margin REAL, stop REAL, tp REAL,
            opened_at TEXT)""")
        rows = conn.execute("SELECT * FROM perp_positions").fetchall()
        return {r[0]: {"side":r[1],"entry":r[2],"peak":r[3],"trough":r[4],
                       "size":r[5],"margin":r[6],"stop":r[7],"tp":r[8],
                       "opened_at":r[9]} for r in rows}
    finally:
        conn.close()

def perp_open_long(client, pair, price, margin, score):
    global perp_positions
    size = round((margin * PERP_LEVERAGE) / price, 6)
    stop = price * (1 - PERP_STOP_PCT)
    tp   = price * (1 + PERP_TP_PCT)
    # Echte INTX Order wenn Keys vorhanden
    if INTX_API_KEY and INTX_PRIVATE_KEY:
        ok = intx_place_order("BUY", pair, size)
        if not ok:
            log(f"PERP LONG FAILED: {pair}")
            return False
        log(f"PERP LONG REAL {pair} @ {price:.4f} | Margin:${margin:.0f} | {PERP_LEVERAGE}x | Score:{score:.1f}")
        tg_perp_open(pair, "LONG", price, margin, PERP_LEVERAGE)
    else:
        log(f"PERP LONG SIM  {pair} @ {price:.4f} | Margin:${margin:.0f} | {PERP_LEVERAGE}x | Score:{score:.1f} [SIMULATION]")
    pos  = {"side":"LONG","entry":price,"peak":price,"trough":price,
            "size":size,"margin":margin,"stop":stop,"tp":tp,
            "opened_at":datetime.now().strftime("%Y-%m-%d %H:%M")}
    perp_positions[pair] = pos
    perp_save(pair, pos)
    return True

def perp_open_short(client, pair, price, margin, score):
    global perp_positions
    size = round((margin * PERP_LEVERAGE) / price, 6)
    stop = price * (1 + PERP_STOP_PCT)
    tp   = price * (1 - PERP_TP_PCT)
    # Echte INTX Order wenn Keys vorhanden
    if INTX_API_KEY and INTX_PRIVATE_KEY:
        ok = intx_place_order("SELL", pair, size)
        if not ok:
            log(f"PERP SHORT FAILED: {pair}")
            return False
        log(f"PERP SHORT REAL {pair} @ {price:.4f} | Margin:${margin:.0f} | {PERP_LEVERAGE}x | Score:{score:.1f}")
        tg_perp_open(pair, "SHORT", price, margin, PERP_LEVERAGE)
    else:
        log(f"PERP SHORT SIM  {pair} @ {price:.4f} | Margin:${margin:.0f} | {PERP_LEVERAGE}x | Score:{score:.1f} [SIMULATION]")
    pos  = {"side":"SHORT","entry":price,"peak":price,"trough":price,
            "size":size,"margin":margin,"stop":stop,"tp":tp,
            "opened_at":datetime.now().strftime("%Y-%m-%d %H:%M")}
    perp_positions[pair] = pos
    perp_save(pair, pos)
    return True

def perp_close(pair, price, reason):
    global perp_positions, perp_wins, perp_losses, perp_trade_log
    pos = perp_positions.get(pair)
    if not pos: return 0
    side = pos["side"]; entry = pos["entry"]; margin = pos["margin"]
    raw_pnl = (price-entry)/entry if side=="LONG" else (entry-price)/entry
    lev_pnl = raw_pnl * PERP_LEVERAGE
    profit  = margin * lev_pnl
    fee     = margin * PERP_LEVERAGE * TOTAL_FEE_PCT
    net     = profit - fee
    # Echte INTX Position schließen
    if INTX_API_KEY and INTX_PRIVATE_KEY:
        intx_close_position(pair)
    perp_positions.pop(pair, None)
    perp_delete(pair)
    icon = "WIN" if net > 0 else "LOSS"
    log(f"PERP {icon} {side} {pair} | {reason} | PnL={lev_pnl*100:+.1f}% | Netto:${net:+.2f}")
    perp_trade_log.append({
        "pair":pair,"side":side,"action":"CLOSE","price":price,
        "reason":reason,"pnl_pct":round(lev_pnl*100,2),
        "profit_usdc":round(net,4),"margin":margin,
        "time":datetime.now().strftime("%H:%M")
    })
    if net > 0: perp_wins += 1
    else: perp_losses += 1
    return net

def perp_cycle(client, bid_prices, candle_cache, fg, btc_trend, total_usdc):
    global perp_positions
    if not PERP_ENABLED: return

    # Offene Positionen verwalten
    for pair in list(perp_positions.keys()):
        pos   = perp_positions[pair]
        side  = pos["side"]
        entry = pos["entry"]
        bid   = bid_prices.get(pair)
        if not bid: continue
        raw_pnl = (bid-entry)/entry if side=="LONG" else (entry-bid)/entry
        if side == "LONG":
            pos["peak"] = max(pos["peak"], bid)
            trail = pos["peak"] * (1-PERP_TRAIL_PCT)
            eff_stop = max(pos["stop"], trail) if raw_pnl*PERP_LEVERAGE > 0.02 else pos["stop"]
            reason = "STOP" if bid <= eff_stop else ("TAKE PROFIT" if bid >= pos["tp"] else None)
        else:
            pos["trough"] = min(pos.get("trough", bid), bid)
            trail = pos["trough"] * (1+PERP_TRAIL_PCT)
            eff_stop = min(pos["stop"], trail) if raw_pnl*PERP_LEVERAGE > 0.02 else pos["stop"]
            reason = "STOP" if bid >= eff_stop else ("TAKE PROFIT" if bid <= pos["tp"] else None)
        lev_pnl = raw_pnl * PERP_LEVERAGE * 100
        log(f"  PERP {side} {pair} PnL={lev_pnl:+.1f}%")
        if reason:
            perp_close(pair, bid, reason)

    # Neue Positionen öffnen
    perp_budget = total_usdc * PERP_ALLOC
    if len(perp_positions) >= PERP_MAX_POS: return
    if perp_budget < PERP_MIN_MARGIN: return
    if fg <= 25 or fg >= 78: return

    long_sigs = []; short_sigs = []
    for pair in PERP_PAIRS:
        if pair in perp_positions: continue
        bid = bid_prices.get(pair)
        if not bid: continue
        cached = candle_cache.get(pair)
        if not cached: continue
        data = cached[1] if isinstance(cached, tuple) else cached
        if not data or len(data.get("close",[])) < 20: continue
        ls = perp_score_long(data, fg, btc_trend)
        ss = perp_score_short(data, fg, btc_trend)
        if btc_trend < 0 and ls < PERP_MIN_SCORE_L+2: ls = 0
        # Bei bearishem BTC: Short-Score BOOST (+2)
        if btc_trend < 0: ss += 2.0
        if btc_trend > 0 and ss < PERP_MIN_SCORE_S+2: ss = 0
        if ls >= PERP_MIN_SCORE_L: long_sigs.append((pair, ls, bid))
        if ss >= PERP_MIN_SCORE_S: short_sigs.append((pair, ss, bid))

    long_sigs.sort(key=lambda x: x[1], reverse=True)
    short_sigs.sort(key=lambda x: x[1], reverse=True)
    if long_sigs or short_sigs:
        log(f"PERP Signale: {len(long_sigs)}L / {len(short_sigs)}S")

    opened = 0
    for pair, score, price in long_sigs[:1]:
        if opened >= 1 or len(perp_positions) >= PERP_MAX_POS: break
        margin = min(max(perp_budget*0.4, PERP_MIN_MARGIN), PERP_MAX_MARGIN)
        perp_open_long(client, pair, price, margin, score)
        opened += 1
    for pair, score, price in short_sigs[:1]:
        if opened >= 1 or len(perp_positions) >= PERP_MAX_POS: break
        margin = min(max(perp_budget*0.4, PERP_MIN_MARGIN), PERP_MAX_MARGIN)
        perp_open_short(client, pair, price, margin, score)
        opened += 1


def intx_get_open_positions():
    """Alle offenen INTX Positionen laden — auch manuelle"""
    pid = get_intx_portfolio()
    if not pid: return []
    data = intx_request("GET", f"/api/v1/portfolios/{pid}/positions")
    if not data: return []
    positions = []
    for pos in data.get("positions", []):
        try:
            net_size = float(pos.get("net_size", 0))
            if abs(net_size) < 1e-8: continue
            entry = float(pos.get("avg_entry_price", 0))
            mark  = float(pos.get("mark_price", entry))
            side  = "LONG" if net_size > 0 else "SHORT"
            pnl   = float(pos.get("unrealized_pnl", 0))
            pair  = pos.get("product_id", "").replace("-PERP", "-USDC")
            positions.append({
                "pair":   pair,
                "side":   side,
                "entry":  entry,
                "now":    mark,
                "size":   abs(net_size),
                "pnl_usdc": pnl,
                "pnl_pct": ((mark-entry)/entry*100) if entry>0 else 0,
                "margin": float(pos.get("initial_margin", 0)),
                "manual": 1,
                "source": "INTX",
            })
        except: continue
    return positions

def intx_get_balance_full():
    """INTX Gesamtguthaben"""
    pid = get_intx_portfolio()
    if not pid: return 0.0, 0.0
    data = intx_request("GET", f"/api/v1/portfolios/{pid}/summary")
    if not data: return 0.0, 0.0
    usdc = float(data.get("collateral", {}).get("collateral_value", 0))
    unrealized = float(data.get("unrealized_pnl", 0))
    return usdc, unrealized


# ══════════════════════════════════════════════════════════════════
# MARKET INTELLIGENCE MODULE
# Strategy Generation + Risk-Reward + Market Regime + Multi-Factor
# ══════════════════════════════════════════════════════════════════

# ── 1. MARKET REGIME DETECTION ────────────────────────────────────
def detect_market_regime(client, fg=50):
    """
    Erkennt aktuelles Marktregime:
    BULL_TREND  — starker Aufwärtstrend
    BEAR_TREND  — starker Abwärtstrend
    SIDEWAYS    — Seitwärtsmarkt
    HIGH_VOL    — hohe Volatilität / Panik
    RECOVERY    — Erholung nach Crash
    """
    data = get_btc_data(client)
    if not data or len(data["close"]) < 50:
        return "SIDEWAYS", 0.5

    closes = data["close"]
    highs  = data.get("high", closes)
    lows   = data.get("low", closes)

    # Trend
    e9  = ema(closes, 9)  or closes[-1]
    e21 = ema(closes, 21) or closes[-1]
    e50 = ema(closes, 50) or closes[-1]
    r   = rsi(closes) or 50

    # Volatilität (ATR-basiert)
    atr_vals = [max(highs[-i]-lows[-i],
                    abs(highs[-i]-closes[-i-1]),
                    abs(lows[-i]-closes[-i-1]))
                for i in range(1, min(15, len(closes)))]
    atr_pct  = (sum(atr_vals)/len(atr_vals)) / closes[-1] * 100 if atr_vals else 2.0

    # Momentum (5-Candle Rendite)
    momentum = (closes[-1] - closes[-5]) / closes[-5] * 100 if len(closes) >= 5 else 0

    # Regime bestimmen
    if fg < 20 and atr_pct > 3.0:
        regime = "HIGH_VOL"
        confidence = 0.9
    elif e9 > e21 > e50 and r > 55 and momentum > 2:
        regime = "BULL_TREND"
        confidence = min(0.95, 0.6 + momentum/20)
    elif e9 < e21 < e50 and r < 45 and momentum < -2:
        regime = "BEAR_TREND"
        confidence = min(0.95, 0.6 + abs(momentum)/20)
    elif fg < 35 and r > 45 and momentum > 0:
        regime = "RECOVERY"
        confidence = 0.7
    else:
        regime = "SIDEWAYS"
        confidence = 0.6

    return regime, confidence

# Regime Cache
_regime_cache = [0, "SIDEWAYS", 0.5]

def get_regime(client, fg=50):
    now = time.time()
    if now - _regime_cache[0] < 120:
        return _regime_cache[1], _regime_cache[2]
    regime, conf = detect_market_regime(client, fg)
    _regime_cache[0] = now
    _regime_cache[1] = regime
    _regime_cache[2] = conf
    log(f"REGIME: {regime} (Konfidenz: {conf:.0%})")
    return regime, conf

# ── 2. DYNAMIC STRATEGY SELECTION ─────────────────────────────────
def get_strategy_params(regime, confidence):
    """
    Wählt optimale Parameter je nach Marktregime.
    Returns: (stop_pct, tp_pct, trail_pct, min_score, allow_short)
    """
    strategies = {
        "BULL_TREND": {
            "stop":  0.012,   # enger Stop — Trend folgen
            "tp":    0.100,   # weiter TP — Trend ausreiten
            "trail": 0.008,   # enger Trail
            "score": 9.5,     # lockerer Score — Trend ist Freund
            "short": False,   # kein Short im Bullen
            "name":  "Trend Following"
        },
        "BEAR_TREND": {
            "stop":  0.008,   # sehr enger Stop für Longs
            "tp":    0.050,   # kleineres TP
            "trail": 0.010,
            "score": 13.0,    # sehr hoher Score — wenige Longs
            "short": True,    # Shorts erlaubt
            "name":  "Bear Defense + Short"
        },
        "SIDEWAYS": {
            "stop":  0.015,   # Standard Stop
            "tp":    0.060,   # Mean-Reversion TP
            "trail": 0.012,
            "score": 10.5,    # Mittel
            "short": True,    # Shorts an Widerständen
            "name":  "Mean Reversion"
        },
        "HIGH_VOL": {
            "stop":  0.020,   # weiter Stop — hohe Volatilität
            "tp":    0.080,   # großes TP
            "trail": 0.015,
            "score": 12.0,    # hoher Score — nur klare Signale
            "short": True,    # Shorts bei Panik
            "name":  "Volatility Breakout"
        },
        "RECOVERY": {
            "stop":  0.010,   # enger Stop
            "tp":    0.120,   # sehr weites TP — Rebound ausreiten
            "trail": 0.008,
            "score": 10.0,
            "short": False,   # kein Short in Erholung
            "name":  "Recovery Play"
        },
    }
    s = strategies.get(regime, strategies["SIDEWAYS"])
    # Konfidenz-Anpassung: bei niedriger Konfidenz konservativer
    if confidence < 0.7:
        s = dict(s)
        s["score"] = s["score"] + 1.0
        s["tp"]    = s["tp"] * 0.8
    return s

# ── 3. MULTI-FACTOR SCORE ─────────────────────────────────────────
def multi_factor_score(base_score, closes, volumes, fg, btc_trend, regime):
    """
    Kombiniert 4 Faktoren mit dynamischer Gewichtung:
    - Momentum (Preis-Impuls)
    - Volatility (ATR-Niveau)
    - Trend (EMA-Struktur)
    - Value (BB-Position)
    """
    if len(closes) < 20: return base_score

    # Momentum Faktor (0-2)
    mom5  = (closes[-1]-closes[-5])/closes[-5]*100 if len(closes)>=5 else 0
    mom20 = (closes[-1]-closes[-20])/closes[-20]*100 if len(closes)>=20 else 0
    momentum_f = min(2.0, max(-1.0, (mom5*0.7 + mom20*0.3) / 3))

    # Volatility Faktor (0-1.5)
    if len(closes) >= 10:
        returns = [(closes[-i]-closes[-i-1])/closes[-i-1] for i in range(1,10)]
        vol = (sum(r**2 for r in returns)/len(returns))**0.5 * 100
        vol_f = 1.0 if 0.5 < vol < 3.0 else (0.5 if vol > 5.0 else 0.3)
    else:
        vol_f = 1.0

    # Trend Faktor (0-2)
    e9  = ema(closes, 9)  or closes[-1]
    e21 = ema(closes, 21) or closes[-1]
    if e9 > e21 * 1.002:   trend_f = 2.0
    elif e9 > e21:          trend_f = 1.0
    elif e9 < e21 * 0.998: trend_f = -1.0
    else:                   trend_f = 0.0

    # Value Faktor (Bollinger Position)
    bm = sum(closes[-20:])/20
    std = (sum((x-bm)**2 for x in closes[-20:])/20)**0.5
    bb_pos = (closes[-1] - (bm-2*std)) / (4*std) if std > 0 else 0.5
    value_f = 2.0 if bb_pos < 0.2 else (1.0 if bb_pos < 0.4 else (-1.0 if bb_pos > 0.8 else 0.0))

    # Regime-basierte Gewichtung
    weights = {
        "BULL_TREND":  {"mom":0.4, "vol":0.1, "trend":0.4, "value":0.1},
        "BEAR_TREND":  {"mom":0.2, "vol":0.2, "trend":0.4, "value":0.2},
        "SIDEWAYS":    {"mom":0.2, "vol":0.2, "trend":0.2, "value":0.4},
        "HIGH_VOL":    {"mom":0.3, "vol":0.3, "trend":0.2, "value":0.2},
        "RECOVERY":    {"mom":0.3, "vol":0.1, "trend":0.3, "value":0.3},
    }
    w = weights.get(regime, weights["SIDEWAYS"])

    mf_bonus = (momentum_f * w["mom"] + vol_f * w["vol"] +
                trend_f * w["trend"] + value_f * w["value"]) * 3.0

    return round(base_score + mf_bonus, 2)

# ── 4. RISK-REWARD OPTIMIZER ─────────────────────────────────────
def optimize_rr(score, regime, confidence, usdc):
    """
    Berechnet optimale Position + R:R basierend auf:
    - Score Stärke
    - Marktregime
    - Kelly Criterion
    - Konfidenz
    """
    params = get_strategy_params(regime, confidence)

    # Geschätzte Win Rate aus Score
    max_score = 35.0
    strength  = min(max(score / max_score, 0), 1.0)
    est_wr    = 0.50 + strength * 0.30  # 50-80%

    # Kelly Criterion
    rr     = params["tp"] / params["stop"]
    kelly  = est_wr - (1-est_wr)/rr
    kelly  = max(0.05, min(0.25, kelly * 0.5))  # Half-Kelly, max 25%

    # Konfidenz-Anpassung
    kelly *= confidence

    pos_size = usdc * kelly
    pos_size = max(MIN_ORDER_USDC, min(pos_size, usdc * MAX_RISK_PCT))

    return {
        "pos_size": round(pos_size, 2),
        "stop_pct": params["stop"],
        "tp_pct":   params["tp"],
        "trail_pct":params["trail"],
        "est_wr":   round(est_wr, 2),
        "rr_ratio": round(rr, 1),
        "kelly":    round(kelly, 3),
        "strategy": params["name"],
    }

# Regime State für Dashboard
_current_regime    = "SIDEWAYS"
_current_regime_conf = 0.5
_current_strategy  = "Mean Reversion"

def run():
    global current_target_index, start_total, last_known_total, wins, losses, positions, scored_signals
    global perp_positions, perp_wins, perp_losses, perp_trade_log
    global consecutive_losses, circuit_breaker_until, daily_start_total, daily_loss_limit_hit, daily_target_hit

    init_db()
    client    = create_client()
    fetch_real_fees(client)  # echte Account-Gebuehren laden
    all_pairs = get_pairs(client)
    positions = sync_positions(client)

    # BOT_PAIRS: Pairs die als Bot-Positionen behandelt werden (nicht MANUELL)
    bot_pairs = os.getenv("BOT_PAIRS", "").split(",")
    bot_pairs = [p.strip() for p in bot_pairs if p.strip()]
    for pair in bot_pairs:
        if pair in positions and positions[pair].get("manual") == 1:
            positions[pair]["manual"] = 0
            save_position(pair, positions[pair]["buy"], positions[pair]["peak"],
                         positions[pair]["amount"], positions[pair]["usdc_spent"], manual=0)
            log(f"BOT_PAIRS: {pair} als Bot-Position markiert")

    perp_positions = perp_load()
    log(f"Perp-Bot: {len(perp_positions)} offene Perp-Positionen geladen")

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
    load_ki_memory()
    log("ULTIMATE SCALPING BOT v6.2 + KI KONSENSUS")
    tg_alert(f"🚀 Bot gestartet\nPairs: {len(all_pairs)} | Stop: {LONG_STOP_PCT*100:.1f}% | TP: {LONG_TP_PCT*100:.1f}%")
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

            # ── MARKET REGIME ────────────────────────────────────
            if cycle % 12 == 1:  # alle 2 Minuten
                global _current_regime, _current_regime_conf, _current_strategy
                _current_regime, _current_regime_conf = get_regime(client, fg)
                _current_strategy = get_strategy_params(_current_regime, _current_regime_conf)["name"]
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
                    # Trailing erst scharf wenn Peak ueber Break-Even PLUS
                    # Mindest-Nettogewinn liegt. Vorher verkaufte das Trailing
                    # knapp ueberm falschen Break-Even -> "Wins" im Minus.
                    min_lock = entry * (1 + TOTAL_FEE_PCT + MIN_NET_PROFIT_PCT)
                    if peak > min_lock:
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
                        # ECHTE Fees: Verkauf ist immer Market (Taker).
                        # Kauf konservativ auch als Taker gerechnet, da der
                        # Limit-Versuch in schnellen Maerkten oft nicht fuellt.
                        buy_fee     = usdc_spent * TAKER_FEE_PCT
                        sell_fee    = sell_value * TAKER_FEE_PCT
                        total_fee   = buy_fee + sell_fee
                        profit_usdc = sell_value - usdc_spent - total_fee
                        net_pnl_pct = (profit_usdc / usdc_spent * 100
                                       if usdc_spent else pnl)
                        log_trade("SELL", pair, bid, round(net_pnl_pct,2),
                                  round(sell_value,4), round(total_fee,4),
                                  round(profit_usdc,4))
                        tg_sell(pair, bid, pnl, profit_usdc, reason,
                                net_pct=net_pnl_pct)
                        record_trade_ki(
                            pair, score=pos.get("score", 0), pnl=pnl,
                            profit=profit_usdc,
                            ob=_book_cache.get(pair,(0,1.0))[1],
                            taker=_trades_cache.get(pair,(0,0.5))[1],
                            btc=btc_t,
                            learn=pos.get("learn", 0),
                            regime=pos.get("regime", _current_regime))
                        # Win = NETTO-Gewinn nach Fees (vorher zaehlte
                        # Brutto-PnL: +1.4% Trades waren "Wins" trotz Verlust)
                        if profit_usdc > 0:
                            wins += 1
                            consecutive_losses = 0
                        else:
                            losses += 1
                            consecutive_losses += 1
                            if consecutive_losses >= 3:
                                circuit_breaker_until = time.time() + 7200
                                log(f"CIRCUIT BREAKER: 3 Verluste - Pause 2h")
                                tg_alert("Circuit Breaker aktiv — 3 Verluste in Folge. Bot pausiert 2h.")
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
                    # WICHTIG: PnL VOR dem Reset berechnen (Bugfix:
                    # vorher wurde erst genullt, Summary zeigte immer 0)
                    day_pnl = total - daily_start_total
                    tg_daily_summary(
                        total=total,
                        pnl=day_pnl,
                        wins=wins, losses=losses,
                        fees=sum(t.get("fee_usdc",0) or 0 for t in trade_log))
                    daily_start_total = None
                    daily_loss_limit_hit = False
                    daily_target_hit = False
                    log("Neuer Tag: Tages-Limits zurueckgesetzt")
            if daily_start_total is None:
                daily_start_total = total
            # EUR-basierte Tagesbilanz (statt Prozent)
            daily_eur = total - daily_start_total
            daily_pct = daily_eur / daily_start_total * 100 if daily_start_total else 0
            if daily_eur <= -DAILY_LOSS_LIMIT and not daily_loss_limit_hit:
                daily_loss_limit_hit = True
                log(f"TAGES-VERLUST LIMIT: {daily_eur:+.2f}EUR "
                    f"(Limit -{DAILY_LOSS_LIMIT:.2f}) - kein Kauf bis Mitternacht")
                tg_alert(f"🛑 Tages-Verlustlimit erreicht: {daily_eur:+.2f}€\n"
                         f"Kein Kauf mehr bis Mitternacht.")
            # TAGESZIEL: bei Erreichen -> Profit-Lock (konservativer Modus)
            if daily_eur >= DAILY_PROFIT_TARGET and not daily_target_hit:
                daily_target_hit = True
                log(f"TAGESZIEL ERREICHT: {daily_eur:+.2f}EUR "
                    f"(Ziel +{DAILY_PROFIT_TARGET:.2f}) - Profit-Lock aktiv")
                tg_alert(f"🎯 TAGESZIEL erreicht: {daily_eur:+.2f}€!\n"
                         f"Bot wird konservativer um den Gewinn zu sichern:\n"
                         f"• Score-Schwelle +2\n"
                         f"• Halbe Positionsgroesse\n"
                         f"• Keine Lern-Trades mehr heute")
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

            # Learning Mode lockert die Einstiegs-Bedingungen:
            # - FG-Schwelle runter auf 15 (auch bei Extreme Fear lernen)
            # - Mindest-USDC nur LEARN_MIN_ORDER ($10) statt MIN_ORDER ($20)
            learn_active = (LEARN_MODE and _current_regime in LEARN_REGIMES)
            fg_floor   = 15 if learn_active else 25
            min_cash   = LEARN_MIN_ORDER if learn_active else MIN_ORDER_USDC

            can_buy = (fg > fg_floor and fg < 78 and
                       len(positions) < MAX_POSITIONS and
                       usdc >= min_cash and
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
                    # Multi-Factor Enhancement
                    score = multi_factor_score(score, data["close"],
                                               data.get("volume",[]),
                                               fg, btc_t, _current_regime)
                    # Dynamischer MIN_SCORE aus Regime
                    dyn_min = get_strategy_params(_current_regime, _current_regime_conf)["score"]
                    # Im Learning Mode: niedrigere Scan-Schwelle, damit auch
                    # schwaechere Signale fuer Lern-Trades durchkommen
                    scan_floor = min(dyn_min, LEARN_MIN_SCORE) if learn_active else dyn_min
                    if score >= scan_floor:
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
                # LEARNING MODE: in ruhigen Regimes (sideways/recovery) macht
                # der Bot kleine Lern-Trades ($10-15) damit die KI auch diese
                # Marktphasen kennenlernt. is_learn entscheidet pro Trade.
                # PROFIT-LOCK: nach erreichtem Tagesziel keine Lern-Trades mehr.
                is_learn_phase = (LEARN_MODE and
                                  _current_regime in LEARN_REGIMES and
                                  not daily_target_hit)

                for pair, score, price, data in best_scored[:5]:
                    if buys >= MAX_BUYS_PER_CYCLE: break
                    if len(positions) >= MAX_POSITIONS: break
                    if pair in positions or usdc < LEARN_MIN_ORDER: continue
                    if pair in last_trade_time:
                        if now_ts - last_trade_time[pair] < COOLDOWN:
                            continue

                    # Ist DIESER Trade ein Lern-Trade?
                    # Lern-Trade wenn: Lernphase aktiv UND Score zwischen
                    # LEARN_MIN_SCORE und normalem dyn_min (also "zu schwach"
                    # fuer einen echten Trade, aber gut genug zum Lernen)
                    dyn_min = get_strategy_params(
                        _current_regime, _current_regime_conf)["score"]
                    is_learn = (is_learn_phase and score < dyn_min
                                and score >= LEARN_MIN_SCORE)

                    if is_learn:
                        # Lern-Trade Limit pruefen (max LEARN_MAX_POS gleichzeitig)
                        learn_open = sum(1 for p in positions.values()
                                         if p.get("learn"))
                        if learn_open >= LEARN_MAX_POS:
                            continue
                        # Kleine feste Order $10-15, skaliert mit Score
                        span = LEARN_MAX_ORDER - LEARN_MIN_ORDER
                        frac = min(1.0, (score - LEARN_MIN_SCORE) / 3.0)
                        pos_usdc = round(LEARN_MIN_ORDER + span * frac, 2)
                        pos_usdc = min(pos_usdc, usdc - 1.0)
                        if pos_usdc < LEARN_MIN_ORDER: continue
                    else:
                        # Normaler Trade: braucht volle MIN_ORDER und MIN_SCORE
                        if usdc < MIN_ORDER_USDC: continue
                        # QUALITY: Bonus auf Schwelle -> nur beste Signale.
                        # PROFIT-LOCK: nach Tagesziel nochmal +2.
                        eff_min = (dyn_min + QUALITY_SCORE_BONUS
                                   + (2.0 if daily_target_hit else 0.0))
                        if score < eff_min: continue
                        # Optimiertes R:R aus Market Intelligence
                        rr_opt = optimize_rr(score, _current_regime,
                                             _current_regime_conf, usdc)
                        pos_usdc = rr_opt["pos_size"]
                        # Nacht-Modus: halbe Position (00-07 UTC)
                        if night_mode:
                            pos_usdc = pos_usdc * 0.5
                        # PROFIT-LOCK: halbe Position nach Tagesziel
                        if daily_target_hit:
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

                    # Lern-Trades brauchen nur 2/4 Konsensus (lockerer zum Lernen)
                    need_votes = 2 if is_learn else 3
                    if buy_v < need_votes:
                        log(f"SKIP {pair}: KI Konsensus {buy_v}/{total_v} "
                            f"(brauche {need_votes}/4)")
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

                    tag = "LERN" if is_learn else "BUY"
                    log(f"{tag} {pair} Score={score:.1f} Konsensus={buy_v}/4 "
                        f"${pos_usdc:.2f} Stop={dyn_stop*100:.1f}% Regime={_current_regime}")

                    if market_buy(client, pair, pos_usdc):
                        amount  = pos_usdc / price
                        buy_fee = round(pos_usdc * TAKER_FEE_PCT, 6)
                        positions[pair] = {"buy":price,"peak":price,
                                           "amount":amount,"usdc_spent":pos_usdc,
                                           "learn": 1 if is_learn else 0,
                                           "score": score,
                                           "regime": _current_regime}
                        save_position(pair, price, price, amount, pos_usdc,
                                      learn=1 if is_learn else 0)
                        last_trade_time[pair]  = now_ts
                        recently_bought[pair]  = now_ts
                        log_trade("BUY", pair, price,
                                  usdc_amount=round(pos_usdc,4),
                                  fee_usdc=round(buy_fee,4))
                        if is_learn:
                            tg_learn(pair, price, pos_usdc, score, _current_regime)
                        else:
                            tg_buy(pair, price, pos_usdc, score, _current_strategy)
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
                                 "pnl":round(pp,2),"drop":round(pd,2),
                                 "manual":pos.get("manual",0)})

            # ── INTX MANUELLE POSITIONEN LADEN ──────────────────
            intx_manual_pos = []
            intx_usdc = 0.0
            intx_unrealized = 0.0
            if INTX_API_KEY and INTX_PRIVATE_KEY and cycle % 6 == 0:
                try:
                    intx_manual_pos = intx_get_open_positions()
                    intx_usdc, intx_unrealized = intx_get_balance_full()
                    if intx_manual_pos:
                        log(f"INTX: {len(intx_manual_pos)} Positionen | "
                            f"Balance: ${intx_usdc:.2f} | PnL: ${intx_unrealized:+.2f}")
                except Exception as e:
                    log(f"INTX LOAD ERR: {e}")

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
                "regime": _current_regime,
                "regime_conf": round(_current_regime_conf, 2),
                "strategy_name": _current_strategy,
                "perp_enabled":PERP_ENABLED,
                "perp_leverage":PERP_LEVERAGE,
                "intx_positions": intx_manual_pos if 'intx_manual_pos' in dir() else [],
                "intx_balance": round(intx_usdc, 2) if 'intx_usdc' in dir() else 0.0,
                "lunarcrush_active": bool(LUNARCRUSH_KEY),
                "whale_alert_active": bool(WHALE_ALERT_KEY),
                "intx_unrealized": round(intx_unrealized, 4) if 'intx_unrealized' in dir() else 0.0,
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
