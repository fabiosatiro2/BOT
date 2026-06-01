Trading Bot v26
Automatisierter Crypto-Trading-Bot für Coinbase Advanced Trade API.

Deployment auf Railway
1. Repository verbinden
GitHub-Repo in Railway importieren → Railway erkennt den Procfile automatisch.

2. Environment Variables setzen
In Railway unter Settings → Variables folgende Keys eintragen:

Variable	Pflicht	Beschreibung
COINBASE_API_KEY	✅	Coinbase Advanced Trade API Key
COINBASE_SECRET	✅	EC Private Key (komplett mit -----BEGIN...)
TELEGRAM_TOKEN	optional	Telegram Bot Token für Benachrichtigungen
TELEGRAM_CHAT_ID	optional	Deine Telegram Chat-ID
DRY_RUN	optional	true = nur simulieren, kein echter Handel
MAX_POSITION_SIZE_USD	optional	Max. Kapitaleinsatz pro Trade (Standard: 5.0)
STOP_LOSS_PCT	optional	Stop-Loss in % (Standard: 1.5)
TAKE_PROFIT_PCT	optional	Take-Profit in % (Standard: 2.5)
MIN_ORDER_VALUE_USD	optional	Mindest-Ordergröße in USD (Standard: 0.25)
3. Service-Typ
Railway → Service → Worker (kein Web-Server nötig).

Lokales Testen
pip install -r requirements.txt

export COINBASE_API_KEY="dein_key"
export COINBASE_SECRET="-----BEGIN EC PRIVATE KEY-----
...
-----END EC PRIVATE KEY-----"
export DRY_RUN="true"

python trading_bot.py
Strategien
ID	Name	Signal
A	Aggressive RSI Oversold	RSI < 30
B	Trend Following EMA+MACD	EMA12 > EMA26 + MACD bullish
C	Fear & Greed Sentiment	F&G Index < 45
D	Conservative Safe	RSI < 35 und F&G < 30
E	Bollinger Squeeze	BB-Bandbreite < 4%
F	RSI Bullish Divergence	Preis-Tief bei steigendem RSI
G	Mean Reversion	RSI < 35
H	Balanced Hybrid	Kombination aus B, C, D, F
Trade wird ausgelöst wenn Score H ≥ 65 oder Score A ≥ 80.