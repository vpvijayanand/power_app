"""
NIFTY Options Auto-Trade Script
================================
Strategy:
  • Uses 1-min NIFTY candles from `index_data` table (polled every 60 s).
  • MA20 and MA200 are read from the stored columns in index_data
    (computed by stream_index.py with full historical context).
  • ADX and DI+/DI- are computed from the last ~50 candles each poll.

  Direction filter:
    20MA > 200MA → only BUY trades allowed  (bullish bias)
    20MA < 200MA → only SELL trades allowed (bearish bias)

  BUY  signal: ADX > 18  AND  DI+ > DI-  AND  20MA > 200MA
    → BUY CE at (floor(nifty/100) * 100) — nearest 100 below spot
  SELL signal: ADX > 18  AND  DI- > DI+  AND  20MA < 200MA
    → BUY PE at (ceil(nifty/100)  * 100) — nearest 100 above spot

  Close triggers:
    (a) ADX drops below 18 → close the current open trade
    (b) New opposite signal → close existing trade, then open new

  Rules:
    • Max 3 completed trades per user per day.
    • All open trades force-closed at 15:15 IST.
    • Does not run on weekends or NSE holidays (holidays.json).

Multi-user:
  • Admin user  → KiteConnect for LTP lookups (no trading).
  • Client users → per-user lot_size, trade_mode ('Live'/'Paper').
  • Live  → real Kite order placed; actual fill price fetched.
  • Paper → simulated; LTP at signal time used as fill price.

Both Live and Paper trades are recorded in `user_trades`.
Every 60 s the current LTP is fetched and max_pnl / min_pnl updated.
"""

from __future__ import annotations  # allows float|None hints on Python 3.9

import json
import logging
import logging.handlers
import math
import os
import sys
import time
from datetime import datetime, date, time as dtime, timedelta

import numpy as np
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from kiteconnect import KiteConnect

# ── env ────────────────────────────────────────────────────────────────────────
load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

DATABASE_URL = os.getenv(
    'DATABASE_URL',
    'postgresql://power_app:vijayPowerAIAPP@localhost:5432/power_app'
)

# ── config ─────────────────────────────────────────────────────────────────────
ADX_PERIOD        = 14
ADX_THRESHOLD     = 18.0
MAX_TRADES_PER_DAY = 3
POLL_INTERVAL     = 60        # seconds between polls
CANDLE_FETCH      = 50        # candles fetched for ADX/DI calculation
ORDER_FILL_TRIES  = 8
ORDER_FILL_WAIT   = 3         # seconds between fill-status polls

MARKET_OPEN  = dtime(9, 15)
MARKET_CLOSE = dtime(15, 15)

HOLIDAYS_FILE = os.path.join(os.path.dirname(__file__), 'holidays.json')

LOG_FILE = os.path.join(os.path.dirname(__file__), 'nifty_trade.log')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s  %(levelname)-8s  %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.StreamHandler(),
        logging.handlers.RotatingFileHandler(
            LOG_FILE, maxBytes=5 * 1024 * 1024, backupCount=5, encoding='utf-8'
        ),
    ],
)
log = logging.getLogger('NiftyTrade')


# ══════════════════════════════════════════════════════════════════════════════
#  Calendar helpers
# ══════════════════════════════════════════════════════════════════════════════

def load_holidays() -> set:
    try:
        with open(HOLIDAYS_FILE, encoding='utf-8') as fh:
            data = json.load(fh)
        holidays = set()
        for lst in data.values():
            if isinstance(lst, list):
                for entry in lst:
                    try:
                        holidays.add(date.fromisoformat(entry['date']))
                    except (KeyError, ValueError):
                        pass
        log.info(f"Loaded {len(holidays)} NSE holiday(s).")
        return holidays
    except FileNotFoundError:
        log.warning("holidays.json not found — no holiday blocking.")
        return set()
    except Exception as exc:
        log.error(f"Failed to load holidays.json: {exc}")
        return set()


def is_trading_day(today: date, holidays: set) -> bool:
    return today.weekday() < 5 and today not in holidays


def is_market_open(now: datetime) -> bool:
    t = now.time()
    return MARKET_OPEN <= t <= MARKET_CLOSE


def secs_until_open(now: datetime) -> int:
    """Seconds from now until 09:15:00 today. Includes seconds component."""
    open_secs = MARKET_OPEN.hour * 3600 + MARKET_OPEN.minute * 60
    now_secs  = now.hour * 3600 + now.minute * 60 + now.second
    return max(0, open_secs - now_secs)


def purge_premarket_data(engine) -> None:
    """
    Deletes NIFTY rows from index_data that fall in the pre-market window:
      yesterday 15:30 PM  →  today 09:12 AM
    Called once at 09:15 to ensure after-hours / pre-open noise is removed
    before the first ADX/MA calculation of the day.
    """
    today = date.today()
    window_start = datetime.combine(today - timedelta(days=1),
                                    dtime(15, 30))
    window_end   = datetime.combine(today, dtime(9, 12))
    try:
        with engine.begin() as conn:
            result = conn.execute(text("""
                DELETE FROM index_data
                WHERE  symbol    = 'NIFTY'
                  AND  timestamp  > :ws
                  AND  timestamp  < :we
            """), {'ws': window_start, 'we': window_end})
            deleted = result.rowcount
        if deleted:
            log.info(f"[PURGE] Removed {deleted} pre-market NIFTY candle(s) "
                     f"({window_start} → {window_end}).")
        else:
            log.info(f"[PURGE] No pre-market candles found to remove "
                     f"({window_start} → {window_end}).")
    except Exception as exc:
        log.error(f"[PURGE] Failed to purge pre-market data: {exc}")


# ══════════════════════════════════════════════════════════════════════════════
#  Kite helpers
# ══════════════════════════════════════════════════════════════════════════════

def get_admin_kite(engine) -> KiteConnect:
    """Returns a ready KiteConnect instance for the Admin user."""
    log.info("[AUTH] Fetching Admin Kite credentials from DB...")
    with engine.connect() as conn:
        row = conn.execute(text("""
            SELECT api_key, access_token FROM users
            WHERE  user_type    = 'Admin'
              AND  access_token IS NOT NULL
              AND  is_active    = TRUE
            LIMIT 1
        """)).fetchone()
    if not row:
        log.error("[AUTH] No active Admin user with access_token found. Exiting.")
        sys.exit(1)
    kite = KiteConnect(api_key=row[0])
    kite.set_access_token(row[1])
    log.info("[AUTH] Admin KiteConnect session established.")
    return kite


def make_kite(user: dict) -> KiteConnect:
    kite = KiteConnect(api_key=user['api_key'])
    kite.set_access_token(user['access_token'])
    return kite


def get_ltp(kite: KiteConnect, nfo_symbols: list) -> dict:
    """Returns {tradingsymbol: ltp}. Empty dict on error."""
    if not nfo_symbols:
        return {}
    try:
        instruments = [f"NFO:{s}" for s in nfo_symbols]
        data = kite.ltp(instruments)
        result = {k.replace('NFO:', ''): v['last_price'] for k, v in data.items()}
        log.debug(f"[LTP] Fetched {len(result)} symbol(s): "
                  f"{', '.join(f'{s}={p:.2f}' for s, p in result.items())}")
        return result
    except Exception as exc:
        log.warning(f"[LTP] Fetch failed for {nfo_symbols}: {exc}")
        return {}


def poll_fill_price(kite: KiteConnect, order_id: str) -> float | None:
    """Polls order history until COMPLETE and returns the average fill price."""
    log.info(f"[FILL] Polling order {order_id} for fill price...")
    for attempt in range(1, ORDER_FILL_TRIES + 1):
        try:
            hist = kite.order_history(order_id)
            if not hist:
                log.debug(f"[FILL] Attempt {attempt}: order history empty, retrying.")
                time.sleep(ORDER_FILL_WAIT)
                continue
            latest = hist[-1]
            status = latest.get('status', '').upper()
            log.debug(f"[FILL] Attempt {attempt}: order_id={order_id} status={status}")
            if status == 'COMPLETE':
                avg_price = float(latest.get('average_price', 0))
                log.info(f"[FILL] Order {order_id} COMPLETE  avg_price={avg_price:.2f}")
                return avg_price
            if status in ('REJECTED', 'CANCELLED'):
                log.error(f"[FILL] Order {order_id} {status}: "
                          f"{latest.get('status_message')}")
                return None
        except Exception as exc:
            log.warning(f"[FILL] Attempt {attempt} exception: {exc}")
        time.sleep(ORDER_FILL_WAIT)
    log.error(f"[FILL] Order {order_id} did not fill after {ORDER_FILL_TRIES} retries — using LTP fallback.")
    return None


# ══════════════════════════════════════════════════════════════════════════════
#  Database — data helpers
# ══════════════════════════════════════════════════════════════════════════════

def get_client_users(engine) -> list:
    """All active Client users with trading credentials."""
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT id, name, email, api_key, access_token,
                   trade_mode, lot_size
            FROM   users
            WHERE  user_type = 'Client'
              AND  is_active = TRUE
            ORDER  BY id
        """)).fetchall()
    users = []
    for r in rows:
        uid, name, email, api_key, access_token, trade_mode, lot_size = r
        if not api_key or not access_token:
            log.warning(f"[USER] id={uid} ({name}) missing api_key/access_token — skipped.")
            continue
        users.append({
            'id':           uid,
            'name':         name,
            'email':        email,
            'api_key':      api_key,
            'access_token': access_token,
            'trade_mode':   (trade_mode or 'Paper').strip(),
            'lot_size':     int(lot_size) if lot_size else 1,
        })
    log.debug(f"[USER] Loaded {len(users)} active client(s).")
    return users


def fetch_nifty_latest(engine, limit: int = CANDLE_FETCH) -> pd.DataFrame:
    """
    Fetches the most recent `limit` 1-min NIFTY candles from index_data.
    Columns returned: open, high, low, close, ma_20, ma_200, adx
    Sorted oldest-first, indexed by timestamp.
    """
    with engine.connect() as conn:
        df = pd.read_sql(text("""
            SELECT timestamp, open, high, low, close,
                   ma_20, ma_200, adx
            FROM   index_data
            WHERE  symbol = 'NIFTY'
            ORDER  BY timestamp DESC
            LIMIT  :lim
        """), conn, params={'lim': limit})

    if df.empty:
        return df
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df = df.sort_values('timestamp').set_index('timestamp')
    return df


def get_instrument(engine, option_type: str, strike: float) -> dict | None:
    """
    Nearest-expiry NIFTY NFO option for the given type and strike.
    If nearest expiry is TODAY, skip it (theta risk) and use next.
    """
    log.debug(f"[INST] Looking up NIFTY {option_type} @ strike={strike}")
    with engine.connect() as conn:
        row = conn.execute(text("""
            SELECT instrument_token, tradingsymbol, expiry, lot_size, strike
            FROM   instruments
            WHERE  name            = 'NIFTY'
              AND  instrument_type = :otype
              AND  strike          = :strike
              AND  exchange        = 'NFO'
              AND  expiry          > CURRENT_DATE
            ORDER  BY expiry ASC
            LIMIT  1
        """), {'otype': option_type, 'strike': float(strike)}).fetchone()
    if not row:
        log.warning(f"[INST] NOT FOUND: NIFTY {option_type} @ {strike} "
                    f"(no future expiry in instruments table)")
        return None
    inst = {
        'instrument_token': row[0],
        'tradingsymbol':    row[1],
        'expiry':           row[2],
        'lot_size':         row[3] or 65,
        'strike':           float(row[4]),
    }
    log.debug(f"[INST] Found: {inst['tradingsymbol']}  "
              f"expiry={inst['expiry']}  lot={inst['lot_size']}")
    return inst


def get_open_trade_for_user(engine, user_id: int) -> dict | None:
    """Returns the single OPEN trade for this user today, or None."""
    with engine.connect() as conn:
        row = conn.execute(text("""
            SELECT id, trade_symbol, trade_instrument_token,
                   option_type, quantity, actual_entry_price,
                   entry_price, max_pnl, min_pnl, trade_mode
            FROM   user_trades
            WHERE  user_id     = :uid
              AND  trade_date  = CURRENT_DATE
              AND  trade_status = 'OPEN'
            LIMIT 1
        """), {'uid': user_id}).fetchone()
    if not row:
        return None
    cols = ['id', 'trade_symbol', 'instrument_token', 'option_type',
            'quantity', 'actual_entry_price', 'entry_price',
            'max_pnl', 'min_pnl', 'trade_mode']
    trade = dict(zip(cols, row))
    log.debug(f"[DB] User {user_id} open trade: id={trade['id']} "
              f"{trade['trade_symbol']} {trade['option_type']} "
              f"qty={trade['quantity']} entry={trade['actual_entry_price']:.2f}")
    return trade


def get_daily_trade_count(engine, user_id: int) -> int:
    """Count how many trades (open + closed) this user has today."""
    with engine.connect() as conn:
        row = conn.execute(text("""
            SELECT COUNT(*) FROM user_trades
            WHERE  user_id    = :uid
              AND  trade_date = CURRENT_DATE
        """), {'uid': user_id}).fetchone()
    count = row[0] if row else 0
    log.debug(f"[DB] User {user_id} trades today: {count}/{MAX_TRADES_PER_DAY}")
    return count


# ══════════════════════════════════════════════════════════════════════════════
#  Indicators — DI+ / DI- / ADX  (Wilder smoothing)
# ══════════════════════════════════════════════════════════════════════════════

def _wilder(s: pd.Series, n: int) -> pd.Series:
    return s.ewm(alpha=1.0 / n, adjust=False).mean()


def calc_dmi(df: pd.DataFrame, period: int = ADX_PERIOD) -> dict:
    """
    Compute DI+, DI-, ADX for the latest bar.
    Returns {'plus_di': float, 'minus_di': float, 'adx': float}.
    """
    h, l, c = df['high'], df['low'], df['close']
    pc = c.shift(1)

    tr  = pd.concat([h - l, (h - pc).abs(), (l - pc).abs()], axis=1).max(axis=1)
    up  = h - h.shift(1)
    dn  = l.shift(1) - l

    pdm = pd.Series(np.where((up > dn) & (up > 0), up, 0.0), index=df.index)
    mdm = pd.Series(np.where((dn > up) & (dn > 0), dn, 0.0), index=df.index)

    s_tr  = _wilder(tr,  period)
    s_pdm = _wilder(pdm, period)
    s_mdm = _wilder(mdm, period)

    pdi = 100.0 * s_pdm / s_tr
    mdi = 100.0 * s_mdm / s_tr
    dx  = 100.0 * (pdi - mdi).abs() / (pdi + mdi)
    adx = _wilder(dx, period)

    return {
        'plus_di':  float(pdi.iloc[-1]),
        'minus_di': float(mdi.iloc[-1]),
        'adx':      float(adx.iloc[-1]),
    }


# ══════════════════════════════════════════════════════════════════════════════
#  Signal detection
# ══════════════════════════════════════════════════════════════════════════════

def detect_signal(df: pd.DataFrame) -> dict:
    """
    BUY  condition: DB ADX > 18  AND  DI+ > DI-  AND  ma20 > ma200
    SELL condition: DB ADX > 18  AND  DI- > DI+  AND  ma20 < ma200

    ADX  → read from index_data.adx  (computed by stream_index.py with full history)
    DI+/DI- → computed locally from last 50 OHLC bars (not stored in DB)
    """
    result = {'signal': None, 'adx': 0.0, 'plus_di': 0.0,
              'minus_di': 0.0, 'ma20': 0.0, 'ma200': 0.0, 'close': 0.0}

    if len(df) < ADX_PERIOD + 5:
        return result

    latest = df.iloc[-1]
    ma20   = float(latest['ma_20'])  if pd.notna(latest['ma_20'])  else 0.0
    ma200  = float(latest['ma_200']) if pd.notna(latest['ma_200']) else 0.0
    close  = float(latest['close'])

    # Use DB-stored ADX — full Wilder smoothing across all historical bars
    adx = float(latest['adx']) if pd.notna(latest['adx']) else 0.0

    # Compute DI+/DI- locally (not stored in DB)
    dmi = calc_dmi(df, ADX_PERIOD)
    pdi = dmi['plus_di']
    mdi = dmi['minus_di']

    result.update({'adx': adx, 'plus_di': pdi, 'minus_di': mdi,
                   'ma20': ma20, 'ma200': ma200, 'close': close})

    if adx > ADX_THRESHOLD:
        if pdi > mdi and ma20 > ma200:
            result['signal'] = 'BUY'
        elif mdi > pdi and ma20 < ma200:
            result['signal'] = 'SELL'

    return result


def strike_for_signal(signal: str, nifty_close: float) -> float:
    """
    BUY  → floor to nearest 100 below spot (ITM CE for bullish)
    SELL → ceil  to nearest 100 above spot (ITM PE for bearish)
    """
    if signal == 'BUY':
        return math.floor(nifty_close / 100) * 100
    else:
        return math.ceil(nifty_close / 100) * 100


# ══════════════════════════════════════════════════════════════════════════════
#  Trade entry / exit
# ══════════════════════════════════════════════════════════════════════════════

def _insert_trade(engine, user: dict, inst: dict, option_type: str,
                  trade_type: str, nifty_close: float,
                  quantity: int, entry_ltp: float,
                  actual_entry: float, order_id: str | None) -> None:
    now = datetime.now()
    try:
        with engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO user_trades (
                    trade_date, user_id,
                    nifty_price, trade_symbol, trade_instrument_token,
                    option_type, strike_price, expiry_date,
                    entry_time, entry_price, actual_entry_price,
                    lot_size, quantity, trade_type,
                    trade_status, trade_mode,
                    kite_order_id_entry,
                    capital_used, max_pnl, min_pnl,
                    created_at, updated_at
                ) VALUES (
                    :dt, :uid,
                    :nifty, :sym, :token,
                    :otype, :strike, :expiry,
                    :et, :ep, :aep,
                    :lot, :qty, :ttype,
                    'OPEN', :tmode,
                    :oid,
                    :capital, 0.0, 0.0,
                    now(), now()
                )
            """), {
                'dt':     date.today(),
                'uid':    user['id'],
                'nifty':  nifty_close,
                'sym':    inst['tradingsymbol'],
                'token':  inst['instrument_token'],
                'otype':  option_type,
                'strike': inst['strike'],
                'expiry': inst['expiry'],
                'et':     now,
                'ep':     entry_ltp,
                'aep':    actual_entry,
                'lot':    user['lot_size'],
                'qty':    quantity,
                'ttype':  trade_type,
                'tmode':  user['trade_mode'],
                'oid':    order_id,
                'capital': actual_entry * quantity,
            })
        log.info(f"  [{user['name']}] Trade recorded: {inst['tradingsymbol']} "
                 f"x{quantity} @ {actual_entry:.2f}")
    except Exception as exc:
        log.error(f"  [{user['name']}] DB insert failed: {exc}")


def open_trade(user: dict, inst: dict, option_type: str,
               nifty_close: float, admin_kite: KiteConnect,
               engine) -> None:
    """Place a BUY order (Live or Paper) and record in user_trades."""
    label     = f"{user['name']} (id={user['id']})"
    qty       = inst['lot_size'] * user['lot_size']
    is_live   = user['trade_mode'].lower() == 'live'
    trade_type = 'BUY'

    log.info(f"  [{label}] ── OPEN TRADE ──────────────────────────────")
    log.info(f"  [{label}] Symbol  : {inst['tradingsymbol']}")
    log.info(f"  [{label}] Strike  : {inst['strike']}  Expiry: {inst['expiry']}")
    log.info(f"  [{label}] Qty     : {qty} ({user['lot_size']} lot × {inst['lot_size']})")
    log.info(f"  [{label}] Mode    : {user['trade_mode']}")
    log.info(f"  [{label}] Nifty   : {nifty_close:.2f}")

    # Get entry LTP for reference price
    ltp_map   = get_ltp(admin_kite, [inst['tradingsymbol']])
    entry_ltp = ltp_map.get(inst['tradingsymbol'], 0.0)
    log.info(f"  [{label}] LTP ref : {entry_ltp:.2f}")

    if is_live:
        kite = make_kite(user)
        try:
            order_id = kite.place_order(
                variety          = KiteConnect.VARIETY_REGULAR,
                exchange         = KiteConnect.EXCHANGE_NFO,
                tradingsymbol    = inst['tradingsymbol'],
                transaction_type = KiteConnect.TRANSACTION_TYPE_BUY,
                quantity         = qty,
                product          = KiteConnect.PRODUCT_MIS,
                order_type       = KiteConnect.ORDER_TYPE_MARKET,
            )
            log.info(f"  [LIVE | {label}] BUY order placed  order_id={order_id}")
        except Exception as exc:
            log.error(f"  [LIVE | {label}] BUY order FAILED: {exc}")
            return

        actual_entry = poll_fill_price(kite, order_id) or entry_ltp
        log.info(f"  [LIVE | {label}] Fill price: {actual_entry:.2f} "
                 f"(LTP fallback used: {actual_entry == entry_ltp})")
    else:
        order_id     = None
        actual_entry = entry_ltp
        log.info(f"  [PAPER | {label}] Simulated BUY @ {entry_ltp:.2f}")

    log.info(f"  [{label}] Capital used: {actual_entry * qty:.2f}")
    _insert_trade(engine, user, inst, option_type, trade_type,
                  nifty_close, qty, entry_ltp, actual_entry, order_id)


def close_trade(user: dict, trade: dict, admin_kite: KiteConnect,
                engine, reason: str = '') -> None:
    """Place a SELL order to close an open trade and update user_trades."""
    label    = f"{user['name']} (id={user['id']})"
    is_live  = trade['trade_mode'].lower() == 'live'
    symbol   = trade['trade_symbol']
    qty      = trade['quantity']

    log.info(f"  [{label}] ── CLOSE TRADE ─────────────────────────────")
    log.info(f"  [{label}] Symbol   : {symbol}  qty={qty}")
    log.info(f"  [{label}] Reason   : {reason}")
    log.info(f"  [{label}] trade_id : {trade['id']}")

    # Current LTP for reference
    ltp_map  = get_ltp(admin_kite, [symbol])
    exit_ltp = ltp_map.get(symbol, trade['entry_price'])
    log.info(f"  [{label}] Exit LTP : {exit_ltp:.2f}")

    exit_order_id   = None
    actual_exit     = exit_ltp

    if is_live:
        kite = make_kite(user)
        try:
            exit_order_id = kite.place_order(
                variety          = KiteConnect.VARIETY_REGULAR,
                exchange         = KiteConnect.EXCHANGE_NFO,
                tradingsymbol    = symbol,
                transaction_type = KiteConnect.TRANSACTION_TYPE_SELL,
                quantity         = qty,
                product          = KiteConnect.PRODUCT_MIS,
                order_type       = KiteConnect.ORDER_TYPE_MARKET,
            )
            log.info(f"  [LIVE | {label}] SELL order placed  order_id={exit_order_id}")
        except Exception as exc:
            log.error(f"  [LIVE | {label}] SELL order FAILED: {exc}")
            log.error(f"  [LIVE | {label}] *** DB NOT updated — position still OPEN in Zerodha! "
                      f"trade_id={trade['id']} symbol={symbol} qty={qty} ***")
            return  # DO NOT mark CLOSED — position is still live in Zerodha
        if exit_order_id:
            actual_exit = poll_fill_price(kite, exit_order_id) or exit_ltp
            log.info(f"  [LIVE | {label}] Exit fill price: {actual_exit:.2f} "
                     f"(LTP fallback: {actual_exit == exit_ltp})")
    else:
        log.info(f"  [PAPER | {label}] Simulated SELL @ {exit_ltp:.2f}")

    # PnL — long position: (exit - entry) * qty
    cost_basis  = trade['actual_entry_price'] or trade['entry_price']
    closing_pnl = (actual_exit - cost_basis) * qty
    max_pnl     = max(trade['max_pnl'] or 0.0, closing_pnl)
    min_pnl     = min(trade['min_pnl'] or 0.0, closing_pnl)
    capital     = cost_basis * qty
    pnl_pct     = (closing_pnl / capital * 100) if capital else 0.0

    log.info(f"  [{label}] Entry={cost_basis:.2f}  Exit={actual_exit:.2f}  "
             f"Qty={qty}  PnL={closing_pnl:.2f} ({pnl_pct:.2f}%)  "
             f"MaxPnL={max_pnl:.2f}  MinPnL={min_pnl:.2f}")

    try:
        with engine.begin() as conn:
            conn.execute(text("""
                UPDATE user_trades
                SET    exit_time          = now(),
                       exit_price         = :ep,
                       actual_exit_price  = :aep,
                       kite_order_id_exit = :oid,
                       trade_status       = 'CLOSED',
                       closing_pnl        = :pnl,
                       pnl_percentage     = :pct,
                       max_pnl            = :max_p,
                       min_pnl            = :min_p,
                       updated_at         = now()
                WHERE  id = :tid
            """), {
                'ep':    exit_ltp,
                'aep':   actual_exit,
                'oid':   exit_order_id,
                'pnl':   closing_pnl,
                'pct':   pnl_pct,
                'max_p': max_pnl,
                'min_p': min_pnl,
                'tid':   trade['id'],
            })
        log.info(f"  [{label}] DB updated: trade_id={trade['id']} → CLOSED")
    except Exception as exc:
        log.error(f"  [{label}] DB close update FAILED for trade_id={trade['id']}: {exc}")


# ══════════════════════════════════════════════════════════════════════════════
#  PnL update — every poll cycle
# ══════════════════════════════════════════════════════════════════════════════

def update_pnl_all(engine, admin_kite: KiteConnect) -> None:
    """
    For every OPEN trade today, fetch current LTP and update
    max_pnl / min_pnl.  Uses a single batch DB UPDATE.
    PnL = (current_ltp - actual_entry_price) * quantity  [long position]
    """
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT id, trade_symbol, quantity,
                   actual_entry_price, entry_price,
                   max_pnl, min_pnl
            FROM   user_trades
            WHERE  trade_status = 'OPEN'
              AND  trade_date   = CURRENT_DATE
        """)).fetchall()

    if not rows:
        return

    cols   = ['id', 'trade_symbol', 'quantity', 'actual_entry_price',
              'entry_price', 'max_pnl', 'min_pnl']
    trades = [dict(zip(cols, r)) for r in rows]

    symbols = list({t['trade_symbol'] for t in trades})
    ltp_map = get_ltp(admin_kite, symbols)

    updates = []
    for t in trades:
        ltp = ltp_map.get(t['trade_symbol'])
        if ltp is None:
            continue
        cost   = t['actual_entry_price'] or t['entry_price']
        pnl    = (ltp - cost) * t['quantity']
        max_p  = max(t['max_pnl'] or 0.0, pnl)
        min_p  = min(t['min_pnl'] or 0.0, pnl)
        updates.append({'max_p': max_p, 'min_p': min_p, 'tid': t['id']})
        log.debug(f"  Trade {t['id']} {t['trade_symbol']}  LTP={ltp:.2f}  PnL={pnl:.2f}")

    for u in updates:
        try:
            with engine.begin() as conn:
                conn.execute(text("""
                    UPDATE user_trades
                    SET    max_pnl    = :max_p,
                           min_pnl   = :min_p,
                           updated_at = now()
                    WHERE  id = :tid
                """), u)
        except Exception as exc:
            log.error(f"[PNL] Update failed for trade_id={u['tid']}: {exc}")


# ══════════════════════════════════════════════════════════════════════════════
#  15:15 square-off — close all open trades
# ══════════════════════════════════════════════════════════════════════════════

def squareoff_all(engine, admin_kite: KiteConnect,
                  clients: list) -> None:
    """Force-close every OPEN trade at market for all users."""
    client_map = {u['id']: u for u in clients}

    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT id, user_id, trade_symbol, trade_instrument_token,
                   option_type, quantity, actual_entry_price,
                   entry_price, max_pnl, min_pnl, trade_mode
            FROM   user_trades
            WHERE  trade_status = 'OPEN'
              AND  trade_date   = CURRENT_DATE
        """)).fetchall()

    if not rows:
        log.info("Square-off: no open trades.")
        return

    cols = ['id', 'user_id', 'trade_symbol', 'instrument_token',
            'option_type', 'quantity', 'actual_entry_price',
            'entry_price', 'max_pnl', 'min_pnl', 'trade_mode']
    trades = [dict(zip(cols, r)) for r in rows]
    log.info(f"Square-off: closing {len(trades)} open trade(s).")

    for t in trades:
        user = client_map.get(t['user_id'])
        if not user:
            log.error(
                f"[SQUAREOFF] *** ORPHANED POSITION — user_id={t['user_id']} not in active "
                f"client list (deactivated?). trade_id={t['id']} symbol={t['trade_symbol']} "
                f"qty={t['quantity']} mode={t['trade_mode']} — CLOSE MANUALLY IN ZERODHA! ***"
            )
            continue
        close_trade(user, t, admin_kite, engine, reason='15:15 squareoff')


# ══════════════════════════════════════════════════════════════════════════════
#  Main trading logic — called on each new 1-min candle
# ══════════════════════════════════════════════════════════════════════════════

def process_signal(sig: dict, engine, admin_kite: KiteConnect,
                   clients: list) -> None:
    """
    For every client:
      1. If ADX < threshold → close any open trade (no new trade).
      2. If valid new signal and < MAX_TRADES_PER_DAY:
           a. Close existing open trade (if any).
           b. Open new trade.
    """
    adx     = sig['adx']
    signal  = sig['signal']
    close   = sig['close']

    for user in clients:
        label     = f"{user['name']} (id={user['id']})"
        open_trade_row = get_open_trade_for_user(engine, user['id'])

        # ── ADX dropped below threshold → close if open ────────────────
        if adx < ADX_THRESHOLD and open_trade_row:
            log.info(f"  [{label}] ADX={adx:.2f} < {ADX_THRESHOLD} — closing trade.")
            close_trade(user, open_trade_row, admin_kite, engine,
                        reason=f'ADX dropped to {adx:.2f}')
            continue   # no new trade without ADX confirmation

        # ── No valid signal ────────────────────────────────────────────
        if not signal:
            continue

        # ── Determine expected option leg for this signal ──────────────
        expected_opt = 'CE' if signal == 'BUY' else 'PE'

        # ── Close on reversal — ALWAYS, regardless of trade count ───────
        # A CE must not stay open while a SELL signal is active.
        if open_trade_row and open_trade_row['option_type'] != expected_opt:
            log.info(
                f"  [{label}] Signal reversal ({signal}) — "
                f"closing {open_trade_row['option_type']} first."
            )
            close_trade(user, open_trade_row, admin_kite, engine,
                        reason=f'signal reversal to {signal}')
            open_trade_row = None

        # ── Hold if already in the correct direction ────────────────────
        if open_trade_row and open_trade_row['option_type'] == expected_opt:
            log.debug(f"  [{label}] Already in {expected_opt} — holding.")
            continue

        # ── Trade count checked AFTER any close, before opening new ─────
        # Ensures count reflects the latest DB state post-close.
        trade_count = get_daily_trade_count(engine, user['id'])
        if trade_count >= MAX_TRADES_PER_DAY:
            log.info(f"  [{label}] Max {MAX_TRADES_PER_DAY} trades reached — no new entry.")
            continue

        # ── Open new trade ─────────────────────────────────────────────
        option_type = expected_opt
        strike      = strike_for_signal(signal, close)
        inst        = get_instrument(engine, option_type, strike)
        if not inst:
            log.warning(f"  [{label}] No instrument for {option_type}@{strike} — skip.")
            continue

        log.info(f"  [{label}] Opening {signal} -> BUY {option_type} @ {strike}  "
                 f"({inst['tradingsymbol']})")
        open_trade(user, inst, option_type, close, admin_kite, engine)


# ══════════════════════════════════════════════════════════════════════════════
#  Main loop
# ══════════════════════════════════════════════════════════════════════════════

def run(engine, admin_kite: KiteConnect) -> None:
    last_ts: datetime | None = None
    squared_off               = False
    purged_today              = False   # premarket purge flag
    holidays                  = load_holidays()
    last_holiday_date: date | None = None

    log.info("Main loop started. Waiting for market hours...")

    while True:
        now   = datetime.now()
        today = now.date()

        # Reload holidays and reset flags on date change
        if last_holiday_date != today:
            holidays           = load_holidays()
            last_holiday_date  = today
            squared_off        = False
            purged_today       = False   # reset purge flag for new day
            last_ts            = None
            log.info(f"[DATE] New trading day: {today}.")

        # ── Weekend / holiday guard ────────────────────────────────────
        if not is_trading_day(today, holidays):
            reason = ('weekend' if today.weekday() >= 5
                      else f'NSE holiday ({today})')
            log.info(f"Not a trading day ({reason}) — sleeping 1 h.")
            time.sleep(3600)
            continue

        # ── Pre-market sleep ───────────────────────────────────────────
        if not is_market_open(now):
            if now.time() < MARKET_OPEN:
                wait = secs_until_open(now)
                # Always sleep at least 60 s to avoid a tight loop
                # if wait==0 but we're right at the boundary second.
                log.info(f"Market opens at 09:15 — sleeping {max(wait, 60)}s.")
                time.sleep(max(wait, 60))
            else:
                log.info("Market closed for today. Sleeping 1 h.")
                time.sleep(3600)
            continue

        # ── 15:15 square-off ───────────────────────────────────────────
        if now.time() >= MARKET_CLOSE and not squared_off:
            log.info("★ ★ ★  15:15 MARKET CLOSE — squaring off all open trades  ★ ★ ★")
            clients = get_client_users(engine)
            squareoff_all(engine, admin_kite, clients)
            squared_off = True
            log.info("Square-off complete. Script will idle until next trading day.")
            time.sleep(POLL_INTERVAL)
            continue

        # ── Purge pre-market data once at 09:15 ────────────────────────
        if not purged_today:
            log.info("[PURGE] First market-open poll — purging pre-market candles.")
            purge_premarket_data(engine)
            purged_today = True

        # ── Per-minute PnL update ──────────────────────────────────────
        log.debug("[PNL] Updating PnL for all open trades...")
        update_pnl_all(engine, admin_kite)

        # ── Fetch latest candles ───────────────────────────────────────
        try:
            df = fetch_nifty_latest(engine, limit=CANDLE_FETCH)
        except Exception as exc:
            log.exception(f"Error fetching candles: {exc}")
            time.sleep(POLL_INTERVAL)
            continue

        if df.empty:
            log.warning("No NIFTY candles in index_data — waiting.")
            time.sleep(POLL_INTERVAL)
            continue

        latest_ts = df.index[-1]
        if last_ts is not None and latest_ts <= last_ts:
            log.debug(f"[CANDLE] No new candle yet. Last seen: {last_ts}.")
            time.sleep(POLL_INTERVAL)
            continue

        last_ts = latest_ts
        log.debug(f"[CANDLE] New candle: {latest_ts}  close={df.iloc[-1]['close']:.2f}  "
                  f"rows_in_window={len(df)}")

        if len(df) < ADX_PERIOD + 5:
            log.info(f"[WARMUP] {len(df)}/{ADX_PERIOD + 5} candles — waiting for warmup.")
            time.sleep(POLL_INTERVAL)
            continue

        # ── Signal detection ───────────────────────────────────────────
        sig = detect_signal(df)

        log.info(
            f"[BAR {latest_ts.strftime('%H:%M')}] "
            f"Close={sig['close']:.2f}  "
            f"ADX={sig['adx']:.2f} (thresh={ADX_THRESHOLD})  "
            f"DI+={sig['plus_di']:.2f}  DI-={sig['minus_di']:.2f}  "
            f"MA20={sig['ma20']:.2f}  MA200={sig['ma200']:.2f}  "
            f"Bias={'BULL' if sig['ma20'] > sig['ma200'] else 'BEAR'}  "
            f"Signal={sig['signal'] or 'NONE'}"
        )

        # ── Process for each client user ───────────────────────────────
        clients = get_client_users(engine)
        process_signal(sig, engine, admin_kite, clients)

        time.sleep(POLL_INTERVAL)


# ══════════════════════════════════════════════════════════════════════════════
#  Entry point
# ══════════════════════════════════════════════════════════════════════════════

def main() -> None:
    log.info("=" * 64)
    log.info("  NIFTY Options Auto-Trade  |  DB-polled  |  Multi-User")
    log.info("=" * 64)
    log.info(f"  ADX period     : {ADX_PERIOD}   threshold : {ADX_THRESHOLD}")
    log.info(f"  Max trades/day : {MAX_TRADES_PER_DAY}")
    log.info(f"  Market hours   : 09:15 – 15:15 IST")
    log.info(f"  Poll interval  : {POLL_INTERVAL}s")
    log.info("=" * 64)

    engine = create_engine(DATABASE_URL, pool_pre_ping=True)

    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        log.info("Database connection OK.")
    except Exception as exc:
        log.error(f"Cannot connect to DB: {exc}")
        sys.exit(1)

    admin_kite = get_admin_kite(engine)
    log.info("Admin Kite session ready.")

    clients = get_client_users(engine)
    live_n  = sum(1 for c in clients if c['trade_mode'].lower() == 'live')
    log.info(f"Clients: {len(clients)} total  |  {live_n} Live  |  {len(clients)-live_n} Paper")

    run(engine, admin_kite)


if __name__ == '__main__':
    main()
