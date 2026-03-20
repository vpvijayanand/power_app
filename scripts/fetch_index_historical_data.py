"""
fetch_historical_data.py
=========================
Fetches 1-minute historical OHLC data for NIFTY and BANKNIFTY from
Zerodha Kite API and stores it in the `index_data` table.

Indicators (MA20, MA200, ADX) are computed on the full per-symbol
history that exists in the DB after each batch insert, so the values
are accurate even when you backfill in chunks.

Kite API limit: 1-min data → max 60 days per request.
The script chunks the date range automatically.

Usage:
    python fetch_historical_data.py --start_date=2026-01-01 --end_date=2026-01-31
    python fetch_historical_data.py --start_date=2026-01-01               # end defaults to today
    python fetch_historical_data.py --symbol=NIFTY --start_date=2026-02-01
"""

from __future__ import annotations

import argparse
import logging
import logging.handlers
import os
import sys
import time
from datetime import date, datetime, timedelta

import numpy as np
import pandas as pd
from dotenv import load_dotenv
from kiteconnect import KiteConnect
from sqlalchemy import create_engine, text

# ── env ────────────────────────────────────────────────────────────────────────
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# Try multiple candidate locations for .env so the script works regardless of
# where it is invoked from.
_ENV_CANDIDATES = [
    os.path.join(_SCRIPT_DIR, '..', '.env'),          # scripts/../.env  (normal)
    os.path.join(_SCRIPT_DIR, '.env'),                 # scripts/.env
    os.path.join(os.getcwd(), '.env'),                 # cwd/.env
    os.path.join(os.getcwd(), '..', '.env'),           # cwd/../.env
]
_loaded_env = None
for _candidate in _ENV_CANDIDATES:
    _candidate = os.path.normpath(_candidate)
    if os.path.isfile(_candidate):
        load_dotenv(_candidate, override=True)
        _loaded_env = _candidate
        break

DATABASE_URL = os.getenv(
    'DATABASE_URL',
    'postgresql://power_app:vijayPowerAIAPP@localhost:5432/power_app'
)

# ── logging ────────────────────────────────────────────────────────────────────
LOG_FILE = os.path.join(_SCRIPT_DIR, 'fetch_historical.log')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s  %(levelname)-8s  %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.StreamHandler(),
        logging.handlers.RotatingFileHandler(
            LOG_FILE, maxBytes=5 * 1024 * 1024, backupCount=3, encoding='utf-8'
        ),
    ],
)
log = logging.getLogger('FetchHistorical')
log.info(f'[ENV] Loaded .env from: {_loaded_env}' if _loaded_env else '[ENV] WARNING: No .env file found in any candidate path!')

# ── Kite instrument tokens ─────────────────────────────────────────────────────
SYMBOLS = {
    'NIFTY':     {'token': 256265,  'exchange': 'NSE'},
    'BANKNIFTY': {'token': 260105,  'exchange': 'NSE'},
}

# Kite allows max 60 days per request for 1-min interval
CHUNK_DAYS   = 50
INTERVAL     = 'minute'
ADX_PERIOD   = 14


# ══════════════════════════════════════════════════════════════════════════════
#  CLI
# ══════════════════════════════════════════════════════════════════════════════

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description='Fetch NIFTY/BANKNIFTY 1-min historical data from Kite'
    )
    p.add_argument('--start_date', required=True,
                   help='Start date  YYYY-MM-DD  (inclusive)')
    p.add_argument('--end_date', default=str(date.today()),
                   help='End date    YYYY-MM-DD  (inclusive, default: today)')
    p.add_argument('--symbol', default='ALL',
                   help='NIFTY | BANKNIFTY | ALL  (default: ALL)')
    return p.parse_args()


# ══════════════════════════════════════════════════════════════════════════════
#  Kite auth
# ══════════════════════════════════════════════════════════════════════════════

def get_kite(engine=None) -> KiteConnect:
    """
    Returns a ready KiteConnect instance.

    Strategy (in order):
      1. DB → Admin user's api_key + access_token  (freshest — updated on each login)
      2. .env → KITE_API_KEY + ACCESS_TOKEN         (fallback for standalone use)

    Kite access_token is a *daily session token* that expires at the end of each
    trading day, so the DB value (written by the app's login flow) is always
    preferred over any static value in .env.
    """
    api_key = access_token = None

    # ── 1. Try DB ──────────────────────────────────────────────────────────────
    if engine is not None:
        try:
            with engine.connect() as conn:
                row = conn.execute(text("""
                    SELECT api_key, access_token FROM users
                    WHERE  access_token IS NOT NULL
                    ORDER  BY updated_at DESC
                    LIMIT  1
                """)).fetchone()
            if row and row[0] and row[1]:
                api_key, access_token = row[0], row[1]
                log.info('[AUTH] Credentials loaded from DB (request_token column).')
        except Exception as exc:
            log.warning(f'[AUTH] Could not read credentials from DB: {exc}')

    # ── 2. Fall back to .env ───────────────────────────────────────────────────
    if not api_key or not access_token:
        api_key      = os.getenv('KITE_API_KEY') or os.getenv('API_KEY')
        access_token = os.getenv('ACCESS_TOKEN')
        if api_key and access_token:
            log.info('[AUTH] Credentials loaded from .env (fallback).')

    # ── Validate ───────────────────────────────────────────────────────────────
    if not api_key:
        log.error('[AUTH] No api_key found in DB or .env. Aborting.')
        sys.exit(1)
    if not access_token:
        log.error('[AUTH] No access_token found in DB or .env. Aborting.')
        sys.exit(1)

    log.info(f'[AUTH] api_key={api_key[:6]}***  access_token={access_token[:6]}***')
    kite = KiteConnect(api_key=api_key)
    kite.set_access_token(access_token)
    log.info('[AUTH] KiteConnect session ready.')
    return kite


# ══════════════════════════════════════════════════════════════════════════════
#  Indicator calculation
# ══════════════════════════════════════════════════════════════════════════════

def _wilder(s: pd.Series, n: int) -> pd.Series:
    return s.ewm(alpha=1.0 / n, adjust=False).mean()


def compute_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """
    Computes MA20, MA200, and ADX on the full DataFrame.
    Called on the complete per-symbol history so values are accurate.
    """
    df = df.sort_values('timestamp').copy()
    c  = df['close']

    df['ma_20']  = c.rolling(20,  min_periods=1).mean()
    df['ma_200'] = c.rolling(200, min_periods=1).mean()

    # ADX (Wilder smoothing)
    h, l = df['high'], df['low']
    pc   = c.shift(1)
    tr   = pd.concat([h - l, (h - pc).abs(), (l - pc).abs()], axis=1).max(axis=1)
    up   = h - h.shift(1)
    dn   = l.shift(1) - l
    pdm  = pd.Series(np.where((up > dn) & (up > 0), up, 0.0), index=df.index)
    mdm  = pd.Series(np.where((dn > up) & (dn > 0), dn, 0.0), index=df.index)
    str_ = _wilder(tr,  ADX_PERIOD)
    pdi  = 100.0 * _wilder(pdm, ADX_PERIOD) / str_
    mdi  = 100.0 * _wilder(mdm, ADX_PERIOD) / str_
    dx   = (100.0 * (pdi - mdi).abs() / (pdi + mdi)).fillna(0)
    df['adx'] = _wilder(dx, ADX_PERIOD)

    return df


# ══════════════════════════════════════════════════════════════════════════════
#  Date chunking  (Kite limit: 60 days per call for 1-min)
# ══════════════════════════════════════════════════════════════════════════════

def date_chunks(start: date, end: date, chunk: int = CHUNK_DAYS):
    cur = start
    while cur <= end:
        yield cur, min(cur + timedelta(days=chunk - 1), end)
        cur += timedelta(days=chunk)


# ══════════════════════════════════════════════════════════════════════════════
#  Fetch from Kite
# ══════════════════════════════════════════════════════════════════════════════

def fetch_kite_candles(kite: KiteConnect, token: int,
                       from_dt: date, to_dt: date) -> list[dict]:
    """
    Fetches 1-min candles from Kite for the given token and date range.
    Returns list of dicts with keys: date, open, high, low, close, volume.
    """
    try:
        data = kite.historical_data(
            instrument_token = token,
            from_date        = datetime.combine(from_dt, datetime.min.time()),
            to_date          = datetime.combine(to_dt,   datetime(1, 1, 1, 23, 59, 59).time()),
            interval         = INTERVAL,
            continuous       = False,
            oi               = False,
        )
        log.info(f'  [KITE] token={token}  {from_dt} → {to_dt}  rows={len(data)}')
        return data
    except Exception as exc:
        log.error(f'  [KITE] Fetch failed for token={token}  {from_dt} → {to_dt}: {exc}')
        return []


# ══════════════════════════════════════════════════════════════════════════════
#  DB helpers
# ══════════════════════════════════════════════════════════════════════════════

def load_existing(engine, symbol: str) -> pd.DataFrame:
    """Load all existing rows for this symbol so we can recompute indicators."""
    with engine.connect() as conn:
        df = pd.read_sql(text("""
            SELECT id, timestamp, open, high, low, close
            FROM   index_data
            WHERE  symbol = :sym
            ORDER  BY timestamp ASC
        """), conn, params={'sym': symbol})
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    return df


def upsert_rows(engine, symbol: str, token: int, rows: list[dict],
                indicators: pd.DataFrame) -> int:
    """
    Inserts new rows; updates OHLC + indicators for existing (timestamp, symbol).
    indicators must be indexed by timestamp.
    Returns count of rows upserted.
    """
    if not rows:
        return 0

    ind = indicators.set_index('timestamp') if 'timestamp' in indicators.columns else indicators

    upserted = 0
    with engine.begin() as conn:
        for r in rows:
            ts_obj = pd.Timestamp(r['date'])
            ts_obj = ts_obj.tz_convert('Asia/Kolkata').tz_localize(None) if ts_obj.tz else ts_obj
            ts = ts_obj.to_pydatetime()
            ind_row = ind.loc[ts] if ts in ind.index else None

            ma20  = float(ind_row['ma_20'])  if ind_row is not None and pd.notna(ind_row['ma_20'])  else None
            ma200 = float(ind_row['ma_200']) if ind_row is not None and pd.notna(ind_row['ma_200']) else None
            adx_v = float(ind_row['adx'])    if ind_row is not None and pd.notna(ind_row['adx'])    else None

            conn.execute(text("""
                INSERT INTO index_data
                    (timestamp, symbol, instrument_token,
                     open, high, low, close,
                     ma_20, ma_200, adx)
                VALUES
                    (:ts, :sym, :tok,
                     :o, :h, :l, :c,
                     :ma20, :ma200, :adx)
                ON CONFLICT (timestamp, symbol)
                DO UPDATE SET
                    open             = EXCLUDED.open,
                    high             = EXCLUDED.high,
                    low              = EXCLUDED.low,
                    close            = EXCLUDED.close,
                    instrument_token = EXCLUDED.instrument_token,
                    ma_20            = EXCLUDED.ma_20,
                    ma_200           = EXCLUDED.ma_200,
                    adx              = EXCLUDED.adx
            """), {
                'ts':   ts,
                'sym':  symbol,
                'tok':  token,
                'o':    float(r['open']),
                'h':    float(r['high']),
                'l':    float(r['low']),
                'c':    float(r['close']),
                'ma20':  ma20,
                'ma200': ma200,
                'adx':   adx_v,
            })
            upserted += 1

    return upserted


def ensure_unique_index(engine) -> None:
    """Create unique index on (timestamp, symbol) if it doesn't exist."""
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE UNIQUE INDEX IF NOT EXISTS
                idx_index_data_ts_symbol
            ON index_data (timestamp, symbol)
        """))
    log.info('[DB] Unique index on (timestamp, symbol) ensured.')


# ══════════════════════════════════════════════════════════════════════════════
#  Main fetch flow per symbol
# ══════════════════════════════════════════════════════════════════════════════

def fetch_symbol(engine, kite: KiteConnect, symbol: str,
                 start: date, end: date) -> None:
    info  = SYMBOLS[symbol]
    token = info['token']

    log.info(f'')
    log.info(f'{"─"*60}')
    log.info(f'  Symbol : {symbol}  (token={token})')
    log.info(f'  Range  : {start} → {end}')
    log.info(f'{"─"*60}')

    all_rows: list[dict] = []

    for chunk_start, chunk_end in date_chunks(start, end):
        log.info(f'  Fetching chunk: {chunk_start} → {chunk_end}')
        rows = fetch_kite_candles(kite, token, chunk_start, chunk_end)
        all_rows.extend(rows)
        time.sleep(0.4)   # Kite rate-limit: ~3 req/sec

    if not all_rows:
        log.warning(f'  No data returned for {symbol} in this range.')
        return

    log.info(f'  Total candles fetched : {len(all_rows)}')

    # ── Build full history for indicator calculation ─────────────────────────
    # Load existing DB rows + merge with new fetched rows (dedup by timestamp)
    existing_df = load_existing(engine, symbol)
    new_df      = pd.DataFrame([{
        'timestamp': pd.Timestamp(r['date']).tz_convert('Asia/Kolkata').tz_localize(None) if pd.Timestamp(r['date']).tz else pd.Timestamp(r['date']),
        'open':  float(r['open']),
        'high':  float(r['high']),
        'low':   float(r['low']),
        'close': float(r['close']),
    } for r in all_rows])

    if existing_df.empty:
        full_df = new_df
    else:
        full_df = pd.concat([
            existing_df[['timestamp', 'open', 'high', 'low', 'close']],
            new_df
        ]).drop_duplicates(subset='timestamp').sort_values('timestamp').reset_index(drop=True)

    log.info(f'  Full history size (for indicators): {len(full_df)} rows')

    ind_df = compute_indicators(full_df)

    # Only upsert the newly fetched rows (not all history)
    n = upsert_rows(engine, symbol, token, all_rows, ind_df)
    log.info(f'  Upserted {n} rows into index_data for {symbol}.')

    # ── Update indicators for ALL existing rows (recompute from full history) ─
    log.info(f'  Updating indicators for all existing {symbol} rows in DB...')
    ind_indexed = ind_df.set_index('timestamp')
    updated = 0
    with engine.begin() as conn:
        for ts, row_ind in ind_indexed.iterrows():
            conn.execute(text("""
                UPDATE index_data
                SET    ma_20  = :ma20,
                       ma_200 = :ma200,
                       adx    = :adx
                WHERE  timestamp = :ts
                  AND  symbol    = :sym
            """), {
                'ma20':  float(row_ind['ma_20'])  if pd.notna(row_ind['ma_20'])  else None,
                'ma200': float(row_ind['ma_200']) if pd.notna(row_ind['ma_200']) else None,
                'adx':   float(row_ind['adx'])    if pd.notna(row_ind['adx'])    else None,
                'ts':    ts.to_pydatetime(),
                'sym':   symbol,
            })
            updated += 1
    log.info(f'  Indicator update complete for {symbol}: {updated} rows touched.')


# ══════════════════════════════════════════════════════════════════════════════
#  Entry point
# ══════════════════════════════════════════════════════════════════════════════

def main() -> None:
    args = parse_args()

    try:
        start_date = date.fromisoformat(args.start_date)
    except ValueError:
        print(f"ERROR: Invalid start_date '{args.start_date}'. Use YYYY-MM-DD.")
        sys.exit(1)
    try:
        end_date = date.fromisoformat(args.end_date)
    except ValueError:
        print(f"ERROR: Invalid end_date '{args.end_date}'. Use YYYY-MM-DD.")
        sys.exit(1)

    if start_date > end_date:
        print('ERROR: start_date must be <= end_date.')
        sys.exit(1)

    symbol_arg = args.symbol.upper()
    if symbol_arg == 'ALL':
        symbols = list(SYMBOLS.keys())
    elif symbol_arg in SYMBOLS:
        symbols = [symbol_arg]
    else:
        print(f"ERROR: Unknown symbol '{args.symbol}'. Use NIFTY, BANKNIFTY, or ALL.")
        sys.exit(1)

    log.info('=' * 64)
    log.info('  NIFTY/BANKNIFTY Historical Data Fetcher')
    log.info(f'  Start  : {start_date}')
    log.info(f'  End    : {end_date}')
    log.info(f'  Symbol : {symbol_arg}')
    log.info(f'  Total days in range: {(end_date - start_date).days + 1}')
    log.info('=' * 64)

    engine = create_engine(DATABASE_URL, pool_pre_ping=True)

    # Verify DB connection
    try:
        with engine.connect() as conn:
            conn.execute(text('SELECT 1'))
        log.info('[DB] Connection OK.')
    except Exception as exc:
        log.error(f'[DB] Cannot connect: {exc}')
        sys.exit(1)

    ensure_unique_index(engine)

    kite = get_kite(engine)

    for sym in symbols:
        try:
            fetch_symbol(engine, kite, sym, start_date, end_date)
        except Exception as exc:
            log.error(f'[ERROR] Failed for {sym}: {exc}', exc_info=True)

    log.info('')
    log.info('=' * 64)
    log.info('  Done. All symbols processed.')
    log.info('=' * 64)


if __name__ == '__main__':
    main()
