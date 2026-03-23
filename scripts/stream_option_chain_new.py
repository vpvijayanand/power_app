import os
import sys
import time
import logging
import threading
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine, text
from kiteconnect import KiteConnect, KiteTicker

# Add parent directory to path to import app config
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from app.config import config

# Logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Global cache for option chain data
# Key: (underlying, expiry, strike)
# Value: dict with ce_data and pe_data
option_chain_cache = {}
initial_oi_map = {}  # Token -> Initial OI
cache_lock = threading.Lock()

# Map instrument token to details for quick lookup
token_map = {}

# ATM strike range (points from spot price)
ATM_RANGE = {
    'NIFTY':     1000,   # ±4000 points from spot
    'BANKNIFTY': 2000    # ±6000 points from spot
}


def get_db_engine():
    db_config = config['development']
    return create_engine(db_config.SQLALCHEMY_DATABASE_URI)


def get_admin_credentials(engine):
    """Fetch admin user's Kite credentials from database"""
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT api_key, api_secret, access_token 
            FROM users 
            WHERE user_type = 'Admin' 
            AND api_key IS NOT NULL 
            AND access_token IS NOT NULL
            LIMIT 1
        """))
        row = result.fetchone()
        if row:
            return {'api_key': row[0], 'access_token': row[2]}
    return None


def get_spot_prices(kite):
    """
    Fetch current live spot price for NIFTY and BANKNIFTY via Kite quote API.
    Returns a dict: {'NIFTY': <price>, 'BANKNIFTY': <price>}
    Falls back to None values if the fetch fails.
    """
    spot_prices = {'NIFTY': None, 'BANKNIFTY': None}
    try:
        quotes = kite.quote(["NSE:NIFTY 50", "NSE:NIFTY BANK"])
        spot_prices['NIFTY']     = quotes["NSE:NIFTY 50"]["last_price"]
        spot_prices['BANKNIFTY'] = quotes["NSE:NIFTY BANK"]["last_price"]
        logger.info(f"Spot prices fetched — NIFTY: {spot_prices['NIFTY']}, BANKNIFTY: {spot_prices['BANKNIFTY']}")
    except Exception as e:
        logger.error(f"Failed to fetch spot prices: {e}")
    return spot_prices


def get_option_instruments(engine, kite):
    """
    Fetch option instruments for NIFTY & BANKNIFTY from the DB,
    then filter strikes to ATM ± ATM_RANGE based on live spot price.

    Steps:
      1. Fetch all NFO-OPT instruments for NIFTY/BANKNIFTY with valid expiry.
      2. Get live spot prices from Kite.
      3. Keep only the nearest N expiries per underlying.
      4. Filter strikes to [spot - range, spot + range].
    """
    logger.info("Fetching instruments from database...")

    query = """
        SELECT instrument_token, tradingsymbol, name, expiry, strike, instrument_type 
        FROM instruments 
        WHERE segment = 'NFO-OPT' 
        AND name IN ('NIFTY', 'BANKNIFTY')
        AND expiry >= CURRENT_DATE
    """

    df = pd.read_sql(query, engine)

    if df.empty:
        logger.error("No option instruments found in database! Run sync_instruments.py first.")
        return [], {}

    df['expiry'] = pd.to_datetime(df['expiry']).dt.date

    # Fetch live spot prices to determine ATM
    spot_prices = get_spot_prices(kite)

    selected_tokens = []
    local_token_map = {}

    for underlying in ['NIFTY', 'BANKNIFTY']:
        spot = spot_prices.get(underlying)

        if spot is None:
            logger.warning(
                f"Spot price unavailable for {underlying}. "
                f"Skipping ATM filter — ALL strikes will be subscribed (this may be large)."
            )

        # Filter for this underlying
        udf = df[df['name'] == underlying].copy()

        # Get sorted unique expiries and pick the nearest N
        expiries = sorted(udf['expiry'].unique())
        limit = 4 if underlying == 'NIFTY' else 2
        selected_expiries = expiries[:limit]
        logger.info(f"Selected expiries for {underlying}: {selected_expiries}")

        # Filter to selected expiries
        udf = udf[udf['expiry'].isin(selected_expiries)]

        # Apply ATM strike range filter only when spot is available
        if spot is not None:
            atm_range = ATM_RANGE[underlying]
            lower = spot - atm_range
            upper = spot + atm_range
            before = len(udf)
            udf = udf[(udf['strike'] >= lower) & (udf['strike'] <= upper)]
            after = len(udf)
            logger.info(
                f"{underlying} @ spot={spot:.2f} — "
                f"strike filter [{lower:.0f}, {upper:.0f}] "
                f"reduced instruments from {before} → {after}"
            )

        if udf.empty:
            logger.warning(
                f"No instruments remaining for {underlying} after strike filter! "
                f"Check if instruments table is stale — re-run sync_instruments.py."
            )
            continue

        for _, row in udf.iterrows():
            token = row['instrument_token']
            selected_tokens.append(token)
            local_token_map[token] = {
                'underlying': row['name'],
                'expiry':     row['expiry'],
                'strike':     row['strike'],
                'type':       row['instrument_type'],   # CE or PE
                'symbol':     row['tradingsymbol'],
                'is_current': row['expiry'] == selected_expiries[0]
            }

    logger.info(f"Total tokens selected: {len(selected_tokens)}")
    return selected_tokens, local_token_map


def on_ticks(ws, ticks):
    """Callback for tick data"""
    global option_chain_cache
    global initial_oi_map

    with cache_lock:
        for tick in ticks:
            token = tick['instrument_token']
            if token not in token_map:
                continue

            details = token_map[token]
            key = (details['underlying'], details['expiry'], details['strike'])

            # Initialize cache entry if missing
            if key not in option_chain_cache:
                option_chain_cache[key] = {
                    'underlying':        details['underlying'],
                    'expiry_date':       details['expiry'],
                    'strike_price':      details['strike'],
                    'timestamp':         datetime.now(),
                    'is_current_expiry': details['is_current'],
                    'ce': {},
                    'pe': {}
                }

            # Handle Initial OI for change calculation (relative to script start)
            current_oi = tick.get('oi', 0)
            if token not in initial_oi_map:
                initial_oi_map[token] = current_oi

            oi_change = current_oi - initial_oi_map[token]

            # Build tick data dict
            data = {
                'oi':             current_oi,
                'oi_change':      oi_change,
                'volume':         tick.get('volume_traded', 0),
                'ltp':            tick.get('last_price', 0),
                'change':         tick.get('change', 0),
                'change_percent': 0,
                'token':          token,
                'symbol':         details['symbol']
            }

            # Calculate price change % from previous close if OHLC is available
            if 'ohlc' in tick:
                close_price = tick['ohlc'].get('close', 0)
                if close_price > 0:
                    data['change']         = data['ltp'] - close_price
                    data['change_percent'] = (data['change'] / close_price) * 100

            # Assign to CE or PE bucket
            opt_type = details['type']
            if opt_type == 'CE':
                option_chain_cache[key]['ce'] = data
            elif opt_type == 'PE':
                option_chain_cache[key]['pe'] = data


def on_connect(ws, response):
    """Callback on successful WebSocket connection"""
    logger.info("Connected to Kite Ticker")
    tokens = list(token_map.keys())
    ws.subscribe(tokens)
    ws.set_mode(ws.MODE_FULL, tokens)
    logger.info(f"Subscribed to {len(tokens)} tokens in FULL mode")


def on_close(ws, code, reason):
    logger.info(f"Connection closed: {code} - {reason}")


def on_error(ws, code, reason):
    logger.error(f"WebSocket error: {code} - {reason}")


def db_updater(engine):
    """
    Background thread that flushes the in-memory cache to the DB once per minute.
    Aligns flush to the top of each minute (e.g. 09:15:00, 09:16:00 ...).
    """
    logger.info("DB updater thread started.")

    while True:
        # Sleep until the next whole minute
        now = datetime.now()
        seconds_to_wait = 60 - now.second
        time.sleep(seconds_to_wait)

        try:
            with cache_lock:
                if not option_chain_cache:
                    continue
                snapshot = option_chain_cache.copy()

            if not snapshot:
                continue

            # Align timestamp to the minute boundary
            batch_timestamp = datetime.now().replace(second=0, microsecond=0)

            rows = []
            for key, data in snapshot.items():
                ce = data.get('ce', {})
                pe = data.get('pe', {})

                row = {
                    'underlying':        data['underlying'],
                    'strike_price':      data['strike_price'],
                    'expiry_date':       data['expiry_date'],
                    'timestamp':         batch_timestamp,
                    'is_current_expiry': data['is_current_expiry'],

                    # CE columns
                    'ce_oi':               ce.get('oi'),
                    'ce_oi_change':        ce.get('oi_change'),
                    'ce_volume':           ce.get('volume'),
                    'ce_ltp':              ce.get('ltp'),
                    'ce_change':           ce.get('change'),
                    'ce_change_percent':   ce.get('change_percent'),
                    'ce_strike_symbol':    ce.get('symbol'),
                    'ce_instrument_token': str(ce['token']) if ce.get('token') else None,

                    # PE columns
                    'pe_oi':               pe.get('oi'),
                    'pe_oi_change':        pe.get('oi_change'),
                    'pe_volume':           pe.get('volume'),
                    'pe_ltp':              pe.get('ltp'),
                    'pe_change':           pe.get('change'),
                    'pe_change_percent':   pe.get('change_percent'),
                    'pe_strike_symbol':    pe.get('symbol'),
                    'pe_instrument_token': str(pe['token']) if pe.get('token') else None,
                }
                rows.append(row)

            if not rows:
                continue

            with engine.connect() as conn:
                conn.execute(
                    text("""
                        INSERT INTO option_chain_data (
                            underlying, strike_price, expiry_date, timestamp, is_current_expiry,
                            ce_oi, ce_oi_change, ce_volume, ce_ltp, ce_change, ce_change_percent,
                            ce_strike_symbol, ce_instrument_token,
                            pe_oi, pe_oi_change, pe_volume, pe_ltp, pe_change, pe_change_percent,
                            pe_strike_symbol, pe_instrument_token
                        ) VALUES (
                            :underlying, :strike_price, :expiry_date, :timestamp, :is_current_expiry,
                            :ce_oi, :ce_oi_change, :ce_volume, :ce_ltp, :ce_change, :ce_change_percent,
                            :ce_strike_symbol, :ce_instrument_token,
                            :pe_oi, :pe_oi_change, :pe_volume, :pe_ltp, :pe_change, :pe_change_percent,
                            :pe_strike_symbol, :pe_instrument_token
                        )
                    """),
                    rows
                )
                conn.commit()
            logger.info(f"Flushed {len(rows)} rows to DB at {batch_timestamp}")

        except Exception as e:
            logger.error(f"DB updater error: {e}", exc_info=True)


def main():
    logger.info("Initializing Option Chain Streamer...")
    engine = get_db_engine()

    # --- Step 1: Get admin credentials ---
    creds = get_admin_credentials(engine)
    if not creds:
        logger.error("Admin credentials not found in DB!")
        return

    # --- Step 2: Initialize KiteConnect (REST) to fetch spot prices ---
    kite = KiteConnect(api_key=creds['api_key'])
    kite.set_access_token(creds['access_token'])

    # --- Step 3: Fetch instruments with ATM-based strike filtering ---
    global token_map
    tokens, token_map = get_option_instruments(engine, kite)

    if not tokens:
        logger.error(
            "No tokens found after filtering. "
            "Possible causes:\n"
            "  1. instruments table is stale — run sync_instruments.py\n"
            "  2. ATM range too narrow — adjust ATM_RANGE in the script\n"
            "  3. Market is closed / pre-market (spot price may be 0)"
        )
        return

    # --- Step 4: Start background DB flush thread ---
    t = threading.Thread(target=db_updater, args=(engine,), daemon=True)
    t.start()

    # --- Step 5: Start KiteTicker (WebSocket) ---
    kws = KiteTicker(creds['api_key'], creds['access_token'])
    kws.on_ticks   = on_ticks
    kws.on_connect = on_connect
    kws.on_close   = on_close
    kws.on_error   = on_error

    logger.info("Connecting to Kite Ticker WebSocket...")
    kws.connect(threaded=True)

    # Keep main thread alive
    while True:
        time.sleep(1)


if __name__ == "__main__":
    main()
