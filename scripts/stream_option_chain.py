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
initial_oi_map = {} # Token -> Initial OI
cache_lock = threading.Lock()

# Map instrument token to details for quick lookup
token_map = {}

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

def get_option_instruments(engine):
    """Fetch and filter option instruments for NIFTY & BANKNIFTY"""
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
    today = datetime.now().date()
    
    selected_tokens = []
    local_token_map = {}
    
    for underlying in ['NIFTY', 'BANKNIFTY']:
        # Filter for underlying
        udf = df[df['name'] == underlying]
        
        # Get sorted unique expiries
        expiries = sorted(udf['expiry'].unique())
        
        # Select required number of expiries
        limit = 4 if underlying == 'NIFTY' else 2
        selected_expiries = expiries[:limit]
        
        logger.info(f"Selected expiries for {underlying}: {selected_expiries}")
        
        # Filter instruments for these expiries
        selected_instruments = udf[udf['expiry'].isin(selected_expiries)]
        
        for _, row in selected_instruments.iterrows():
            token = row['instrument_token']
            selected_tokens.append(token)
            local_token_map[token] = {
                'underlying': row['name'],
                'expiry': row['expiry'],
                'strike': row['strike'],
                'type': row['instrument_type'], # CE or PE
                'symbol': row['tradingsymbol'],
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
                    'underlying': details['underlying'],
                    'expiry_date': details['expiry'],
                    'strike_price': details['strike'],
                    'timestamp': datetime.now(),
                    'is_current_expiry': details['is_current'],
                    'ce': {}, 'pe': {}
                }
            
            # Handle Initial OI for Change Calculation
            current_oi = tick.get('oi', 0)
            if token not in initial_oi_map:
                initial_oi_map[token] = current_oi
                
            # Calculate OI Change (from script start)
            initial_oi = initial_oi_map[token]
            oi_change = current_oi - initial_oi
            
            # Extract data
            data = {
                'oi': current_oi,
                'oi_change': oi_change,
                'volume': tick.get('volume_traded', 0),
                'ltp': tick.get('last_price', 0),
                'change': tick.get('change', 0), 
                'change_percent': 0, # Calculate later
                'token': token,
                'symbol': details['symbol']
            }
            
            # Calculate change and percent for PRICE if ohlc is available
            if 'ohlc' in tick:
                close_price = tick['ohlc']['close']
                if close_price > 0:
                    data['change'] = data['ltp'] - close_price
                    data['change_percent'] = (data['change'] / close_price) * 100
            
            # Assign to CE or PE bucket
            if details['type'] == 'CE':
                option_chain_cache[key]['ce'] = data
            elif details['type'] == 'PE':
                option_chain_cache[key]['pe'] = data
    
    # logger.info(f"Received {len(ticks)} ticks")

def on_connect(ws, response):
    """Callback on successful connection"""
    logger.info("Connected to Kite Ticker")
    ws.subscribe(list(token_map.keys()))
    ws.set_mode(ws.MODE_FULL, list(token_map.keys()))
    logger.info("Subscribed to tokens")

def on_close(ws, code, reason):
    logger.info(f"Connection closed: {code} - {reason}")

def on_error(ws, code, reason):
    logger.error(f"Error: {code} - {reason}")

def db_updater(engine):
    """Background thread to flush cache to DB"""
    logger.info("Starting DB updater thread...")
    while True:
        # Wait for the next minute start to align data?
        # Or just sleep 60s. Aligning is better for "1 minute" charts.
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
                
            # Prepare batch data
            # USE SINGLE TIMESTAMP FOR ALL ROWS IN BATCH
            batch_timestamp = datetime.now().replace(second=0, microsecond=0)
            
            rows = []
            for key, data in snapshot.items():
                ce = data['ce']
                pe = data['pe']
                
                # Create row dict
                row = {
                    'underlying': data['underlying'],
                    'strike_price': data['strike_price'],
                    'expiry_date': data['expiry_date'],
                    'timestamp': batch_timestamp,
                    'is_current_expiry': data['is_current_expiry'],
                    
                    # CE Data
                    'ce_oi': ce.get('oi'),
                    'ce_oi_change': ce.get('oi_change'),
                    'ce_volume': ce.get('volume'),
                    'ce_ltp': ce.get('ltp'),
                    'ce_change': ce.get('change'),
                    'ce_change_percent': ce.get('change_percent'),
                    'ce_strike_symbol': ce.get('symbol'),
                    'ce_instrument_token': str(ce.get('token')) if ce.get('token') else None,
                    
                    # PE Data
                    'pe_oi': pe.get('oi'),
                    'pe_oi_change': pe.get('oi_change'),
                    'pe_volume': pe.get('volume'),
                    'pe_ltp': pe.get('ltp'),
                    'pe_change': pe.get('change'),
                    'pe_change_percent': pe.get('change_percent'),
                    'pe_strike_symbol': pe.get('symbol'),
                    'pe_instrument_token': str(pe.get('token')) if pe.get('token') else None
                }
                rows.append(row)
            
            if not rows:
                continue

            # Check if rows exist to update or insert
            # Since user asked simply to "store", and standard option chain implies current state.
            # Storing history would explode the table size quickly (ticks per second * 2000 strikes).
            # I will assume "Upsert" logic: update if exists, insert if not.
            # However, standard SQLAlchemy bulk insert is insert-only.
            # To handle updates efficiently, we can delete current snapshot and insert new?
            # Or use PostgreSQL ON CONFLICT.
            
            # Ideally we should identify a unique row by underlying + expiry + strike.
            # But the table has 'id' serial primary key and no unique constraint on (underlying, expiry, strike).
            # The user request implies a history of data ("timestamp" field).
            # BUT streaming every second to DB is heavy.
            # "gets all the data ... and store in the below table"
            # I will assume append-only for now but maybe throttle it?
            # NO, option chain usually means "Latest View".
            # BUT "timestamp" suggests time series.
            # Given the request is for a websocket script, it usually implies high frequency data collection.
            # I will implement APPEND for now, but I'll limit the frequency to 1 sec.
            # Wait, 1 sec update for 2000 rows is BIG. 2000 inserts/sec.
            # Maybe the user wants a snapshot every minute? 
            # Or maybe just the LATEST state?
            # "store in the below table" -> typically means populate it.
            # Let's try to UPDATE existing rows for the same timestamp? No, timestamp changes.
            # It's better to treat it as "Latest State" table for a real-time view?
            # If I just append, the table grows by ~1000 rows/sec.
            # I will assume the user wants the LATEST state for the dashboard.
            # So I should DELETE old record for (underlying, expiry, strike) or UPDATE it.
            # Let's use Upsert logic using SQL.
            
            with engine.connect() as conn:
                # We will use a transaction to delete old data for these specific keys and insert new
                # actually, deleting is slow.
                # Let's assume we want to KEEP history for analysis?
                # User asked "store in the below table".
                # I will create a new record for every snapshot interval (e.g. 5 seconds) to be safe?
                # or just stream it.
                # Let's stick to 1-second batch insert.
                
                # Actually, standard option chain view requires latest data.
                # If I append, fetching "latest" requires complicated queries.
                # I will use a "Latest State" approach: Delete all rows and insert new?
                # Or Update.
                # Since I don't have unique constraint, I can't easily upsert.
                # I'll implement "Insert New" but maybe flush every 5 seconds to reduce volume?
                # No, websocket implies real-time.
                
                # DECISION: I will Insert new rows. The user provided a schema with "timestamp".
                # This suggests time-series data.
                # I'll append data. Monitor performance.
                
                conn.execute(
                    text("""
                    INSERT INTO option_chain_data (
                        underlying, strike_price, expiry_date, timestamp, is_current_expiry,
                        ce_oi, ce_oi_change, ce_volume, ce_ltp, ce_change, ce_change_percent, ce_strike_symbol, ce_instrument_token,
                        pe_oi, pe_oi_change, pe_volume, pe_ltp, pe_change, pe_change_percent, pe_strike_symbol, pe_instrument_token
                    ) VALUES (
                        :underlying, :strike_price, :expiry_date, :timestamp, :is_current_expiry,
                        :ce_oi, :ce_oi_change, :ce_volume, :ce_ltp, :ce_change, :ce_change_percent, :ce_strike_symbol, :ce_instrument_token,
                        :pe_oi, :pe_oi_change, :pe_volume, :pe_ltp, :pe_change, :pe_change_percent, :pe_strike_symbol, :pe_instrument_token
                    )
                    """),
                    rows
                )
                conn.commit()
                # logger.info(f"Inserted {len(rows)} rows to DB")

        except Exception as e:
            logger.error(f"DB Update Error: {e}")

def main():
    logger.info("Initializing Option Chain Streamer...")
    engine = get_db_engine()
    
    # Get credentials
    creds = get_admin_credentials(engine)
    if not creds:
        logger.error("Admin credentials not found!")
        return
        
    # Get tokens
    global token_map
    tokens, token_map = get_option_instruments(engine)
    if not tokens:
        logger.error("No tokens found.")
        return
        
    # Start DB updater
    t = threading.Thread(target=db_updater, args=(engine,))
    t.daemon = True
    t.start()
    
    # Initialize Ticker
    kws = KiteTicker(creds['api_key'], creds['access_token'])
    
    kws.on_ticks = on_ticks
    kws.on_connect = on_connect
    kws.on_close = on_close
    kws.on_error = on_error
    
    logger.info("Connecting to Kite Ticker...")
    kws.connect(threaded=True)
    
    # Keep main thread alive
    while True:
        time.sleep(1)

if __name__ == "__main__":
    main()
