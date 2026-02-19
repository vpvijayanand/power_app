import os
import sys
import logging
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import create_engine, text
from kiteconnect import KiteConnect

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from app.config import config
from scripts.stream_index import Indicators, TOKENS

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Config
DB_CONFIG = config['development']
DATABASE_URI = DB_CONFIG.SQLALCHEMY_DATABASE_URI

def get_db_engine():
    return create_engine(DATABASE_URI)

def get_admin_credentials(engine):
    with engine.connect() as conn:
        result = conn.execute(text("SELECT api_key, access_token FROM users WHERE user_type = 'Admin' AND access_token IS NOT NULL LIMIT 1")).fetchone()
        if not result:
            logger.error("No Admin credentials found!")
            sys.exit(1)
        return {'api_key': result[0], 'access_token': result[1]}

def fetch_and_process_history(kite, symbol, token, days=5):
    to_date = datetime.now()
    from_date = to_date - timedelta(days=days)
    
    logger.info(f"Fetching history for {symbol} ({token}) from {from_date} to {to_date}")
    
    try:
        # Fetch minute data
        records = kite.historical_data(token, from_date, to_date, "minute")
        if not records:
            logger.warning(f"No records found for {symbol}")
            return pd.DataFrame()
            
        df = pd.DataFrame(records)
        df.set_index('date', inplace=True)
        # Rename index to timestamp to match our model
        df.index.name = 'timestamp'
        
        # Calculate Indicators
        logger.info(f"Calculating indicators for {symbol}...")
        df['ma_20'] = Indicators.calculate_sma(df['close'], 20)
        df['ma_200'] = Indicators.calculate_sma(df['close'], 200)
        df['fast_ma_39'] = Indicators.calculate_sma(df['close'], 39)
        df['fast_ma_69'] = Indicators.calculate_sma(df['close'], 69)
        
        df['atr'] = Indicators.calculate_atr(df, 14)
        df['adx'] = Indicators.calculate_adx(df, 14)
        
        st, st_dir = Indicators.calculate_supertrend(df, 10, 3)
        df['supertrend'] = st
        df['supertrend_direction'] = st_dir
        
        renko = Indicators.calculate_renko(df, 10)
        df['super_power'] = renko
        
        return df
        
    except Exception as e:
        logger.error(f"Error fetching history: {e}")
        return pd.DataFrame()

def save_to_db(engine, symbol, token, df):
    if df.empty:
        return

    logger.info(f"Saving {len(df)} rows for {symbol} to DB...")
    
    # We want to insert or ignore, or delete overlap?
    # Deleting overlap is safer to avoid unique constraint errors if any.
    # But usually timestamp+symbol is unique?
    # Let's check model definition. It likely has PK on ID.
    # Ideally we clear the range we fetched and re-insert.
    
    min_ts = df.index.min()
    max_ts = df.index.max()
    
    with engine.begin() as conn:
        # Delete existing data in this range
        conn.execute(text(f"DELETE FROM index_data WHERE symbol = '{symbol}' AND timestamp >= :min_ts AND timestamp <= :max_ts"), {'min_ts': min_ts, 'max_ts': max_ts})
        
        # Insert new data
        count = 0
        for timestamp, row in df.iterrows():
            conn.execute(text("""
                INSERT INTO index_data (
                    timestamp, symbol, instrument_token, open, high, low, close,
                    ma_20, ma_200, fast_ma_39, fast_ma_69, atr, adx,
                    supertrend, supertrend_direction, super_power, super_power_brick
                ) VALUES (
                    :timestamp, :symbol, :token, :open, :high, :low, :close,
                    :ma_20, :ma_200, :ma_39, :ma_69, :atr, :adx,
                    :st, :st_dir, :renko, 10.0
                )
            """), {
                'timestamp': timestamp,
                'symbol': symbol,
                'token': token,
                'open': float(row['open']),
                'high': float(row['high']),
                'low': float(row['low']),
                'close': float(row['close']),
                'ma_20': float(row['ma_20']) if pd.notna(row['ma_20']) else None,
                'ma_200': float(row['ma_200']) if pd.notna(row['ma_200']) else None,
                'ma_39': float(row['fast_ma_39']) if pd.notna(row['fast_ma_39']) else None,
                'ma_69': float(row['fast_ma_69']) if pd.notna(row['fast_ma_69']) else None,
                'atr': float(row['atr']) if pd.notna(row['atr']) else None,
                'adx': float(row['adx']) if pd.notna(row['adx']) else None,
                'st': float(row['supertrend']) if pd.notna(row['supertrend']) else None,
                'st_dir': int(row['supertrend_direction']) if pd.notna(row['supertrend_direction']) else None,
                'renko': str(row['super_power']) if row['super_power'] else None
            })
            count += 1
            
    logger.info(f"Saved {count} records.")

def main():
    engine = get_db_engine()
    creds = get_admin_credentials(engine)
    
    logger.info("Connecting to Kite...")
    kite = KiteConnect(api_key=creds['api_key'])
    kite.set_access_token(creds['access_token'])
    
    for token, symbol in TOKENS.items():
        df = fetch_and_process_history(kite, symbol, token, days=5) # 5 days history
        save_to_db(engine, symbol, token, df)
        
    logger.info("Backfill complete!")

if __name__ == "__main__":
    main()
