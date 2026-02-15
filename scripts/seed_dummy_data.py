
import os
import sys
import random
import math
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, time
from sqlalchemy import create_engine, text

# Add parent directory to path to import app config
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
try:
    from app.config import config
except ImportError:
    # Fallback
    sys.path.append('c:/apps/power_app')
    from app.config import config

def get_db_engine():
    db_config = config['development']
    return create_engine(db_config.SQLALCHEMY_DATABASE_URI)

def generate_index_data(engine, symbol, start_price, volatility):
    print(f"Generating Index Data for {symbol}...")
    
    # Time range: Today 9:15 to 3:30
    today = datetime.now().date()
    start_time = datetime.combine(today, time(9, 15))
    end_time = datetime.combine(today, time(15, 30))
    # If today is weekend, use Friday? No, user said "today".
    # If currently it's before 9:15, maybe generate for yesterday?
    # Let's check current time.
    if datetime.now() < start_time:
         # Use yesterday?
         # But user said "for today". If it's early morning, maybe they mean "simulate a day".
         # Let's just generate for "today" even if it's in the future relative to now?
         # Or stick to "today" date but generate full day.
         pass

    current_time = start_time
    price = start_price
    
    rows = []
    
    # For consistent random walk
    np.random.seed(42 if symbol == 'NIFTY' else 43)
    
    while current_time <= end_time:
        # Random walk
        change = np.random.normal(0, volatility)
        price += change
        
        # OHLC
        open_p = price
        high_p = price + abs(np.random.normal(0, volatility/2))
        low_p = price - abs(np.random.normal(0, volatility/2))
        close_p = (open_p + high_p + low_p) / 3 # Rough approx
        
        # Adjust close to be next open
        price = close_p
        
        # Indictors (Mocked)
        ma_20 = close_p * (1 + math.sin(current_time.minute/10) * 0.001)
        ma_200 = close_p * 0.99
        
        # Supertrend (Mocked)
        # Alternate every hour
        st_dir = 1 if (current_time.hour % 2) == 0 else -1
        st = close_p - (50 if st_dir == 1 else -50)
        
        row = {
            'timestamp': current_time,
            'symbol': symbol,
            'instrument_token': 256265 if symbol == 'NIFTY' else 260105,
            'open': open_p,
            'high': high_p,
            'low': low_p,
            'close': close_p,
            'ma_20': ma_20,
            'ma_200': ma_200,
            'fast_ma_39': ma_20, # reuse 
            'fast_ma_69': ma_200, # reuse
            'atr': volatility * 2,
            'adx': 25.0 + math.sin(current_time.minute/20) * 10,
            'supertrend': st,
            'supertrend_direction': st_dir,
            'super_power': 'BUY' if st_dir == 1 else 'SELL',
            'super_power_brick': 10.0
        }
        rows.append(row)
        current_time += timedelta(minutes=1)
        
    # Bulk Insert
    if rows:
        df = pd.DataFrame(rows)
        # Delete existing for today
        with engine.begin() as conn:
            conn.execute(text(f"DELETE FROM index_data WHERE symbol='{symbol}' AND date(timestamp)='{today}'"))
            df.to_sql('index_data', conn, if_exists='append', index=False)
            
    print(f"Inserted {len(rows)} index records for {symbol}")
    return rows # Return for option chain generation

def generate_option_chain(engine, index_rows, symbol):
    print(f"Generating Option Chain for {symbol}...")
    if not index_rows:
        return

    # Filter to 5-min intervals
    ticks = [r for i, r in enumerate(index_rows) if i % 5 == 0]
    
    rows = []
    today = datetime.now().date()
    expiry = today + timedelta(days=(3 - today.weekday() + 7) % 7)
    
    # Strikes around initial price
    center_price = round(index_rows[0]['close'] / 50) * 50
    strikes = range(int(center_price) - 200, int(center_price) + 200 + 50, 50)
    
    for tick in ticks:
        spot = tick['close']
        timestamp = tick['timestamp']
        
        for strike in strikes:
            # Mock Pricing (Black-scholes-ish but simplified)
            dist = spot - strike
            
            # CE
            ce_intrinsic = max(0, dist)
            ce_time_value = 100 * math.exp(-0.0001 * abs(dist)) # Decaying time value
            ce_ltp = ce_intrinsic + ce_time_value
            
            # PE
            pe_intrinsic = max(0, -dist)
            pe_time_value = 100 * math.exp(-0.0001 * abs(dist))
            pe_ltp = pe_intrinsic + pe_time_value
            
            row = {
                'underlying': symbol,
                'strike_price': float(strike),
                'expiry_date': expiry,
                'timestamp': timestamp,
                'is_current_expiry': True,
                
                # CE
                'ce_oi': int(100000 + (1000 * math.sin(timestamp.hour))),
                'ce_oi_change': int((random.random() - 0.5) * 5000), # Mock OI Change
                'ce_volume': int(5000 + (100 * math.cos(timestamp.minute))),
                'ce_ltp': ce_ltp,
                'ce_change': (random.random() - 0.5) * 10,
                'ce_change_percent': (random.random() - 0.5) * 5,
                'ce_iv': 15.0,
                'ce_strike_symbol': f"{symbol}{strike}CE",
                
                # PE
                'pe_oi': int(100000 - (1000 * math.sin(timestamp.hour))),
                'pe_oi_change': int((random.random() - 0.5) * 5000), # Mock OI Change
                'pe_volume': int(5000 - (100 * math.cos(timestamp.minute))),
                'pe_ltp': pe_ltp,
                'pe_change': (random.random() - 0.5) * 10,
                'pe_change_percent': (random.random() - 0.5) * 5,
                'pe_iv': 16.0,
                'pe_strike_symbol': f"{symbol}{strike}PE"
            }
            rows.append(row)
            
    # Bulk Insert
    if rows:
        df = pd.DataFrame(rows)
        with engine.begin() as conn:
            conn.execute(text(f"DELETE FROM option_chain_data WHERE underlying='{symbol}' AND date(timestamp)='{today}'"))
            df.to_sql('option_chain_data', conn, if_exists='append', index=False)
            
    print(f"Inserted {len(rows)} option chain records for {symbol}")

if __name__ == "__main__":
    engine = get_db_engine()
    
    nifty_rows = generate_index_data(engine, 'NIFTY', 19500, 5.0)
    generate_option_chain(engine, nifty_rows, 'NIFTY')
    
    bank_rows = generate_index_data(engine, 'BANKNIFTY', 44500, 15.0)
    generate_option_chain(engine, bank_rows, 'BANKNIFTY')
    
    print("✅ Dummy data generation complete!")
