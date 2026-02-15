import os
import sys
import random
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text

# Add parent directory to path to import app config
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from app.config import config

def get_db_engine():
    db_config = config['development']
    return create_engine(db_config.SQLALCHEMY_DATABASE_URI)

def seed_data():
    engine = get_db_engine()
    
    # Minimal Configuration for Speed
    underlyings = [
        {'name': 'NIFTY', 'spot': 19500, 'step': 50, 'range': 2} # Just 5 rows (center +/- 2)
    ]
    
    today = datetime.now().date()
    # Nearest Thursday
    expiry = today + timedelta(days=(3 - today.weekday() + 7) % 7)
    expiries = [expiry]
        
    rows = []
    timestamp = datetime.now()
    
    print(f"Generating data for expiry: {expiry}")

    for expiry in expiries:
        for u in underlyings:
            spot = u['spot']
            step = u['step']
            count = u['range']
            
            # Generate strikes
            min_strike = spot - (count * step)
            max_strike = spot + (count * step)
            
            for strike in range(min_strike, max_strike + step, step):
                # Simulate data
                ce_ltp = 100.0
                pe_ltp = 50.0
                
                row = {
                    'underlying': u['name'],
                    'strike_price': float(strike),
                    'expiry_date': expiry,
                    'timestamp': timestamp,
                    'is_current_expiry': True,
                    
                    # CE
                    'ce_oi': 10000,
                    'ce_oi_change': 500,
                    'ce_volume': 50000,
                    'ce_ltp': ce_ltp,
                    'ce_change': 5.0,
                    'ce_change_percent': 5.0,
                    'ce_iv': 15.0,
                    'ce_strike_symbol': f"{u['name']}26{expiry.strftime('%b').upper()}{strike}CE",
                    
                    # PE
                    'pe_oi': 8000,
                    'pe_oi_change': -200,
                    'pe_volume': 40000,
                    'pe_ltp': pe_ltp,
                    'pe_change': -2.0,
                    'pe_change_percent': -4.0,
                    'pe_iv': 16.0,
                    'pe_strike_symbol': f"{u['name']}26{expiry.strftime('%b').upper()}{strike}PE"
                }
                rows.append(row)
    
    print(f"Prepared {len(rows)} rows. Inserting...")
    
    with engine.connect() as conn:
        try:
            # Clear existing
            conn.execute(text("DELETE FROM option_chain_data"))
            conn.commit()
            
            # Insert new
            if rows:
                conn.execute(
                    text("""
                    INSERT INTO option_chain_data (
                        underlying, strike_price, expiry_date, timestamp, is_current_expiry,
                        ce_oi, ce_oi_change, ce_volume, ce_ltp, ce_change, ce_change_percent, ce_iv, ce_strike_symbol,
                        pe_oi, pe_oi_change, pe_volume, pe_ltp, pe_change, pe_change_percent, pe_iv, pe_strike_symbol
                    ) VALUES (
                        :underlying, :strike_price, :expiry_date, :timestamp, :is_current_expiry,
                        :ce_oi, :ce_oi_change, :ce_volume, :ce_ltp, :ce_change, :ce_change_percent, :ce_iv, :ce_strike_symbol,
                        :pe_oi, :pe_oi_change, :pe_volume, :pe_ltp, :pe_change, :pe_change_percent, :pe_iv, :pe_strike_symbol
                    )
                    """),
                    rows
                )
                conn.commit()
                print("Commit successful!")
        except Exception as e:
            print(f"Error: {e}")
        
    print(f"Done. {len(rows)} records inserted.")

if __name__ == "__main__":
    seed_data()
