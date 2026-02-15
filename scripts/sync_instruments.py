"""
Standalone script to fetch and sync instruments from Kite to database.
This script is designed to be run as a cron job once per day.

Usage:
    python scripts/sync_instruments.py

Requirements:
    - Admin user must have valid Kite API credentials and access token
    - Database must be accessible
"""

import sys
import os
from pathlib import Path

# Add parent directory to path to import app modules
sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine, text
from app.models import db, User, Instrument
from app.config import config


def fetch_and_store_instruments():
    """Main function to fetch instruments from Kite and store in database"""
    print("=" * 60)
    print("🔄 Starting Instruments Sync (Public Mode)...")
    print("=" * 60)
    
    # 1. Fetch Instruments (Public URL)
    print("\n📥 Step 1: Downloading instruments CSV from Kite public endpoint...")
    try:
        url = "https://api.kite.trade/instruments"
        print(f"   URL: {url}")
        
        # Read CSV directly into DataFrame
        df = pd.read_csv(url)
        print(f"✅ Downloaded {len(df):,} instruments")
        
    except Exception as e:
        print(f"❌ Failed to download instruments: {e}")
        return False

    # 2. Process Data
    print("\n⚙️  Step 2: Processing instrument data...")
    try:
        # Add fetch_date
        today = datetime.now().date()
        df['fetch_date'] = today
        
        # Add expiry_weekday
        df['expiry'] = pd.to_datetime(df['expiry'], errors='coerce')
        df['expiry_weekday'] = df['expiry'].dt.day_name()
        
        # Add timestamps
        df['created_at'] = datetime.now()
        df['updated_at'] = datetime.now()
        
        # Select columns matching the model
        target_cols = [
            'instrument_token', 'exchange_token', 'tradingsymbol', 'name', 'last_price', 
            'expiry', 'strike', 'tick_size', 'lot_size', 'instrument_type', 'segment', 
            'exchange', 'created_at', 'updated_at', 'fetch_date', 'expiry_weekday'
        ]
        
        # Ensure all target cols exist in df (fill missing with None)
        for col in target_cols:
            if col not in df.columns:
                df[col] = None
                
        df = df[target_cols]
        
        print(f"✅ Processed {len(df):,} records")
        
    except Exception as e:
        print(f"❌ Data Processing Error: {e}")
        return False

    # 3. Store in Database
    print("\n💾 Step 3: Storing data in database...")
    try:
        db_config = config['development']
        engine = create_engine(db_config.SQLALCHEMY_DATABASE_URI)
        
        # Delete old data for today to avoid duplicates
        with engine.connect() as conn:
            result = conn.execute(text(f"DELETE FROM instruments WHERE fetch_date = '{today}'"))
            conn.commit()
            deleted_count = result.rowcount
            if deleted_count > 0:
                print(f"🗑️  Deleted {deleted_count:,} existing records for {today}")

        # Insert using pandas to_sql
        print(f"📝 Inserting {len(df):,} records...")
        df.to_sql('instruments', engine, if_exists='append', index=False, chunksize=5000)
        
        print(f"✅ Successfully inserted {len(df):,} records")
        
    except Exception as e:
        print(f"❌ Database Error: {e}")
        return False

    # 4. Summary
    print("\n" + "=" * 60)
    print("✅ Instruments Sync Completed Successfully!")
    print("=" * 60)
    print(f"📊 Summary:")
    print(f"   - Total Instruments: {len(df):,}")
    print(f"   - Fetch Date: {today}")
    print(f"   - Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    
    return True


if __name__ == "__main__":
    try:
        success = fetch_and_store_instruments()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n\n⚠️  Script interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n\n❌ Unexpected Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
