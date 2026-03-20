import os
import sys
from datetime import datetime

os.environ['FLASK_APP'] = 'app'
# we must import the app and run inside app context
from app import create_app
from app.models import db, OptionChainData, IndexData

app = create_app()

with app.app_context():
    query_date = datetime.strptime('2026-03-20', '%Y-%m-%d').date()
    market_open = datetime.combine(query_date, datetime.strptime('09:15', '%H:%M').time())
    market_close = datetime.combine(query_date, datetime.strptime('15:30', '%H:%M').time())
    
    # 1. Check a sample of OptionChainData on this date
    sample = db.session.query(OptionChainData).filter(
        db.func.date(OptionChainData.timestamp) == query_date
    ).first()
    
    print(f"Sample row:")
    if sample:
        print(f"  timestamp: {sample.timestamp}")
        print(f"  strike: {sample.strike_price} type: {type(sample.strike_price)}")
        print(f"  expiry: {sample.expiry_date} type: {type(sample.expiry_date)}")
        print(f"  underlying: {sample.underlying}")
        
    print(f"\nTime range queried: {market_open} to {market_close}")
    
    # 2. Check expiries
    expiries = db.session.query(OptionChainData.expiry_date).filter(
        db.func.date(OptionChainData.timestamp) == query_date
    ).distinct().all()
    print(f"\nDistinct expiries on {query_date}:")
    for e in expiries:
        print(f"  {e[0]} (type: {type(e[0])})")
        
    # 3. Check strikes for 2026-03-24
    if any(str(e[0]) == '2026-03-24' for e in expiries):
        strikes = db.session.query(OptionChainData.strike_price).filter(
            db.func.date(OptionChainData.timestamp) == query_date,
            OptionChainData.expiry_date == '2026-03-24'
        ).distinct().order_by(OptionChainData.strike_price).all()
        s_list = [s[0] for s in strikes]
        print(f"\nStrikes for expiry 2026-03-24: {len(s_list)} found")
        print(f"  Min: {min(s_list) if s_list else 'N/A'}")
        print(f"  Max: {max(s_list) if s_list else 'N/A'}")
        
        # Check what strikes exist near 23100
        near_23100 = [s for s in s_list if 22800 <= s <= 23400]
        print(f"  Strikes between 22800 and 23400: {near_23100}")
