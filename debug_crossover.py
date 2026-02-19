import sys
import os
from datetime import datetime
from sqlalchemy import create_engine, func, text
from sqlalchemy.orm import sessionmaker

# Setup
sys.path.append(os.getcwd())
from app.config import config
from app.models import OptionChainData, IndexData

# DB Query Helper
def get_data():
    engine = create_engine(config['development'].SQLALCHEMY_DATABASE_URI)
    Session = sessionmaker(bind=engine)
    session = Session()

    query_date = datetime.now().date()
    # query_date = datetime.strptime("2026-02-16", "%Y-%m-%d").date() # Force today if needed
    
    print(f"Querying for Date: {query_date}")

    # 1. Get Expiry
    expiries = session.query(OptionChainData.expiry_date).filter(
        OptionChainData.expiry_date >= query_date
    ).distinct().order_by(OptionChainData.expiry_date).all()
    expiries = [e[0] for e in expiries if e[0]]
    
    if not expiries:
        print("No expiries found.")
        return

    expiry = expiries[0]
    underlying = 'NIFTY'
    print(f"Selected Expiry: {expiry}")

    # 2. Fetch Option Data
    option_query = session.query(
        OptionChainData.timestamp,
        func.sum(OptionChainData.ce_oi).label('ce_oi_total'),
        func.sum(OptionChainData.pe_oi).label('pe_oi_total'),
        func.sum(OptionChainData.ce_oi_change).label('ce_change_total'),
        func.sum(OptionChainData.pe_oi_change).label('pe_change_total')
    ).filter(
        OptionChainData.underlying == underlying,
        OptionChainData.expiry_date == expiry,
        func.date(OptionChainData.timestamp) == query_date
    ).group_by(OptionChainData.timestamp).order_by(OptionChainData.timestamp)
    
    option_data = option_query.all()
    print(f"Option Data Rows: {len(option_data)}")
    if len(option_data) > 0:
        print(f"Sample Option Row: {option_data[0].timestamp} - CE: {option_data[0].ce_oi_total}")

    # 3. Fetch Index Data
    index_query = session.query(IndexData).filter(
        IndexData.symbol == underlying,
        func.date(IndexData.timestamp) == query_date
    ).order_by(IndexData.timestamp).all()
    
    print(f"Index Data Rows: {len(index_query)}")
    if len(index_query) > 0:
        print(f"Sample Index Row: {index_query[0].timestamp} - Close: {index_query[0].close}")

    # 4. Merge
    data_map = {}
    
    for row in option_data:
        ts = row.timestamp.strftime('%H:%M')
        if ts not in data_map:
            data_map[ts] = {}
        data_map[ts].update({
            'ce_oi': row.ce_oi_total,
            'pe_oi': row.pe_oi_total
        })
        
    for row in index_query:
        ts = row.timestamp.strftime('%H:%M')
        if ts not in data_map:
            data_map[ts] = {}
        data_map[ts]['price'] = row.close
        
    print(f"Merged Data Points (Minutes): {len(data_map)}")
    
    sorted_keys = sorted(data_map.keys())
    if sorted_keys:
        print(f"First 5 keys: {sorted_keys[:5]}")
        first_key = sorted_keys[0]
        print(f"Data for {first_key}: {data_map[first_key]}")

if __name__ == "__main__":
    get_data()
