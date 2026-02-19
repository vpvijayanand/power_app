import sys
import os
from datetime import datetime
from sqlalchemy import create_engine, func, text
from sqlalchemy.orm import sessionmaker

# Setup
sys.path.append(os.getcwd())
from app.config import config
from app.models import IndexData

def check_data():
    engine = create_engine(config['development'].SQLALCHEMY_DATABASE_URI)
    Session = sessionmaker(bind=engine)
    session = Session()

    print("Checking IndexData for today...")
    today = datetime.now().date()
    
    # Get latest 5 rows
    rows = session.query(IndexData).filter(
        func.date(IndexData.timestamp) == today
    ).order_by(IndexData.timestamp.desc()).limit(5).all()
    
    output = []
    if not rows:
        output.append("No IndexData found for today.")
    else:
        output.append(f"Found {len(rows)} rows. Inspecting latest:")
        for row in rows:
            output.append(f"Timestamp: {row.timestamp.strftime('%H:%M:%S')}")
            output.append(f"  Close: {row.close}")
            output.append(f"  MA20: {row.ma_20}")
            output.append(f"  MA200: {row.ma_200}")
            output.append(f"  ADX: {row.adx}")
            output.append(f"  ATR: {row.atr}")
            output.append("-" * 20)

    with open("debug_index_data.txt", "w", encoding="utf-8") as f:
        f.write("\n".join(output))
    print("Output written to debug_index_data.txt")

if __name__ == "__main__":
    check_data()
