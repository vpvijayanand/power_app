from app import create_app, db
from app.models import Instrument
from sqlalchemy import func

app = create_app('development')

with app.app_context():
    print("--- unique instrument_types ---")
    types = db.session.query(Instrument.instrument_type).distinct().all()
    print([t[0] for t in types])

    print("\n--- unique segments ---")
    segments = db.session.query(Instrument.segment).distinct().all()
    print([s[0] for s in segments])

    print("\n--- unique exchanges ---")
    exchanges = db.session.query(Instrument.exchange).distinct().all()
    print([e[0] for e in exchanges])
    
    print("\n--- sample expiry dates (first 10) ---")
    expiries = db.session.query(Instrument.expiry).distinct().order_by(Instrument.expiry).limit(10).all()
    print([str(e[0]) for e in expiries])
