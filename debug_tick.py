import sys
import os
import logging
from kiteconnect import KiteTicker
from sqlalchemy import create_engine, text

# Setup
sys.path.append(os.getcwd())
from app.config import config

logging.basicConfig(level=logging.DEBUG)

def get_creds_and_token():
    engine = create_engine(config['development'].SQLALCHEMY_DATABASE_URI)
    with engine.connect() as conn:
        # Creds
        creds = conn.execute(text("SELECT api_key, access_token FROM users WHERE user_type='Admin' AND access_token IS NOT NULL")).fetchone()
        # Token (Any NIFTY Option)
        token = conn.execute(text("SELECT instrument_token FROM instruments WHERE segment='NFO-OPT' AND name='NIFTY' LIMIT 1")).scalar()
    return creds, token

creds, token = get_creds_and_token()
print(f"Token: {token}")

kws = KiteTicker(creds[0], creds[1])

def on_ticks(ws, ticks):
    with open("tick_dump.txt", "w") as f:
        f.write(str(ticks[0]))
    print("Tick written to tick_dump.txt", flush=True)
    ws.close()

def on_connect(ws, response):
    ws.subscribe([token])
    ws.set_mode(ws.MODE_FULL, [token])

def on_close(ws, code, reason):
    print(f"Closed: {code} - {reason}", flush=True)
    sys.exit(0)

kws.on_ticks = on_ticks
kws.on_connect = on_connect
kws.on_close = on_close
kws.connect()
