import sys
import os
from kiteconnect import KiteConnect
from sqlalchemy import create_engine, text

# Setup
sys.path.append(os.getcwd())
from app.config import config

def get_creds_and_token():
    engine = create_engine(config['development'].SQLALCHEMY_DATABASE_URI)
    with engine.connect() as conn:
        creds = conn.execute(text("SELECT api_key, access_token FROM users WHERE user_type='Admin' AND access_token IS NOT NULL")).fetchone()
        token = conn.execute(text("SELECT instrument_token FROM instruments WHERE segment='NFO-OPT' AND name='NIFTY' LIMIT 1")).scalar()
        symbol = conn.execute(text("SELECT tradingsymbol FROM instruments WHERE instrument_token=:token"), {"token": token}).scalar()
    return creds, token, symbol

creds, token, symbol = get_creds_and_token()
print(f"Token: {token}, Symbol: {symbol}")

kite = KiteConnect(api_key=creds[0], access_token=creds[1])
# Quote requires "exchange:symbol"
# We need to find the exchange. NFO.
exchange_symbol = f"NFO:{symbol}"
print(f"Fetching quote for {exchange_symbol}...")

try:
    quote = kite.quote(exchange_symbol)
    print("--- QUOTE RECEIVED ---")
    print(quote)
except Exception as e:
    print(f"Error: {e}")
