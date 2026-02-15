import os
import sys
import logging
import json
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from kiteconnect import KiteTicker

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Add parent directory to path to import app config
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
try:
    from app.config import config
except ImportError:
    # Fallback for manual run
    sys.path.append('c:/apps/power_app')
    from app.config import config

# Tokens
TOKENS = {
    256265: 'NIFTY',
    260105: 'BANKNIFTY'
}

# Configuration
DB_CONFIG = config['development']
DATABASE_URI = DB_CONFIG.SQLALCHEMY_DATABASE_URI

class Indicators:
    @staticmethod
    def calculate_sma(series, period):
        return series.rolling(window=period).mean()

    @staticmethod
    def calculate_atr(df, period=14):
        high = df['high']
        low = df['low']
        close = df['close']
        prev_close = close.shift(1)
        
        tr1 = high - low
        tr2 = (high - prev_close).abs()
        tr3 = (low - prev_close).abs()
        
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        # Wilder's Smoothing
        atr = tr.ewm(alpha=1/period, adjust=False).mean()
        return atr

    @staticmethod
    def calculate_adx(df, period=14):
        high = df['high']
        low = df['low']
        close = df['close']
        
        up = high - high.shift(1)
        down = low.shift(1) - low
        
        plus_dm = np.where((up > down) & (up > 0), up, 0.0)
        minus_dm = np.where((down > up) & (down > 0), down, 0.0)
        
        tr = Indicators.calculate_atr(df, period) # This returns ATR, we need TR sum or smoothed? Standard ADX uses smoothed TR/DM.
        # Let's verify standard ADX calculation.
        # TR1 = ..., ATR = Wilder(TR, 14)
        # +DI = 100 * Wilder(+DM, 14) / ATR
        # -DI = 100 * Wilder(-DM, 14) / ATR
        # DX = 100 * abs(+DI - -DI) / (+DI + -DI)
        # ADX = Wilder(DX, 14)
        
        # Recalculate TR specifically for ADX if needed or reuse ATR series as denominator?
        # Standard formula: Smooth(TR), Smooth(+DM), Smooth(-DM).
        
        # TR
        prev_close = close.shift(1)
        tr1 = high - low
        tr2 = (high - prev_close).abs()
        tr3 = (low - prev_close).abs()
        tr_series = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        
        smooth_tr = tr_series.ewm(alpha=1/period, adjust=False).mean()
        smooth_plus_dm = pd.Series(plus_dm, index=df.index).ewm(alpha=1/period, adjust=False).mean()
        smooth_minus_dm = pd.Series(minus_dm, index=df.index).ewm(alpha=1/period, adjust=False).mean()
        
        plus_di = 100 * (smooth_plus_dm / smooth_tr)
        minus_di = 100 * (smooth_minus_dm / smooth_tr)
        
        dx = 100 * (abs(plus_di - minus_di) / (plus_di + minus_di))
        adx = dx.ewm(alpha=1/period, adjust=False).mean()
        return adx

    @staticmethod
    def calculate_supertrend(df, period=10, multiplier=3):
        # ATR
        atr = Indicators.calculate_atr(df, period)
        
        hl2 = (df['high'] + df['low']) / 2
        basic_upper = hl2 + (multiplier * atr)
        basic_lower = hl2 - (multiplier * atr)
        
        # To avoid loop, we need Numba or iterate. Iteration is fine for small batch or incremental.
        # For full history, iteration is slow. But we only need the *last* few values usually.
        # Here we re-calculate whole series.
        
        supertrend = [0.0] * len(df)
        direction = [1] * len(df) # 1 Up, -1 Down
        final_upper = [0.0] * len(df)
        final_lower = [0.0] * len(df)
        
        close = df['close'].values
        
        for i in range(1, len(df)):
            # Upper Band
            if basic_upper[i] < final_upper[i-1] or close[i-1] > final_upper[i-1]:
                final_upper[i] = basic_upper[i]
            else:
                final_upper[i] = final_upper[i-1]
                
            # Lower Band
            if basic_lower[i] > final_lower[i-1] or close[i-1] < final_lower[i-1]:
                final_lower[i] = basic_lower[i]
            else:
                final_lower[i] = final_lower[i-1]
                
            # Direction & Supertrend
            if direction[i-1] == 1: # Uptrend
                if close[i] < final_lower[i]:
                    direction[i] = -1
                    supertrend[i] = final_upper[i]
                else:
                    direction[i] = 1
                    supertrend[i] = final_lower[i]
            else: # Downtrend
                if close[i] > final_upper[i]:
                    direction[i] = 1
                    supertrend[i] = final_lower[i]
                else:
                    direction[i] = -1
                    supertrend[i] = final_upper[i]
                    
        return pd.Series(supertrend, index=df.index), pd.Series(direction, index=df.index)

    @staticmethod
    def calculate_renko(df, brick_size=10):
        # Renko is tricky on time-series. It's price driven.
        # We need to return the 'current' trend or signal based on bricks formed.
        # We'll calculate bricks from close prices.
        
        signals = [None] * len(df)
        
        if len(df) < 2:
            return pd.Series(signals, index=df.index)
            
        last_brick_price = df['close'].iloc[0]
        # Align to brick size
        last_brick_price = (last_brick_price // brick_size) * brick_size
        
        current_trend = 0 # 0 none, 1 up, -1 down
        
        for i in range(1, len(df)):
            close = df['close'].iloc[i]
            diff = close - last_brick_price
            
            num_bricks = int(diff / brick_size)
            
            if num_bricks != 0:
                new_brick_price = last_brick_price + (num_bricks * brick_size)
                
                # Signal logic: If Trend Changes?
                if num_bricks > 0: # Bullish
                    if current_trend <= 0:
                         signals[i] = 'BUY'
                         current_trend = 1
                    last_brick_price = new_brick_price # Update reference
                    
                elif num_bricks < 0: # Bearish
                    if current_trend >= 0:
                        signals[i] = 'SELL'
                        current_trend = -1
                    last_brick_price = new_brick_price
        
        return pd.Series(signals, index=df.index)

class IndexStreamer:
    def __init__(self):
        self.engine = create_engine(DATABASE_URI)
        self.credentials = self.get_credentials()
        self.candles = {
            'NIFTY': {'open': 0, 'high': 0, 'low': float('inf'), 'close': 0, 'ticks': 0},
            'BANKNIFTY': {'open': 0, 'high': 0, 'low': float('inf'), 'close': 0, 'ticks': 0}
        }
        self.current_minute = datetime.now().minute
        # Keep minimal history in memory for efficient calculation?
        # Actually better to fetch from DB on restart, and append in memory.
        self.history = {
            'NIFTY': pd.DataFrame(),
            'BANKNIFTY': pd.DataFrame()
        }
        self.load_history()

    def get_credentials(self):
        with self.engine.connect() as conn:
            result = conn.execute(text("SELECT api_key, access_token FROM users WHERE user_type = 'Admin' AND access_token IS NOT NULL LIMIT 1")).fetchone()
            if not result:
                logger.error("No Admin credentials found!")
                sys.exit(1)
            return {'api_key': result[0], 'access_token': result[1]}

    def load_history(self):
        # Load last 300 candles for warm-up
        logger.info("Loading history...")
        for symbol in self.history.keys():
            query = text(f"""
                SELECT timestamp, open, high, low, close 
                FROM index_data 
                WHERE symbol = '{symbol}' 
                ORDER BY timestamp DESC LIMIT 300
            """)
            df = pd.read_sql(query, self.engine)
            if not df.empty:
                df = df.sort_values('timestamp').set_index('timestamp')
                self.history[symbol] = df
        logger.info("History loaded.")

    def save_candle(self, symbol, timestamp, candle):
        # Prepare DataFrame for calculation
        new_row = pd.DataFrame([{
            'timestamp': timestamp,
            'open': candle['open'],
            'high': candle['high'],
            'low': candle['low'],
            'close': candle['close']
        }]).set_index('timestamp')
        
        # Access history
        hist = self.history.get(symbol, pd.DataFrame())
        full_df = pd.concat([hist, new_row])
        
        # Truncate history to keep memory usage low (e.g. 500 rows)
        if len(full_df) > 500:
            full_df = full_df.iloc[-500:]
        
        self.history[symbol] = full_df # Update memory
        
        # Calculate Indicators on full_df
        # 1. MA
        full_df['ma_20'] = Indicators.calculate_sma(full_df['close'], 20)
        full_df['ma_200'] = Indicators.calculate_sma(full_df['close'], 200)
        full_df['fast_ma_39'] = Indicators.calculate_sma(full_df['close'], 39)
        full_df['fast_ma_69'] = Indicators.calculate_sma(full_df['close'], 69)
        
        # 2. ATR & ADX
        full_df['atr'] = Indicators.calculate_atr(full_df, 14)
        full_df['adx'] = Indicators.calculate_adx(full_df, 14)
        
        # 3. Supertrend
        st, st_dir = Indicators.calculate_supertrend(full_df, 10, 3)
        full_df['supertrend'] = st
        full_df['supertrend_direction'] = st_dir

        # 4. Renko
        renko_signals = Indicators.calculate_renko(full_df, 10)
        full_df['super_power'] = renko_signals

        # Extract latest calculated values
        latest = full_df.iloc[-1]
        
        # Insert into DB
        with self.engine.begin() as conn: # Transaction
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
                'token': [k for k,v in TOKENS.items() if v == symbol][0],
                'open': float(latest['open']),
                'high': float(latest['high']),
                'low': float(latest['low']),
                'close': float(latest['close']),
                'ma_20': float(latest['ma_20']) if not pd.isna(latest['ma_20']) else None,
                'ma_200': float(latest['ma_200']) if not pd.isna(latest['ma_200']) else None,
                'ma_39': float(latest['fast_ma_39']) if not pd.isna(latest['fast_ma_39']) else None,
                'ma_69': float(latest['fast_ma_69']) if not pd.isna(latest['fast_ma_69']) else None,
                'atr': float(latest['atr']) if not pd.isna(latest['atr']) else None,
                'adx': float(latest['adx']) if not pd.isna(latest['adx']) else None,
                'st': float(latest['supertrend']) if not pd.isna(latest['supertrend']) else None,
                'st_dir': int(latest['supertrend_direction']) if not pd.isna(latest['supertrend_direction']) else None,
                'renko': str(latest['super_power']) if latest['super_power'] else None
            })
            
        logger.info(f"Saved candle for {symbol} at {timestamp}")

    def start(self):
        kws = KiteTicker(self.credentials['api_key'], self.credentials['access_token'])
        
        def on_ticks(ws, ticks):
            now = datetime.now()
            minute_changed = False
            
            if now.minute != self.current_minute:
                # Candle closed
                minute_changed = True
                close_time = now.replace(second=0, microsecond=0) # current minute is new candle, so prev candle closed at now?
                # Actually, if minute changed from 01 to 02, the candle for 01 is done.
                # The timestamp should be the minute start or end? Usually start.
                # So if we detect change at 10:02:00, the candle for 10:01:00 is complete.
                candle_timestamp = now.replace(second=0, microsecond=0) - timedelta(minutes=1)
                
                for token, symbol in TOKENS.items():
                    c = self.candles[symbol]
                    if c['ticks'] > 0:
                        self.save_candle(symbol, candle_timestamp, c)
                        # Reset for new candle
                        self.candles[symbol] = {'open': 0, 'high': 0, 'low': float('inf'), 'close': 0, 'ticks': 0}
                
                self.current_minute = now.minute

            if minute_changed:
                 # Process the tick for the NEW candle
                 pass

            for tick in ticks:
                token = tick['instrument_token']
                if token in TOKENS:
                    symbol = TOKENS[token]
                    price = tick['last_price']
                    c = self.candles[symbol]
                    
                    if c['ticks'] == 0:
                        c['open'] = price
                        c['high'] = price
                        c['low'] = price
                        c['close'] = price
                    else:
                        c['high'] = max(c['high'], price)
                        c['low'] = min(c['low'], price)
                        c['close'] = price
                    c['ticks'] += 1

        def on_connect(ws, response):
            ws.subscribe(list(TOKENS.keys()))
            ws.set_mode(ws.MODE_FULL, list(TOKENS.keys()))
            logger.info("Connected to Kite Ticker")

        def on_close(ws, code, reason):
            logger.error(f"Connection closed: {code} - {reason}")
            ws.stop()

        kws.on_ticks = on_ticks
        kws.on_connect = on_connect
        kws.on_close = on_close

        kws.connect()

if __name__ == "__main__":
    streamer = IndexStreamer()
    streamer.start()
