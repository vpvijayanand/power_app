from datetime import datetime
from flask_sqlalchemy import SQLAlchemy
from werkzeug.security import generate_password_hash, check_password_hash

db = SQLAlchemy()


class Instrument(db.Model):
    """Instrument model for storing Kite instruments data"""
    __tablename__ = 'instruments'
    
    id = db.Column(db.Integer, primary_key=True)
    instrument_token = db.Column(db.BigInteger, nullable=False, index=True)
    exchange_token = db.Column(db.BigInteger)
    tradingsymbol = db.Column(db.String(50), nullable=False, index=True)
    name = db.Column(db.String(200))
    last_price = db.Column(db.Numeric(10, 2))
    expiry = db.Column(db.Date)
    strike = db.Column(db.Numeric(10, 2))
    tick_size = db.Column(db.Numeric(10, 2))
    lot_size = db.Column(db.Integer)
    instrument_type = db.Column(db.String(20))
    segment = db.Column(db.String(20))
    exchange = db.Column(db.String(20), index=True)
    fetch_date = db.Column(db.Date, nullable=False, index=True)
    expiry_weekday = db.Column(db.String(20))
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    def __repr__(self):
        return f'<Instrument {self.tradingsymbol}>'


class OptionChainData(db.Model):
    __tablename__ = 'option_chain_data'

    id = db.Column(db.Integer, primary_key=True)
    underlying = db.Column(db.String(20), nullable=False)
    strike_price = db.Column(db.Float, nullable=False)
    expiry_date = db.Column(db.Date, nullable=False)
    
    # CE Data
    ce_oi = db.Column(db.Integer)
    ce_oi_change = db.Column(db.Integer)
    ce_volume = db.Column(db.Integer)
    ce_ltp = db.Column(db.Float)
    ce_change = db.Column(db.Float)
    ce_change_percent = db.Column(db.Float)
    ce_iv = db.Column(db.Float)
    
    # PE Data
    pe_oi = db.Column(db.Integer)
    pe_oi_change = db.Column(db.Integer)
    pe_volume = db.Column(db.Integer)
    pe_ltp = db.Column(db.Float)
    pe_change = db.Column(db.Float)
    pe_change_percent = db.Column(db.Float)
    pe_iv = db.Column(db.Float)
    
    timestamp = db.Column(db.DateTime, nullable=False)
    is_current_expiry = db.Column(db.Boolean)
    
    # Metadata
    ce_strike_symbol = db.Column(db.String(100))
    ce_instrument_token = db.Column(db.String(50))
    pe_strike_symbol = db.Column(db.String(100))
    pe_instrument_token = db.Column(db.String(50))

    def __repr__(self):
        return f'<OptionChain {self.underlying} {self.expiry_date} {self.strike_price}>'


class User(db.Model):
    """User model matching the PostgreSQL schema"""
    
    __tablename__ = 'users'
    
    # Primary key
    id = db.Column(db.Integer, primary_key=True)
    
    # User information
    name = db.Column(db.String(100), nullable=False)
    email = db.Column(db.String(150), unique=True, nullable=False, index=True)
    mobile = db.Column(db.String(15))
    password_hash = db.Column(db.String(255), nullable=False)
    
    # Kite API credentials
    api_key = db.Column(db.String(100))
    api_secret = db.Column(db.String(100))
    request_token = db.Column(db.Text)
    access_token = db.Column(db.Text)
    kite_callback_token = db.Column(db.String(128), unique=True, index=True)
    
    # User configuration
    user_type = db.Column(db.String(20), nullable=False)  # 'Admin' or 'Client'
    trade_mode = db.Column(db.String(20), nullable=False, default='Paper')  # 'Live' or 'Paper'
    lot_size = db.Column(db.Integer, nullable=False, default=1)
    
    # Account tracking
    account_growth_percentage = db.Column(db.Numeric(15, 2), nullable=False, default=0.00)
    kite_account_balance = db.Column(db.Numeric(15, 2), nullable=False, default=0.00)
    
    # Status
    is_active = db.Column(db.Boolean, nullable=False, default=True)
    
    # Timestamps
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, onupdate=datetime.utcnow)
    last_balance_update = db.Column(db.DateTime)
    
    def set_password(self, password):
        """Hash and set the user's password"""
        self.password_hash = generate_password_hash(password)
    
    def check_password(self, password):
        """Verify the user's password"""
        return check_password_hash(self.password_hash, password)
    
    def is_admin(self):
        """Check if user is an admin"""
        return self.user_type == 'Admin'
    
    def is_client(self):
        """Check if user is a client"""
        return self.user_type == 'Client'
    
    def __repr__(self):
        return f'<User {self.email}>'


class IndexData(db.Model):
    __tablename__ = 'index_data'
    id = db.Column(db.Integer, primary_key=True)
    timestamp = db.Column(db.DateTime, nullable=False, index=True)
    symbol = db.Column(db.String(20), nullable=False, index=True) # NIFTY, BANKNIFTY
    instrument_token = db.Column(db.Integer)
    
    # OHLC
    open = db.Column(db.Float)
    high = db.Column(db.Float)
    low = db.Column(db.Float)
    close = db.Column(db.Float)
    
    # Indicators
    ma_20 = db.Column(db.Float)
    ma_200 = db.Column(db.Float)
    fast_ma_39 = db.Column(db.Float)
    fast_ma_69 = db.Column(db.Float)
    
    atr = db.Column(db.Float)
    adx = db.Column(db.Float)
    
    # Supertrend (Default 10, 3)
    supertrend = db.Column(db.Float)
    supertrend_direction = db.Column(db.Integer) # 1 for Up (Green), -1 for Down (Red)
    
    # Renko (Super Power)
    super_power = db.Column(db.String(20)) # 'BUY', 'SELL'
    super_power_brick = db.Column(db.Float, default=10.0)

    def __repr__(self):
        return f'<IndexData {self.symbol} {self.timestamp}>'
