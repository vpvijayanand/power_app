from kiteconnect import KiteConnect
from datetime import datetime
from app.models import db
from flask import current_app


class KiteService:
    """Service for Kite API integration"""
    
    @staticmethod
    def create_kite_instance(user):
        """Create a KiteConnect instance for a user"""
        if not user.api_key:
            return None
        
        kite = KiteConnect(api_key=user.api_key)
        
        # Set access token if request token exists
        if user.request_token and user.api_secret:
            try:
                data = kite.generate_session(user.request_token, api_secret=user.api_secret)
                return kite
            except Exception as e:
                current_app.logger.error(f"Error creating Kite session for user {user.id}: {str(e)}")
                return None
        
        return kite
    
    @staticmethod
    def generate_login_url(user):
        """Generate Kite login URL with user's callback token"""
        if not user.api_key or not user.kite_callback_token:
            return None
        
        kite = KiteConnect(api_key=user.api_key)
        callback_url = f"{current_app.config['KITE_CALLBACK_BASE_URL']}/kite/callback?token={user.kite_callback_token}"
        
        return kite.login_url()
    
    @staticmethod
    def set_access_token(user, request_token):
        """Exchange request token for access token and store it"""
        try:
            if not user.api_key or not user.api_secret:
                return False
            
            kite = KiteConnect(api_key=user.api_key)
            
            # Generate session
            data = kite.generate_session(request_token, api_secret=user.api_secret)
            access_token = data["access_token"]
            
            # Update user with tokens
            user.request_token = request_token
            user.access_token = access_token
            db.session.commit()
            
            return True
            
        except Exception as e:
            print(f"Error setting access token: {e}")
            return False
    
    @staticmethod
    def get_account_balance(user):
        """Fetch and update user's account balance from Kite"""
        kite = KiteService.create_kite_instance(user)
        
        if not kite:
            return None
        
        try:
            # Get margins (account balance)
            margins = kite.margins()
            
            if 'equity' in margins:
                balance = margins['equity'].get('available', {}).get('live_balance', 0)
                
                # Update user's balance
                user.kite_account_balance = balance
                user.last_balance_update = datetime.utcnow()
                db.session.commit()
                
                return balance
        except Exception as e:
            current_app.logger.error(f"Error fetching balance for user {user.id}: {str(e)}")
            return None
    
    @staticmethod
    def update_credentials(user, api_key, api_secret):
        """Update user's Kite API credentials"""
        user.api_key = api_key
        user.api_secret = api_secret
        db.session.commit()
        return True
