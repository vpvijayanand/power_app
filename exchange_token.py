from app import create_app, db
from app.models import User
from kiteconnect import KiteConnect

app = create_app()
app.app_context().push()

user = User.query.filter_by(name='Vijayanand').first()

if not user:
    print("User not found!")
    exit()

if not user.api_key or not user.api_secret or not user.request_token:
    print("Missing API credentials or request token.")
    exit()

print(f"Exchanging Request Token: {user.request_token}")

kite = KiteConnect(api_key=user.api_key)

try:
    data = kite.generate_session(user.request_token, api_secret=user.api_secret)
    user.access_token = data["access_token"]
    db.session.commit()
    print(f"Success! Access Token set: {user.access_token}")
except Exception as e:
    print(f"Error exchanging token: {e}")
