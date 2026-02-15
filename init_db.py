"""
Database initialization script
Creates the initial admin user
"""
from app import create_app, db
from app.models import User
import secrets

def init_db():
    """Initialize database and create admin user"""
    app = create_app()
    
    with app.app_context():
        # Create all tables
        db.create_all()
        
        # Check if admin exists
        admin = User.query.filter_by(email='admin@algotrade.com').first()
        
        if not admin:
            # Create admin user
            admin = User(
                name='Admin',
                email='admin@algotrade.com',
                user_type='Admin',
                trade_mode='Live',
                lot_size=1,
                kite_callback_token=secrets.token_urlsafe(32),
                account_growth_percentage=0.00,
                kite_account_balance=0.00,
                is_active=True
            )
            admin.set_password('admin123')
            
            db.session.add(admin)
            db.session.commit()
            
            print("✅ Database initialized successfully!")
            print(f"✅ Admin user created:")
            print(f"   Email: admin@algotrade.com")
            print(f"   Password: admin123")
            print(f"   ⚠️  Please change the password after first login!")
        else:
            print("ℹ️  Admin user already exists")

if __name__ == '__main__':
    init_db()
