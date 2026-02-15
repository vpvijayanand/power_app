# Algo Trading Web Application

A scalable, multi-user Algo Trading platform built with Flask, PostgreSQL, and Zerodha Kite API integration. Features a premium black-themed UI with real-time market data capabilities.

## Features

- 🔐 **Secure Authentication** - Role-based access control (Admin/Client)
- 👥 **Multi-User Support** - Isolated user accounts with individual Kite credentials
- 📊 **Kite Integration** - Seamless Zerodha Kite API integration with unique callback tokens
- 💼 **Account Management** - Track balance, growth, and trading performance
- 🎨 **Premium UI** - Black-themed interface with illuminated green/red/blue accents
- 🔒 **Security** - Password hashing, CSRF protection, secure sessions

## Tech Stack

- **Backend**: Flask (Python)
- **Database**: PostgreSQL
- **Trading API**: Zerodha Kite Connect
- **ORM**: SQLAlchemy
- **Forms**: Flask-WTF
- **Migrations**: Flask-Migrate

## Project Structure

```
power_app/
├── app/
│   ├── __init__.py          # Application factory
│   ├── models.py            # Database models
│   ├── config.py            # Configuration
│   ├── routes.py            # Main routes
│   ├── auth/                # Authentication blueprint
│   ├── dashboard/           # Client dashboard blueprint
│   ├── admin/               # Admin dashboard blueprint
│   ├── kite/                # Kite API integration blueprint
│   ├── services/            # Business logic services
│   ├── static/              # CSS, JS, images
│   └── templates/           # Jinja2 templates
├── migrations/              # Database migrations
├── .env                     # Environment variables
├── requirements.txt         # Python dependencies
├── run.py                   # Application entry point
└── init_db.py              # Database initialization
```

## Setup Instructions

### 1. Prerequisites

- Python 3.8+
- PostgreSQL 12+
- Zerodha Kite API credentials

### 2. Database Setup

Create a PostgreSQL database:

```sql
CREATE DATABASE power_app;
```

### 3. Environment Configuration

Copy `.env.example` to `.env` and update the values:

```bash
cp .env.example .env
```

Edit `.env`:
```env
SECRET_KEY=your-secret-key-here
DB_HOST=localhost
DB_PORT=5432
DB_NAME=power_app
DB_USER=postgres
DB_PASSWORD=postgres
KITE_CALLBACK_BASE_URL=http://localhost:5000
```

### 4. Install Dependencies

```bash
pip install -r requirements.txt
```

### 5. Initialize Database

```bash
# Initialize migrations
flask db init

# Create migration
flask db migrate -m "Initial migration"

# Apply migration
flask db upgrade

# Create admin user
python init_db.py
```

**Default Admin Credentials:**
- Email: `admin@algotrade.com`
- Password: `admin123`

⚠️ **Change the password after first login!**

### 6. Run the Application

```bash
python run.py
```

The application will be available at `http://localhost:5000`

## Usage

### Admin Functions

1. **Login** with admin credentials
2. **Add Users** - Create new client or admin users
3. **Manage Users** - Activate/deactivate user accounts
4. **View Statistics** - Monitor system-wide metrics

### Client Functions

1. **Login** with client credentials
2. **Set Kite Credentials** - Add API key and secret
3. **Connect Kite** - Authenticate with Zerodha
4. **View Dashboard** - Track balance, growth, and trades
5. **Refresh Balance** - Update account balance from Kite

## Kite API Integration

### Callback Flow

1. User sets API credentials in settings
2. User clicks "Connect Kite"
3. System generates unique callback URL with user token
4. User authenticates on Kite
5. Kite redirects to callback with request token
6. System identifies user and stores access token
7. Account balance is fetched and updated

### Callback URL Format

```
http://yourdomain.com/kite/callback?token={user_kite_callback_token}
```

## Security Features

- ✅ Password hashing with Werkzeug
- ✅ CSRF protection on all forms
- ✅ Role-based route protection
- ✅ Secure session management
- ✅ Environment variable secrets
- ✅ SQL injection prevention (SQLAlchemy ORM)

## UI/UX Design

### Color Palette

- **Background**: Deep Black (#000000)
- **Accent Green**: Illuminated Green (#00FF88) - Profits/Positive
- **Accent Red**: Illuminated Red (#FF0055) - Losses/Negative
- **Accent Blue**: Illuminated Blue (#00AAFF) - Headings/Highlights
- **Text**: White (#FFFFFF)

### Typography

- **Font**: Inter, Roboto
- **Style**: Clean, crisp, professional trading terminal
- **Effects**: Glow effects, smooth animations

## Future Enhancements

- 📡 WebSocket integration for real-time data
- 📈 Advanced charting with TradingView
- 🤖 Strategy engine module
- 📊 Backtesting capabilities
- 📱 Mobile responsive improvements
- 🔔 Real-time notifications

## Production Deployment

For production deployment:

1. Use a production WSGI server (Gunicorn/uWSGI)
2. Set up Nginx as reverse proxy
3. Configure SSL certificates
4. Set `FLASK_ENV=production`
5. Use strong `SECRET_KEY`
6. Enable `SESSION_COOKIE_SECURE`
7. Set up logging and monitoring
8. Configure database backups

## License

Proprietary - All rights reserved

## Support

For issues or questions, contact the development team.
