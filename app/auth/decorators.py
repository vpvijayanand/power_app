from functools import wraps
from flask import session, redirect, url_for, flash
from app.models import User, db


def login_required(f):
    """Decorator to require login for a route"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user_id' not in session:
            flash('Please log in to access this page.', 'warning')
            return redirect(url_for('auth.login'))
        return f(*args, **kwargs)
    return decorated_function


def admin_required(f):
    """Decorator to require admin role for a route"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user_id' not in session:
            flash('Please log in to access this page.', 'warning')
            return redirect(url_for('auth.login'))
        
        user = db.session.get(User, session['user_id'])
        if not user or not user.is_admin():
            flash('Admin access required.', 'danger')
            return redirect(url_for('main.home'))
        
        return f(*args, **kwargs)
    return decorated_function


def client_required(f):
    """Decorator to require client role for a route"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user_id' not in session:
            flash('Please log in to access this page.', 'warning')
            return redirect(url_for('auth.login'))
        
        user = db.session.get(User, session['user_id'])
        if not user or not user.is_client():
            flash('Client access required.', 'danger')
            return redirect(url_for('main.home'))
        
        return f(*args, **kwargs)
    return decorated_function


def get_current_user():
    """Get the currently logged-in user"""
    if 'user_id' in session:
        return db.session.get(User, session['user_id'])
    return None
