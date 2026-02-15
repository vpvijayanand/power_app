from flask import render_template, redirect, url_for, flash, session, request
from app.models import User, db
from app.auth.forms import LoginForm, RegisterForm
from app.auth.decorators import login_required, admin_required
from app.auth import auth_bp
import secrets


@auth_bp.route('/login', methods=['GET', 'POST'])
def login():
    """User login"""
    # Redirect if already logged in
    if 'user_id' in session:
        user = db.session.get(User, session['user_id'])
        if user:
            if user.is_admin():
                return redirect(url_for('admin.dashboard'))
            else:
                return redirect(url_for('dashboard.client_dashboard'))
    
    form = LoginForm()
    
    if form.validate_on_submit():
        user = User.query.filter_by(email=form.email.data).first()
        
        if user and user.check_password(form.password.data):
            if not user.is_active:
                flash('Your account has been deactivated. Please contact admin.', 'danger')
                return redirect(url_for('auth.login'))
            
            # Set session
            session['user_id'] = user.id
            session['user_name'] = user.name
            session['user_type'] = user.user_type
            session.permanent = True
            
            flash(f'Welcome back, {user.name}!', 'success')
            
            # Redirect based on role
            if user.is_admin():
                return redirect(url_for('admin.dashboard'))
            else:
                return redirect(url_for('dashboard.client_dashboard'))
        else:
            flash('Invalid email or password.', 'danger')
    
    return render_template('auth/login.html', form=form)


@auth_bp.route('/logout')
@login_required
def logout():
    """User logout"""
    user_name = session.get('user_name', 'User')
    session.clear()
    flash(f'Goodbye, {user_name}!', 'info')
    return redirect(url_for('main.home'))


@auth_bp.route('/register', methods=['GET', 'POST'])
@admin_required
def register():
    """User registration (admin only)"""
    form = RegisterForm()
    
    if form.validate_on_submit():
        # Generate unique callback token
        kite_callback_token = secrets.token_urlsafe(32)
        
        # Create new user
        user = User(
            name=form.name.data,
            email=form.email.data,
            mobile=form.mobile.data if form.mobile.data else None,
            user_type=form.user_type.data,
            trade_mode=form.trade_mode.data,
            lot_size=form.lot_size.data,
            kite_callback_token=kite_callback_token,
            account_growth_percentage=0.00,
            kite_account_balance=0.00,
            is_active=True
        )
        user.set_password(form.password.data)
        
        db.session.add(user)
        db.session.commit()
        
        flash(f'User {user.name} created successfully!', 'success')
        return redirect(url_for('admin.dashboard'))
    
    return render_template('auth/register.html', form=form)
