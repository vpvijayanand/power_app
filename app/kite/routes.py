from flask import redirect, url_for, flash, request, render_template, current_app
from app.auth.decorators import login_required, get_current_user
from app.services.kite_service import KiteService
from app.models import User, db
from app.kite import kite_bp


@kite_bp.route('/connect')
@login_required
def connect():
    """Redirect user to Kite login"""
    user = get_current_user()
    
    if not user.api_key:
        flash('Please set your Kite API credentials first.', 'warning')
        return redirect(url_for('kite.credentials'))
    
    # Generate login URL
    login_url = KiteService.generate_login_url(user)
    
    if not login_url:
        flash('Error generating Kite login URL.', 'danger')
        return redirect(url_for('dashboard.client_dashboard'))
    
    return redirect(login_url)


@kite_bp.route('/callback')
def callback():
    """Handle Kite callback with request token"""
    request_token = request.args.get('request_token')
    user_token = request.args.get('token')
    status = request.args.get('status')
    
    # Check for errors
    if status == 'error' or not request_token or not user_token:
        flash('Kite authentication failed.', 'danger')
        return redirect(url_for('main.home'))
    
    # Find user by callback token
    user = User.query.filter_by(kite_callback_token=user_token).first()
    
    if not user:
        flash('Invalid callback token.', 'danger')
        return redirect(url_for('main.home'))
    
    # Set access token
    if KiteService.set_access_token(user, request_token):
        # Fetch and update balance
        balance = KiteService.get_account_balance(user)
        
        if balance is not None:
            flash(f'Kite connected successfully! Account balance: ₹{balance:,.2f}', 'success')
        else:
            flash('Kite connected, but could not fetch balance.', 'warning')
    else:
        flash('Error connecting to Kite.', 'danger')
    
    # Redirect based on user type
    if user.is_admin():
        return redirect(url_for('admin.dashboard'))
    else:
        return redirect(url_for('dashboard.client_dashboard'))


@kite_bp.route('/credentials', methods=['GET', 'POST'])
@login_required
def credentials():
    """Update Kite API credentials"""
    user = get_current_user()
    
    if request.method == 'POST':
        api_key = request.form.get('api_key')
        api_secret = request.form.get('api_secret')
        
        if api_key and api_secret:
            KiteService.update_credentials(user, api_key, api_secret)
            flash('Kite credentials updated successfully!', 'success')
            
            if user.is_admin():
                return redirect(url_for('admin.dashboard'))
            else:
                return redirect(url_for('dashboard.client_dashboard'))
        else:
            flash('Please provide both API key and secret.', 'danger')
    
    return render_template('kite/credentials.html', user=user, current_user=user,
                           callback_base_url=current_app.config['KITE_CALLBACK_BASE_URL'])


@kite_bp.route('/refresh-balance')
@login_required
def refresh_balance():
    """Refresh user's account balance"""
    user = get_current_user()
    
    balance = KiteService.get_account_balance(user)
    
    if balance is not None:
        flash(f'Balance updated: ₹{balance:,.2f}', 'success')
    else:
        flash('Could not fetch balance. Please check your Kite connection.', 'danger')
    
    # Redirect based on user type
    if user.is_admin():
        return redirect(url_for('admin.dashboard'))
    else:
        return redirect(url_for('dashboard.client_dashboard'))
