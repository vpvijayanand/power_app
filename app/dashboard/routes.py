from flask import render_template
from app.auth.decorators import login_required, client_required, get_current_user
from app.dashboard import dashboard_bp


@dashboard_bp.route('/')
@login_required
@client_required
def client_dashboard():
    """Client dashboard"""
    user = get_current_user()
    
    # Calculate growth color
    growth_color = 'green' if user.account_growth_percentage > 0 else 'red' if user.account_growth_percentage < 0 else 'white'
    
    return render_template('dashboard/client.html', 
                         user=user, 
                         growth_color=growth_color)
