from flask import Blueprint

auth_bp = Blueprint('auth', __name__, url_prefix='/auth')

# Import routes after blueprint is defined to avoid circular imports
from app.auth import routes
