from flask import Blueprint

kite_bp = Blueprint('kite', __name__, url_prefix='/kite')

from app.kite import routes
