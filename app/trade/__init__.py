from flask import Blueprint

trade_bp = Blueprint('trade', __name__, url_prefix='/admin/trade')

from app.trade import routes
