from flask import Blueprint, render_template

main_bp = Blueprint('main', __name__)


@main_bp.route('/')
def home():
    """Homepage with animated glowing text"""
    return render_template('home.html')
