import os
from flask import Flask
from flask_migrate import Migrate
from app.config import config
from app.models import db


migrate = Migrate()


def create_app(config_name=None):
    """Application factory pattern"""
    
    if config_name is None:
        config_name = os.environ.get('FLASK_ENV', 'development')
    
    app = Flask(__name__)
    app.config.from_object(config[config_name])
    
    # Initialize extensions
    db.init_app(app)
    migrate.init_app(app, db)
    
    # Register blueprints
    from app.auth import auth_bp
    from app.dashboard import dashboard_bp
    from app.admin import admin_bp
    from app.kite import kite_bp
    
    app.register_blueprint(auth_bp)
    app.register_blueprint(dashboard_bp)
    app.register_blueprint(admin_bp)
    app.register_blueprint(kite_bp)
    
    # Register main routes
    from app import routes
    app.register_blueprint(routes.main_bp)
    
    # Error handlers
    @app.errorhandler(404)
    def not_found_error(error):
        from flask import render_template
        return render_template('errors/404.html'), 404
    
    @app.errorhandler(500)
    def internal_error(error):
        from flask import render_template
        db.session.rollback()
        return render_template('errors/500.html'), 500
    
    return app
