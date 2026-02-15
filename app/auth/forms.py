from flask_wtf import FlaskForm
from wtforms import StringField, PasswordField, SelectField, IntegerField
from wtforms.validators import DataRequired, Email, Length, EqualTo, ValidationError
from app.models import User


class LoginForm(FlaskForm):
    """Login form"""
    email = StringField('Email', validators=[
        DataRequired(),
        Email()
    ])
    password = PasswordField('Password', validators=[
        DataRequired()
    ])


class RegisterForm(FlaskForm):
    """User registration form (admin only)"""
    name = StringField('Full Name', validators=[
        DataRequired(),
        Length(min=2, max=100)
    ])
    email = StringField('Email', validators=[
        DataRequired(),
        Email(),
        Length(max=150)
    ])
    mobile = StringField('Mobile', validators=[
        Length(max=15)
    ])
    password = PasswordField('Password', validators=[
        DataRequired(),
        Length(min=6)
    ])
    confirm_password = PasswordField('Confirm Password', validators=[
        DataRequired(),
        EqualTo('password', message='Passwords must match')
    ])
    user_type = SelectField('User Type', choices=[
        ('Client', 'Client'),
        ('Admin', 'Admin')
    ], validators=[DataRequired()])
    trade_mode = SelectField('Trade Mode', choices=[
        ('Paper', 'Paper Trading'),
        ('Live', 'Live Trading')
    ], validators=[DataRequired()])
    lot_size = IntegerField('Lot Size', validators=[
        DataRequired()
    ], default=1)
    
    def validate_email(self, field):
        """Check if email already exists"""
        if User.query.filter_by(email=field.data).first():
            raise ValidationError('Email already registered.')
