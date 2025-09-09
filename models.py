# models.py

import datetime
from flask_sqlalchemy import SQLAlchemy
from flask_login import UserMixin

db = SQLAlchemy()

# 음식점 정보 테이블
class RestaurantInfo(db.Model):
    __tablename__ = 'restaurant_info'
    res_id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    res_name = db.Column(db.String(64), nullable=False)
    address = db.Column(db.String(255), nullable=False)
    lat = db.Column(db.Float, nullable=False)
    lng = db.Column(db.Float, nullable=False)
    res_phone = db.Column(db.String(32), nullable=True)
    category = db.Column(db.String(32), nullable=True)
    price = db.Column(db.Integer, nullable=True)
    score = db.Column(db.Float, nullable=True)

# 사용자 테이블
class User(db.Model, UserMixin):
    __tablename__ = 'users'
    user_num = db.Column(db.Integer, primary_key=True, autoincrement=True)
    user_id = db.Column(db.String(32), unique=True, nullable=False)
    user_name = db.Column(db.String(32), nullable=False)
    user_nickname = db.Column(db.String(32), nullable=False)
    password = db.Column(db.Text, nullable=False)
    email = db.Column(db.String(128), unique=True, nullable=False)
    date = db.Column(db.TIMESTAMP, nullable=True)
    def get_id(self):
        return str(self.user_num)  

# 이메일 인증 코드 테이블
class EmailVerification(db.Model):
    __tablename__ = 'email_verifications'
    id = db.Column(db.Integer, primary_key=True)
    email = db.Column(db.String(128), nullable=False, unique=True)
    code = db.Column(db.String(6), nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.datetime.utcnow)
