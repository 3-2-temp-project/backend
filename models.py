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
    price = db.Column(db.Integer, nullable=True)  # 대표 가격 (평균)
    score = db.Column(db.Float, nullable=True)
    # 가격 통계 필드 추가
    price_min = db.Column(db.Integer, nullable=True)  # 최저 가격
    price_max = db.Column(db.Integer, nullable=True)  # 최고 가격
    price_avg = db.Column(db.Integer, nullable=True)  # 평균 가격
    price_count = db.Column(db.Integer, default=0)    # 가격 데이터 수집 횟수
    people = db.Column(db.Integer, nullable=True)      # 평균 인원수(명)

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

# 리뷰 테이블
class Review(db.Model):
    __tablename__ = 'reviews'
    id         = db.Column(db.Integer, primary_key=True, autoincrement=True)
    res_id     = db.Column(db.Integer, db.ForeignKey('restaurant_info.res_id', ondelete="CASCADE"), nullable=False)
    user_id    = db.Column(db.Integer, db.ForeignKey('users.user_num',       ondelete="CASCADE"), nullable=False)
    user_nickname = db.Column(db.String(32), nullable=False)  # ✅ 여기 추가
    content    = db.Column(db.Text, nullable=False)
    rating     = db.Column(db.Integer, nullable=False)  # 1~5
    photo_url  = db.Column(db.String(255))
    created_at = db.Column(db.DateTime, server_default=db.func.now())

    user = db.relationship("User", backref="reviews")

    

# 제보 테이블
class Suggestion(db.Model):
    __tablename__ = 'suggestions'
    id          = db.Column(db.Integer, primary_key=True, autoincrement=True)
    res_name    = db.Column(db.String(64),  nullable=False)  
    address     = db.Column(db.String(255))
    user_id     = db.Column(db.Integer, db.ForeignKey('users.user_num', ondelete="CASCADE"), nullable=False)
    photo_url   = db.Column(db.String(255))
    description = db.Column(db.Text)
    created_at  = db.Column(db.DateTime, server_default=db.func.now())

# 뱃지 테이블
class Badge(db.Model):
    __tablename__ = 'badges'
    id          = db.Column(db.Integer, primary_key=True, autoincrement=True)
    res_id      = db.Column(db.Integer, db.ForeignKey('restaurant_info.res_id', ondelete="CASCADE"), nullable=False)
    badge_type  = db.Column(db.String(32),  nullable=False)
    description = db.Column(db.String(128))
    issued_at   = db.Column(db.DateTime, server_default=db.func.now())

# 방문 내역 테이블
class Visit(db.Model):
    __tablename__ = 'visits'
    vi_id      = db.Column(db.Integer, primary_key=True, autoincrement=True)
    res_id     = db.Column(db.Integer, db.ForeignKey('restaurant_info.res_id', ondelete="CASCADE"), nullable=False)
    visit_date = db.Column(db.Date, nullable=False)
