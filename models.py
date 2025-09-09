from flask_sqlalchemy import SQLAlchemy

db = SQLAlchemy()

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

class User(db.Model):
    __tablename__ = 'users'
    user_num = db.Column(db.Integer, primary_key=True, autoincrement=True)
    user_id = db.Column(db.String(32), unique=True, nullable=False)
    user_name = db.Column(db.String(32), nullable=False)
    user_nickname = db.Column(db.String(32), nullable=False)
    password = db.Column(db.String(32), nullable=False)
    email = db.Column(db.String(32), unique=True, nullable=False)
    date = db.Column(db.TIMESTAMP, nullable=True)
