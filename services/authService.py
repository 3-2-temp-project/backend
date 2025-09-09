from models import db, User, EmailVerification
from werkzeug.security import generate_password_hash
import datetime

def verify_email_code(email, input_code):
    record = EmailVerification.query.filter_by(email=email, code=input_code).first()
    if not record:
        return False, "코드가 틀렸거나 존재하지 않습니다."

    if (datetime.datetime.utcnow() - record.created_at).total_seconds() > 180:
        db.session.delete(record)
        db.session.commit()
        return False, "코드가 만료되었습니다."

    db.session.delete(record)
    db.session.commit()
    return True, "인증 성공"

def register_user(data):
    if User.query.filter_by(email=data["email"]).first():
        return {"message": "이미 등록된 이메일입니다."}, 409

    user = User(
        user_id=data["user_id"],
        user_name=data["user_name"],
        user_nickname=data["user_nickname"],
        password=generate_password_hash(data["password"]),
        email=data["email"]
    )
    db.session.add(user)
    db.session.commit()

    return user, 201 

def get_user_by_email(email):
    return User.query.filter_by(email=email).first()