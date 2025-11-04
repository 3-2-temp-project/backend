import random
import datetime
from flask_mail import Message, Mail
from models import db, EmailVerification

mail = Mail()

def generate_code():
    return str(random.randint(1000, 9999))

def send_verification_code(email):
    code = generate_code()

    # 기존 기록 있으면 삭제
    existing = EmailVerification.query.filter_by(email=email).first()
    if existing:
        db.session.delete(existing)
        db.session.commit()

    new_code = EmailVerification(email=email, code=code)
    db.session.add(new_code)
    db.session.commit()

    # 이메일 전송
    msg = Message(
        subject="이메일 인증 코드",
        recipients=[email],
        html=f"""
        <h3>이메일 인증 요청</h3>
        <p>아래 인증코드를 입력하세요:</p>
        <h2>{code}</h2>
        <p>이 코드는 3분간 유효합니다.</p>
        """
    )
    mail.send(msg)
