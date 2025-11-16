import random
import datetime

from flask import current_app
from flask_mail import Message, Mail
from email.utils import make_msgid   # ✅ Message-ID 생성용
# from email.header import Header    # <- 일단 테스트에선 안 씀 (한글 제목 제거)
from models import db, EmailVerification

mail = Mail()


def generate_code():
    """4자리 랜덤 숫자 코드 생성"""
    return str(random.randint(1000, 9999))


def send_verification_code(email: str) -> None:
    """이메일로 인증 코드 발송 (테스트용: recipients 고정 + 헤더 디버깅)"""

    # 1) 인증 코드 생성 및 DB 저장
    code = generate_code()

    existing = EmailVerification.query.filter_by(email=email).first()
    if existing:
        db.session.delete(existing)
        db.session.commit()

    new_code = EmailVerification(email=email, code=code)
    db.session.add(new_code)
    db.session.commit()

    # 2) 메일 제목 → 일단 ASCII만 사용 (테스트용)
    subject = "Email verification code"

    # 3) 보내는 사람(sender) 설정
    sender_email = (
        current_app.config.get("MAIL_USERNAME")
        or current_app.config.get("MAIL_DEFAULT_SENDER")
    )

    # 4) Message 생성 (테스트: recipients 고정)
    msg = Message(
        subject=subject,
        sender=sender_email,
        # recipients=[email],  # 나중에 되면 이걸로 복귀
        recipients=[email],
        body=f"Your verification code is: {code}. This code is valid for 3 minutes.",
    )

    # 5) Message-ID를 ASCII 도메인으로 강제 설정
    msg.msgId = make_msgid(domain="project2025.com")  # ✅ 임의의 ASCII 도메인

    # 6) 헤더 디버그 출력
    print("==== DEBUG HEADERS START ====")
    raw = msg._message()
    for k, v in raw.items():
        print(f"{k}: {repr(v)}")
    print("==== DEBUG HEADERS END ====")

    # 7) 메일 전송
    mail.send(msg)
