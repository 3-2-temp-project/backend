from flask import Blueprint, request, jsonify
from flask_login import login_user
from services.mailService import send_verification_code
from services.authService import verify_email_code, register_user, get_user_by_email

auth_bp = Blueprint("auth", __name__)

@auth_bp.route("/send-code", methods=["POST"])
def send_code():
    email = request.json.get("email")
    send_verification_code(email)
    return jsonify({"message": "인증코드 전송 완료"}), 200

@auth_bp.route("/verify-code", methods=["POST"])
def verify_code():
    data = request.json
    email = data.get("email")
    code = data.get("code")
    success, msg = verify_email_code(email, code)
    return (jsonify({"message": msg}), 200) if success else (jsonify({"message": msg}), 400)

@auth_bp.route("/signup", methods=["POST"])
def signup():
    data = request.json
    user, status = register_user(data)
    if status == 201:
        login_user(user)  # 자동 로그인 처리
    return jsonify({"message": "회원가입 완료!"} if status == 201 else user), status
