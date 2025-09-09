from flask import Blueprint, request, jsonify
from flask_login import login_required, login_user, logout_user, current_user
from services.mailService import send_verification_code
from services.authService import (
    verify_email_code,
    register_user,
    login_user_by_credentials
)

# 이후 사용자 인증에 필요한 API에 @login_required 붙여서 사용하기

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

@auth_bp.route("/login", methods=["POST"])
def login():
    data = request.json
    user_id = data.get("user_id")
    password = data.get("password")

    user, msg, status = login_user_by_credentials(user_id, password)

    if status == 200:
        return jsonify({"message": msg}), 200
    else:
        return jsonify({"message": msg}), status

@auth_bp.route("/logout", methods=["POST"])
@login_required
def logout():
    logout_user()
    return jsonify({"message": "로그아웃 되었습니다."}), 200

@auth_bp.route("/me", methods=["GET"])
@login_required
def get_current_user():
    return jsonify({
        "user_id": current_user.user_id,
        "user_nickname": current_user.user_nickname,
        "email": current_user.email
    }), 200