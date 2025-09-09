from flask import Flask
from models import db
from routes.authRoute import auth_bp
from services.mailService import mail
from flask_login import LoginManager
from dotenv import load_dotenv
import os

# .env 로드
load_dotenv()

app = Flask(__name__)

# DB 설정
app.config['SQLALCHEMY_DATABASE_URI'] = os.environ.get("DATABASE_URL")
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

# 로그인 매니저 설정
app.config['SECRET_KEY'] = os.environ.get("SECRET_KEY", "default_secret_key")
login_manager = LoginManager()
login_manager.init_app(app)

# User 모델 로딩 함수
from models import User
@login_manager.user_loader
def load_user(user_id):
    return User.query.get(int(user_id))

# 메일 설정
app.config['MAIL_SERVER'] = os.environ.get("MAIL_SERVER", "smtp.gmail.com")
app.config['MAIL_PORT'] = int(os.environ.get("MAIL_PORT", 587))
app.config['MAIL_USE_TLS'] = True
app.config['MAIL_USERNAME'] = os.environ.get("MAIL_USERNAME")
app.config['MAIL_PASSWORD'] = os.environ.get("MAIL_PASSWORD")
app.config['MAIL_DEFAULT_SENDER'] = os.environ.get("MAIL_USERNAME")

# 초기화
db.init_app(app)
mail.init_app(app)

# 라우터 등록
app.register_blueprint(auth_bp)

@app.route('/')
def index():
    return "서버 정상 작동 중!"

if __name__ == "__main__":
    with app.app_context():
        db.drop_all()     # 기존 테이블 전체 삭제 => 테스트용으로 추후 제거 필요
        db.create_all()
    app.run(host='0.0.0.0', port=5000, debug=True)
