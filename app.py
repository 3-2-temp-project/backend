import os
from flask import Flask
from dotenv import load_dotenv
from flask_login import LoginManager
from models import db, User
from services.mailService import mail
from routes.authRoute import auth_bp
from routes.locationRoute import location_bp
from init_data import insert_initial_restaurants

# .env 파일 로드
load_dotenv()

app = Flask(__name__)

# 시크릿 키 & DB 설정
app.config['SECRET_KEY'] = os.environ.get("SECRET_KEY", "default_secret_key")
app.config['SQLALCHEMY_DATABASE_URI'] = os.environ.get("DATABASE_URL")
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

# 로그인 매니저 설정
login_manager = LoginManager()
login_manager.init_app(app)

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

# 확장 초기화
db.init_app(app)
mail.init_app(app)

# 블루프린트 등록
app.register_blueprint(auth_bp)
app.register_blueprint(location_bp, url_prefix="/")

# 기본 라우트
@app.route('/')
def index():
    return "서버 정상 작동 중!"

# 앱 실행
if __name__ == "__main__":
    with app.app_context():
        db.drop_all()  # ⚠️ 개발 중에만! 운영에서는 제거
        db.create_all()
        insert_initial_restaurants()
    app.run(host='0.0.0.0', port=5000, debug=True)
