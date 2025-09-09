import os
from flask import Flask
from dotenv import load_dotenv
from models import db
from routes.locationRoute import location_bp
from init_data import insert_initial_restaurants

# .env 파일 로드
load_dotenv()

app = Flask(__name__)

# 시크릿 키 (세션에 필요)
app.secret_key = os.getenv("SECRET_KEY")

# DB 설정
app.config['SQLALCHEMY_DATABASE_URI'] = os.getenv("DATABASE_URL")
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

# SQLAlchemy 초기화
db.init_app(app)

# 라우터 등록
app.register_blueprint(location_bp, url_prefix="/")

# 기본 라우트
@app.route('/')
def index():
    return "서버 정상 작동 중!"

# 앱 실행
if __name__ == "__main__":
    with app.app_context():
        db.drop_all()              # ⚠️ 개발 중에만! 운영에서는 제거!
        db.create_all()
        insert_initial_restaurants()
    app.run(host='0.0.0.0', port=5000, debug=True)
