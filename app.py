import os
from flask import Flask
from dotenv import load_dotenv
from flask_login import LoginManager
from models import db, User, RestaurantInfo  # ✅ RestaurantInfo를 가져와 사후 검증에 사용
from services.mailService import mail
from routes.authRoute import auth_bp
from routes.locationRoute import location_bp
from routes.reviewRoute import review_bp
from routes.suggestionRoute import suggestion_bp
from routes.badgeRoute import badge_bp
from routes.visitRoute import visit_bp
from flask.json.provider import DefaultJSONProvider

import importlib
import init_data as _init_data

from flask_cors import CORS


class UTF8JSONProvider(DefaultJSONProvider):
    def dumps(self, obj, **kwargs):
        kwargs.setdefault("ensure_ascii", False)
        return super().dumps(obj, **kwargs)

# .env 로드
load_dotenv()

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "http://localhost:3000"}}, supports_credentials=True)
app.json_provider_class = UTF8JSONProvider
app.json = app.json_provider_class(app)
app.config['JSON_AS_ASCII'] = False
app.config['JSONIFY_MIMETYPE'] = 'application/json; charset=utf-8'

# 시크릿/DB
app.config['SECRET_KEY'] = os.environ.get("SECRET_KEY", "default_secret_key")
app.config['SQLALCHEMY_DATABASE_URI'] = os.environ.get("DATABASE_URL")
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['SQLALCHEMY_ENGINE_OPTIONS'] = {
    "pool_pre_ping": True,
    "pool_recycle": 1800,
}

# 로그인 매니저
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = "auth.login"

@login_manager.user_loader
def load_user(user_id):
    return db.session.get(User, int(user_id))

# env helpers
def _env_flag(name, default="0"):
    return os.getenv(name, default).lower() in ("1", "true", "yes", "y")

def _env_int(name, default: int):
    try:
        return int(os.getenv(name, str(default)))
    except Exception:
        return default

# 메일
MAIL_USE_SSL = _env_flag("MAIL_USE_SSL", "0")
MAIL_USE_TLS = _env_flag("MAIL_USE_TLS", "1") and not MAIL_USE_SSL
app.config['MAIL_SERVER'] = os.environ.get("MAIL_SERVER", "smtp.gmail.com")
app.config['MAIL_PORT'] = _env_int("MAIL_PORT", 465 if MAIL_USE_SSL else 587)
app.config['MAIL_USE_TLS'] = MAIL_USE_TLS
app.config['MAIL_USE_SSL'] = MAIL_USE_SSL
app.config['MAIL_USERNAME'] = os.environ.get("MAIL_USERNAME")
app.config['MAIL_PASSWORD'] = os.environ.get("MAIL_PASSWORD")
app.config['MAIL_DEFAULT_SENDER'] = os.environ.get("MAIL_USERNAME")

# 확장 초기화
db.init_app(app)
mail.init_app(app)

# 블루프린트
app.register_blueprint(auth_bp,       url_prefix="/auth")
app.register_blueprint(location_bp,   url_prefix="/")
app.register_blueprint(review_bp,     url_prefix="/reviews")
app.register_blueprint(suggestion_bp, url_prefix="/suggestions")
app.register_blueprint(badge_bp,      url_prefix="/badges")
app.register_blueprint(visit_bp,      url_prefix="/visits")

@app.route("/")
def index():
    return "서버 정상 작동 중!"

def run_pdf_downloader_inprocess():
    import sys
    from services.pdf_downloader import main as downloader_main

    # 스레드 모드 신호
    os.environ["RUN_FROM_FLASK"] = "1"                   # ← Flask 내부 실행 표시
    os.environ.setdefault("DOWNLOADER_EXECUTOR", "thread")  # ← 스레드 실행 강제

    outdir   = os.getenv("PDF_BASE_DIR", "pdf_data")
    use_sln  = _env_flag("DOWNLOAD_SELENIUM", "0")
    debug    = _env_flag("DOWNLOAD_DEBUG", "0")
    testmode = _env_flag("DOWNLOAD_TEST", "0")
    workers  = os.getenv("DOWNLOAD_WORKERS", "8")        # 원하는 병렬 스레드 수

    argv = ["pdf_downloader", "--workers", workers, "--outdir", outdir]
    if use_sln: argv.append("--selenium")
    if debug:   argv.append("--debug")
    if testmode:argv.append("--test")

    print("[DOWNLOAD_ON_BOOT] run in-process with argv:", " ".join(argv))
    _old_argv = sys.argv[:]
    try:
        sys.argv = argv
        downloader_main()
        print("[DOWNLOAD_ON_BOOT] done (in-process)")
    finally:
        sys.argv = _old_argv

def run_pdf_downloader_subprocess():
    """
    다운로더를 별도 프로세스로 실행 (ProcessPoolExecutor 안전)
    - services/pdf_downloader.py 가 모듈로 실행 가능해야 함(services 폴더에 __init__.py 필요)
    """
    import sys, subprocess, os

    outdir   = os.getenv("PDF_BASE_DIR", "pdf_data")
    workers  = os.getenv("DOWNLOAD_WORKERS", "12")   # 프로세스 개수
    use_sln  = _env_flag("DOWNLOAD_SELENIUM", "0")
    debug    = _env_flag("DOWNLOAD_DEBUG", "0")
    testmode = _env_flag("DOWNLOAD_TEST", "0")

    # 우선 모듈 실행 시도 (services 가 패키지여야 함: services/__init__.py 존재)
    args = [sys.executable, "-m", "services.pdf_downloader",
            "--workers", workers, "--outdir", outdir]
    if use_sln:  args.append("--selenium")
    if debug:    args.append("--debug")
    if testmode: args.append("--test")

    print("[DOWNLOAD_ON_BOOT] spawn:", " ".join(args))

    # 현재 프로젝트 루트에서 실행되도록 보장
    cwd = os.path.dirname(os.path.abspath(__file__))

    try:
        subprocess.check_call(args, cwd=cwd)
    except Exception:
        # 패키지 실행이 불가능하면 직접 파일 경로로 재시도
        script_path = os.path.join(cwd, "services", "pdf_downloader.py")
        alt_args = [sys.executable, script_path,
                    "--workers", workers, "--outdir", outdir]
        if use_sln:  alt_args.append("--selenium")
        if debug:    alt_args.append("--debug")
        if testmode: alt_args.append("--test")

        print("[DOWNLOAD_ON_BOOT] fallback spawn:", " ".join(alt_args))
        subprocess.check_call(alt_args, cwd=cwd)

    print("[DOWNLOAD_ON_BOOT] done (subprocess)")
    
if __name__ == "__main__":
    # 리로더 자식에서 1회만
    run_once = (os.environ.get("WERKZEUG_RUN_MAIN") == "true") or (not app.debug)

    TESTMODE = _env_flag("TESTMODE", "0")
    if TESTMODE:
        os.environ.setdefault("ALLOW_NO_GEOCODE", "1")  # 테스트 시 지오코딩 실패 허용

    # 🔹 다운로드 전체 스킵 플래그
    DOWNLOAD_SKIP = _env_flag("DOWNLOAD_SKIP", "0")

    with app.app_context():
        # 현재 연결된 DB 위치 확인 로그 (문제 추적에 도움)
        try:
            print(f"[DB] engine url = {db.engine.url}")
        except Exception as e:
            print("[DB] engine url 확인 실패:", e)

        RESET_DB = _env_flag("RESET_DB", "0")
        if RESET_DB:
            db.drop_all()
            db.create_all()
        else:
            db.create_all()

        if run_once:
            # 1) PDF/첨부 다운로더 실행 (v5 main() 직접 호출)
            if (not TESTMODE) and _env_flag("DOWNLOAD_ON_BOOT", "1"):
                if DOWNLOAD_SKIP:
                    print("[DOWNLOAD_ON_BOOT] skipped due to DOWNLOAD_SKIP=1")
                else:
                    try:
                        print("[DOWNLOAD_ON_BOOT] start")
                        run_pdf_downloader_inprocess()
                    except Exception as e:
                        print("[DOWNLOAD_ON_BOOT] error:", repr(e))
            else:
                print("[DOWNLOAD_ON_BOOT] skipped (TESTMODE or disabled)")

            # 2) 파싱 → (지오코딩) → DB 업서트 (스트리밍/무제한 가능)
            try:
                INIT_LIMIT     = _env_int("INIT_LIMIT", 0)           # 0 또는 음수 → 무제한
                INIT_CHUNK     = _env_int("INIT_CHUNK_SIZE", 1000)   # 배치 커밋 크기
                USE_STREAMING  = _env_flag("USE_STREAMING_UPSERT", "1")
                base_dir       = os.getenv("PDF_BASE_DIR", "pdf_data")

                if USE_STREAMING:
                    from init_data import refresh_init_data_and_insert_streaming
                    effective_limit = 0 if INIT_LIMIT <= 0 else INIT_LIMIT
                    refresh_init_data_and_insert_streaming(
                        base_dir=base_dir,
                        limit=effective_limit,           # ✅ 0이면 무제한
                        commit_every=INIT_CHUNK,         # ✅ 이름 일치
                        require_both=True,
                        allow_no_geocode=True,
                    )
                else:
                    from init_data import refresh_init_data_and_insert
                    limit = None if INIT_LIMIT <= 0 else INIT_LIMIT
                    refresh_init_data_and_insert(base_dir=base_dir, limit=limit)

                # 모듈 리로드는 중복 호출 방지를 위해 유지만(캐시 초기화 용)
                importlib.reload(_init_data)

                # ✅ 사후 검증: 실제 테이블에 몇 건 들어갔는지 출력
                try:
                    total = db.session.execute(
                        db.select(db.func.count()).select_from(RestaurantInfo)
                    ).scalar_one()
                    print(f"[VERIFY] restaurant_info count = {total}")
                except Exception as e:
                    print("[VERIFY] count 확인 실패:", e)

            except Exception as e:
                print("[INIT_DATA] refresh error:", e)

    app.run(host="0.0.0.0", port=_env_int("PORT", 5000), debug=True, threaded=True)
