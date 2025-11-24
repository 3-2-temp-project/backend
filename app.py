import os
from pathlib import Path
from flask import Flask
from dotenv import load_dotenv
from flask_login import LoginManager
from flask_cors import CORS
from flask_jwt_extended import JWTManager
from models import db, User, RestaurantInfo
from services.mailService import mail
from routes.authRoute import auth_bp
from routes.locationRoute import location_bp
from routes.reviewRoute import review_bp
from routes.suggestionRoute import suggestion_bp
from routes.badgeRoute import badge_bp
from routes.visitRoute import visit_bp
from routes.restaurantRoutes import restaurant_bp

from flask.json.provider import DefaultJSONProvider

import importlib
import init_data as _init_data

class UTF8JSONProvider(DefaultJSONProvider):
    def dumps(self, obj, **kwargs):
        kwargs.setdefault("ensure_ascii", False)
        return super().dumps(obj, **kwargs)

# ============================================
# 환경 변수 로드 (명시적 경로 지정)
# ============================================
BASE_DIR = Path(__file__).resolve().parent
ENV_PATH = BASE_DIR / '.env'

print(f"[ENV] Current working directory: {os.getcwd()}")
print(f"[ENV] Script location: {BASE_DIR}")
print(f"[ENV] Looking for .env at: {ENV_PATH}")
print(f"[ENV] .env exists: {ENV_PATH.exists()}")

if ENV_PATH.exists():
    load_dotenv(ENV_PATH, override=True)  # override=True로 명시적 로드
    print(f"[ENV] ✓ .env loaded successfully from: {ENV_PATH}")
else:
    print(f"[ENV] ✗ .env not found at: {ENV_PATH}")
    load_dotenv()  # 기본 경로에서 시도

# 환경 변수 검증 및 디버깅
print(f"\n[ENV] Environment variables check:")
env_vars = {
    'NAVER_CLIENT_ID': os.environ.get('NAVER_CLIENT_ID'),
    'NAVER_CLIENT_SECRET': os.environ.get('NAVER_CLIENT_SECRET'),
    'NAVER_LOCAL_SEARCH_CLIENT_ID': os.environ.get('NAVER_LOCAL_SEARCH_CLIENT_ID'),
    'NAVER_LOCAL_SEARCH_CLIENT_SECRET': os.environ.get('NAVER_LOCAL_SEARCH_CLIENT_SECRET'),
    'DATABASE_URL': os.environ.get('DATABASE_URL', '')[:50] + '...' if os.environ.get('DATABASE_URL') else None,
}

for key, value in env_vars.items():
    status = '✓' if value else '✗'
    display_value = value if value and len(str(value)) < 20 else (str(value)[:15] + '...' if value else 'Not set')
    print(f"  {status} {key}: {display_value}")

# 필수 환경 변수 체크
critical_vars = ['DATABASE_URL', 'SECRET_KEY']
missing_critical = [var for var in critical_vars if not os.environ.get(var)]
if missing_critical:
    print(f"\n[ENV] ⚠️  WARNING: Missing critical variables: {', '.join(missing_critical)}")

app = Flask(__name__)
app.json_provider_class = UTF8JSONProvider
app.json = app.json_provider_class(app)
app.config['JSON_AS_ASCII'] = False
app.config['JSONIFY_MIMETYPE'] = 'application/json; charset=utf-8'

app.config['SECRET_KEY'] = os.environ.get("SECRET_KEY", "default_secret_key")
app.config['SQLALCHEMY_DATABASE_URI'] = os.environ.get("DATABASE_URL")
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['SQLALCHEMY_ENGINE_OPTIONS'] = {
    "pool_pre_ping": True,
    "pool_recycle": 1800,
}

app.config['JWT_SECRET_KEY'] = os.environ.get("JWT_SECRET_KEY", app.config['SECRET_KEY'])
app.config['JWT_ACCESS_TOKEN_EXPIRES'] = int(os.environ.get("JWT_ACCESS_TOKEN_EXPIRES", 3600))
app.config['JWT_REFRESH_TOKEN_EXPIRES'] = int(os.environ.get("JWT_REFRESH_TOKEN_EXPIRES", 2592000))
app.config['JWT_TOKEN_LOCATION'] = ['headers']
app.config['JWT_HEADER_NAME'] = 'Authorization'
app.config['JWT_HEADER_TYPE'] = 'Bearer'

CORS(app, 
     supports_credentials=True,
     origins=os.environ.get("CORS_ORIGINS", "http://localhost:3000,http://localhost:8080").split(","),
     allow_headers=["Content-Type", "Authorization"],
     expose_headers=["Content-Type", "Authorization"],
     methods=["GET", "POST", "PUT", "DELETE", "OPTIONS", "PATCH"])

jwt = JWTManager(app)

login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = "auth.login"

@login_manager.user_loader
def load_user(user_id):
    return db.session.get(User, int(user_id))

@jwt.user_identity_loader
def user_identity_lookup(user):
    return user.id

@jwt.user_lookup_loader
def user_lookup_callback(_jwt_header, jwt_data):
    identity = jwt_data["sub"]
    return db.session.get(User, identity)

@jwt.expired_token_loader
def expired_token_callback(jwt_header, jwt_payload):
    return {"message": "토큰이 만료되었습니다.", "error": "token_expired"}, 401

@jwt.invalid_token_loader
def invalid_token_callback(error):
    return {"message": "유효하지 않은 토큰입니다.", "error": "invalid_token"}, 401

@jwt.unauthorized_loader
def missing_token_callback(error):
    return {"message": "인증 토큰이 필요합니다.", "error": "authorization_required"}, 401

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
app.register_blueprint(restaurant_bp, url_prefix="/restaurants")


@app.route("/")
def index():
    return "서버 정상 작동 중!"

def run_pdf_downloader_inprocess():
    import sys
    from services.pdf_downloader import main as downloader_main

    os.environ["RUN_FROM_FLASK"] = "1"
    os.environ.setdefault("DOWNLOADER_EXECUTOR", "thread")

    outdir   = os.getenv("PDF_BASE_DIR", "pdf_data")
    use_sln  = _env_flag("DOWNLOAD_SELENIUM", "0")
    debug    = _env_flag("DOWNLOAD_DEBUG", "0")
    testmode = _env_flag("DOWNLOAD_TEST", "0")
    workers  = os.getenv("DOWNLOAD_WORKERS", "8")

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
    import sys, subprocess, os

    outdir   = os.getenv("PDF_BASE_DIR", "pdf_data")
    workers  = os.getenv("DOWNLOAD_WORKERS", "12")
    use_sln  = _env_flag("DOWNLOAD_SELENIUM", "0")
    debug    = _env_flag("DOWNLOAD_DEBUG", "0")
    testmode = _env_flag("DOWNLOAD_TEST", "0")

    args = [sys.executable, "-m", "services.pdf_downloader",
            "--workers", workers, "--outdir", outdir]
    if use_sln:  args.append("--selenium")
    if debug:    args.append("--debug")
    if testmode: args.append("--test")

    print("[DOWNLOAD_ON_BOOT] spawn:", " ".join(args))

    cwd = os.path.dirname(os.path.abspath(__file__))

    try:
        subprocess.check_call(args, cwd=cwd)
    except Exception:
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
    run_once = (os.environ.get("WERKZEUG_RUN_MAIN") == "true") or (not app.debug)
    INIT_DATA_ENABLE = _env_flag("INIT_DATA_ENABLE", "1")

    TESTMODE = _env_flag("TESTMODE", "0")
    if TESTMODE:
        os.environ.setdefault("ALLOW_NO_GEOCODE", "1")

    DOWNLOAD_SKIP = _env_flag("DOWNLOAD_SKIP", "0")

    with app.app_context():
        RESET_DB = _env_flag("RESET_DB", "0")
        if RESET_DB:
            print("[DB] Dropping and recreating all tables...")
            db.drop_all()
            db.create_all()
            print("[DB] ✓ Tables recreated")
        else:
            db.create_all()
            print("[DB] ✓ Tables verified/created")

        if run_once:
            # 1) INIT_DATA_ENABLE이 켜져 있을 때만 init_data 실행
            if INIT_DATA_ENABLE:
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

                try:
                    INIT_LIMIT     = _env_int("INIT_LIMIT", 0)
                    INIT_CHUNK     = _env_int("INIT_CHUNK_SIZE", 1000)
                    USE_STREAMING  = _env_flag("USE_STREAMING_UPSERT", "1")
                    base_dir       = os.getenv("PDF_BASE_DIR", "pdf_data")

                    if USE_STREAMING:
                        from init_data import refresh_init_data_and_insert_streaming
                        effective_limit = 0 if INIT_LIMIT <= 0 else INIT_LIMIT
                        refresh_init_data_and_insert_streaming(
                            base_dir=base_dir,
                            limit=effective_limit,
                            commit_every=INIT_CHUNK,
                            require_both=True,
                            allow_no_geocode=True,
                        )
                    else:
                        from init_data import refresh_init_data_and_insert
                        limit = None if INIT_LIMIT <= 0 else INIT_LIMIT
                        refresh_init_data_and_insert(base_dir=base_dir, limit=limit)

                    importlib.reload(_init_data)

                    try:
                        total = db.session.execute(
                            db.select(db.func.count()).select_from(RestaurantInfo)
                        ).scalar_one()
                        print(f"[VERIFY] restaurant_info count = {total}")
                    except Exception as e:
                        print("[VERIFY] count 확인 실패:", e)

                except Exception as e:
                    print("[INIT_DATA] refresh error:", e)
            else:
                print("[INIT_DATA] skipped (INIT_DATA_ENABLE=0)")

            # ============================================
            # REPAIR 기능 (에러 핸들링 강화)
            # ============================================
            ENABLE_REPAIR = _env_flag("ENABLE_REPAIR", "1")  # 기본값 1
            
            if ENABLE_REPAIR:
                try:
                    from init_data import repair_restaurant_info
                    
                    repair_mode  = os.getenv("REPAIR_MODE", "addr")
                    repair_limit = _env_int("REPAIR_LIMIT", 1000)
                    repair_dry   = _env_flag("REPAIR_DRY_RUN", "0")
                    
                    # API 키 검증
                    has_geocode_api = bool(
                        os.environ.get('NAVER_CLIENT_ID') and 
                        os.environ.get('NAVER_CLIENT_SECRET')
                    )
                    has_local_api = bool(
                        os.environ.get('NAVER_LOCAL_SEARCH_CLIENT_ID') and 
                        os.environ.get('NAVER_LOCAL_SEARCH_CLIENT_SECRET')
                    )
                    
                    print(f"\n[REPAIR] Configuration:")
                    print(f"  - Mode: {repair_mode}")
                    print(f"  - Limit: {repair_limit}")
                    print(f"  - Dry run: {repair_dry}")
                    print(f"  - Geocode API: {'✓' if has_geocode_api else '✗'}")
                    print(f"  - Local Search API: {'✓' if has_local_api else '✗'}")
                    
                    # mode에 따라 필요한 API 체크
                    can_proceed = True
                    if repair_mode in ("addr", "all") and not has_geocode_api:
                        print(f"[REPAIR] ⚠️  WARNING: Address repair requires NAVER_CLIENT_ID/SECRET")
                        if repair_mode == "addr":
                            can_proceed = False
                        else:
                            print(f"[REPAIR] Will skip address repair, continue with name repair only")
                            repair_mode = "name"
                    
                    if repair_mode in ("name", "all") and not has_local_api:
                        print(f"[REPAIR] ⚠️  WARNING: Name repair requires NAVER_LOCAL_SEARCH_CLIENT_ID/SECRET")
                        if repair_mode == "name":
                            can_proceed = False
                        else:
                            print(f"[REPAIR] Will skip name repair, continue with address repair only")
                            repair_mode = "addr"
                    
                    if can_proceed:
                        print(f"[REPAIR] Starting repair with mode: {repair_mode}")
                        result = repair_restaurant_info(
                            mode=repair_mode,
                            limit=repair_limit,
                            dry_run=repair_dry,
                        )
                        
                        print(f"\n[REPAIR] ✓ Complete:")
                        if result.get('addresses'):
                            addr_stats = result['addresses']
                            print(f"  Address repair: {addr_stats.get('updated', 0)} updated, "
                                  f"{addr_stats.get('skipped', 0)} skipped, "
                                  f"{addr_stats.get('errors', 0)} errors")
                        
                        if result.get('names'):
                            name_stats = result['names']
                            print(f"  Name repair: {name_stats.get('updated', 0)} updated, "
                                  f"{name_stats.get('skipped', 0)} skipped, "
                                  f"{name_stats.get('errors', 0)} errors")
                    else:
                        print(f"[REPAIR] ✗ Skipped: Missing required API credentials")
                        
                except ImportError as e:
                    print(f"[REPAIR] ✗ Import error: {e}")
                except Exception as e:
                    print(f"[REPAIR] ✗ Error during repair: {e}")
                    import traceback
                    traceback.print_exc()
            else:
                print("[REPAIR] Skipped (ENABLE_REPAIR=0)")

    print("\n[APP] Starting Flask application...")
    app.run(host="0.0.0.0", port=_env_int("PORT", 5000), debug=True, threaded=True)