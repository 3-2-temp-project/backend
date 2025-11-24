import os
import re
import csv
import math
import logging
import unicodedata
from functools import lru_cache
from typing import Optional, List, Tuple, Dict, Set, Any
from datetime import datetime
import time
from collections import defaultdict

import requests
import pdfplumber

# ---------- Optional parsers ----------
try:
    import openpyxl  # .xlsx
except ImportError:
    openpyxl = None

try:
    import xlrd  # .xls
except ImportError:
    xlrd = None

try:
    import olefile  # .hwp (OLE) preview text
except ImportError:
    olefile = None

import zipfile            # .hwpx (ZIP)
import xml.etree.ElementTree as ET  # XML text extraction for HWPX

from models import db, RestaurantInfo
from concurrent.futures import ProcessPoolExecutor, as_completed
import multiprocessing

# ============================================
# 로깅 설정 - 프로덕션에서는 WARNING 레벨 권장
# ============================================
LOG_LEVEL = os.getenv("LOG_LEVEL", "WARNING").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.WARNING),
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger(__name__)

# ============================================
# 환경 변수 및 설정
# ============================================
PDF_BASE_DIR = os.getenv("PDF_BASE_DIR", "pdf_data")
NAVER_GEOCODE_URL = "https://maps.apigw.ntruss.com/map-geocode/v2/geocode"
NAVER_CLIENT_ID = os.getenv("NAVER_CLIENT_ID")
NAVER_CLIENT_SECRET = os.getenv("NAVER_CLIENT_SECRET")
NAVER_LOCAL_SEARCH_CLIENT_ID = os.getenv("NAVER_LOCAL_SEARCH_CLIENT_ID")
NAVER_LOCAL_SEARCH_CLIENT_SECRET = os.getenv("NAVER_LOCAL_SEARCH_CLIENT_SECRET")
NAVER_LOCAL_URL = "https://openapi.naver.com/v1/search/local.json"


REGION_HINTS: List[str] = [h.strip() for h in os.getenv("GEOCODE_REGION_HINT", "").split("|") if h.strip()]
ALLOW_NO_GEOCODE = os.getenv("ALLOW_NO_GEOCODE", "true").lower() in ("1", "true", "yes", "y")
print("[DEBUG] NAVER_CLIENT_ID =", repr(NAVER_CLIENT_ID))

# 성능 최적화 설정
MAX_GEOCODE_CACHE = 2000  # 지오코딩 캐시 크기
DEFAULT_BATCH_SIZE = 300  # 기본 배치 크기
DB_RETRY_COUNT = 3  # DB 작업 재시도 횟수
DB_RETRY_DELAY = 0.5  # 재시도 간 대기 시간 (초)
# ============================================
# 네이버 Local 검색으로 카테고리 가져오기
# ============================================

# (이름, 주소) 단위로 카테고리 캐시
_LOCAL_CATEGORY_CACHE: Dict[Tuple[str, str], Optional[str]] = {}

def _worker_parse_and_process(filepath: str, limit: Optional[int] = None) -> List[Dict]:
    """
    서브 프로세스에서 실행:
    - 파일 파싱(parse_file)
    - 행 변환(process_extracted_rows)
    - DB는 건드리지 않고 record 리스트만 반환
    """
    rows = parse_file(filepath)
    if not rows:
        return []
    # limit은 각 파일당 제한이라, 전체 limit는 메인에서 다시 체크
    return process_extracted_rows(rows, limit)

def haversine(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    """
    두 위도/경도 사이 거리(m) 계산 (Haversine formula)
    """
    R = 6371000  # 지구 반지름 (미터)
    rad = math.radians

    dlat = rad(lat2 - lat1)
    dlng = rad(lng2 - lng1)

    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(rad(lat1)) * math.cos(rad(lat2)) * math.sin(dlng / 2) ** 2
    )
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c

def _call_naver_local_search(query: str, display: int = 5) -> Tuple[Optional[List[Dict[str, Any]]], str, int]:
    """
    네이버 검색 > 지역 API 호출
    query: 상호명 + 주소 등으로 만든 검색어
    """
    if not NAVER_LOCAL_SEARCH_CLIENT_ID or not NAVER_LOCAL_SEARCH_CLIENT_SECRET:
        return None, "NAVER_LOCAL_SEARCH_CLIENT_ID / NAVER_LOCAL_SEARCH_CLIENT_SECRET 필요", 500

    headers = {
        "X-Naver-Client-Id": NAVER_LOCAL_SEARCH_CLIENT_ID,
        "X-Naver-Client-Secret": NAVER_LOCAL_SEARCH_CLIENT_SECRET,
    }

    params = {
        "query": query,
        "display": min(max(display, 1), 5),
        "start": 1,
        "sort": "random",  # 공식 옵션: random / comment ...
    }

    try:
        resp = requests.get(NAVER_LOCAL_URL, headers=headers, params=params, timeout=5)
    except Exception as e:
        return None, f"Naver Local API 요청 실패: {e}", 502

    if resp.status_code != 200:
        return None, f"Naver Local API 오류: HTTP {resp.status_code}", 502

    data = resp.json()
    items = data.get("items", [])
    if not items:
        return None, "검색 결과 없음", 404

    return items, "ok", 200


def _choose_best_local_place(
    items: List[Dict[str, Any]],
    lat: float,
    lng: float,
    res_name: str,
    max_distance: int = 300,
) -> Optional[Dict[str, Any]]:
    """
    Local 결과 중 우리 좌표/상호명과 가장 잘 맞는 후보 선택
    - 이름 매칭 점수 > 거리 순
    - lat/lng가 0인 경우에는 거리 필터 없이 이름 매칭만 사용
    """
    target_name = (res_name or "").lower()
    best: Optional[Tuple[Dict[str, Any], float, int]] = None  # (item, dist, name_score)

    for it in items:
        mapx = it.get("mapx")
        mapy = it.get("mapy")
        if not mapx or not mapy:
            continue

        try:
            cand_lng = float(mapx) / 1e7
            cand_lat = float(mapy) / 1e7
        except ValueError:
            continue

        # 좌표가 0,0 이면 거리 필터링은 안 씀
        if lat == 0.0 and lng == 0.0:
            dist = 0.0
        else:
            dist = haversine(lat, lng, cand_lat, cand_lng)
            if max_distance and dist > max_distance:
                # 너무 멀면 제외
                continue

        title = _strip_html_tags(it.get("title") or "")
        lower_title = title.lower()

        name_score = 0
        if target_name and target_name in lower_title:
            name_score = 2
        elif target_name and lower_title in target_name:
            name_score = 1

        if best is None:
            best = (it, dist, name_score)
            continue

        _, prev_dist, prev_score = best

        # 이름 점수 우선, 같으면 더 가까운 것
        if name_score > prev_score or (name_score == prev_score and dist < prev_dist):
            best = (it, dist, name_score)

    if best is None:
        return None
    return best[0]


def _get_category_from_naver_local(
    res_name: str,
    lat: float,
    lng: float,
    address: str = "",
    radius: int = 300,
    debug: bool = False,
) -> Optional[str]:
    """
    상호명 + 좌표(+주소)를 이용해서 네이버 지역검색에서 카테고리 가져오기
    (캐시 포함)
    """
    name_key = (res_name or "").strip()
    addr_key = (address or "").strip()
    cache_key = (name_key, addr_key)

    # 캐시 사용
    if cache_key in _LOCAL_CATEGORY_CACHE:
        return _LOCAL_CATEGORY_CACHE[cache_key]

    # 검색어: 상호명 + 주소
    query_parts = [name_key]
    if addr_key:
        query_parts.append(addr_key)
    query = " ".join(p for p in query_parts if p)

    if not query:
        _LOCAL_CATEGORY_CACHE[cache_key] = None
        return None

    if debug:
        log.debug(f"[LOCAL] query='{query}', center=({lat}, {lng}), radius={radius}m")

    items, msg, status = _call_naver_local_search(query)
    if status != 200 or not items:
        if debug:
            log.debug(f"[LOCAL] 검색 실패: {msg} (status={status})")
        _LOCAL_CATEGORY_CACHE[cache_key] = None
        return None

    best_item = _choose_best_local_place(items, lat, lng, name_key, max_distance=radius)
    if not best_item:
        if debug:
            log.debug("[LOCAL] 반경 내 적절한 후보 없음")
        _LOCAL_CATEGORY_CACHE[cache_key] = None
        return None

    category = best_item.get("category")
    if debug:
        title = _strip_html_tags(best_item.get("title") or "")
        log.debug(f"[LOCAL] 선택된 가게: {title}, category={category}")

    _LOCAL_CATEGORY_CACHE[cache_key] = category
    return category

# ============================================
# 카테고리 자동 분류
# ============================================
_HTML_TAG_RE = re.compile(r"<[^>]*>")

def _strip_html_tags(text: str) -> str:
    """네이버 title 필드의 HTML 태그 제거"""
    if not text:
        return ""
    return _HTML_TAG_RE.sub("", text)

_CATEGORY_KEYWORDS = {
    "카페": [
        "카페", "cafe", "coffee", "커피", "빈티지", "로스터리", "에스프레소",
        "베이커리카페", "디저트카페", "브런치카페", "테라스", "루프탑카페",
        "스타벅스", "이디야", "투썸", "엔제리너스", "할리스", "빽다방",
        "메가커피", "컴포즈", "탐앤탐스", "카페베네"
    ],
    "한식": [
        "한식", "국밥", "찌개", "된장", "김치", "비빔밥", "불고기", "갈비",
        "삼겹살", "보쌈", "족발", "순대", "곱창", "막창", "한정식", "백반",
        "칼국수", "수제비", "냉면", "국수", "만두", "떡볶이", "순두부",
        "설렁탕", "곰탕", "갈비탕", "삼계탕", "추어탕", "해장국", "감자탕"
    ],
    "중식": [
        "중식", "중국집", "짜장", "짬뽕", "탕수육", "양장피", "마라", "훠궈",
        "딤섬", "중국만두", "유산슬", "깐풍", "라조기", "궈바오", "양꼬치",
        "꿔바로우", "팔보채", "간짜장"
    ],
    "일식": [
        "일식", "초밥", "스시", "사시미", "회", "라멘", "돈카츠", "덮밥",
        "우동", "소바", "텐동", "규동", "가츠동", "이자카야", "오꼬노미야끼",
        "타코야끼", "야키토리", "샤브샤브", "스키야키", "롤", "일본",
        "참치", "연어", "장어", "모밀"
    ],
    "양식": [
        "양식", "스테이크", "파스타", "피자", "리조또", "그라탕", "오믈렛",
        "샐러드", "샌드위치", "버거", "햄버거", "브런치", "이탈리안", "프렌치",
        "레스토랑", "비스트로", "트라토리아"
    ],
    "치킨": [
        "치킨", "닭강정", "닭갈비", "후라이드", "양념치킨", "통닭", "BBQ",
        "교촌", "bhc", "굽네", "페리카나", "네네", "처갓집", "호식이",
        "멕시카나", "깐풍기"
    ],
    "피자": [
        "피자", "pizza", "도미노", "피자헛", "파파존스", "미스터피자",
        "피자스쿨", "반올림피자", "피자마루", "피자알볼로"
    ],
    "패스트푸드": [
        "패스트푸드", "버거킹", "맘스터치", "롯데리아", "맥도날드", "KFC",
        "서브웨이", "퀴즈노스", "맥", "버거", "햄버거", "프랜차이즈"
    ],
    "분식": [
        "분식", "떡볶이", "순대", "튀김", "김밥", "라면", "어묵", "오뎅",
        "쫄면", "물떡", "로제떡볶이"
    ],
    "베이커리": [
        "베이커리", "빵", "제과", "제빵", "뚜레쥬르", "파리바게뜨", "던킨",
        "크로와상", "베이글", "도넛", "마카롱", "케이크", "타르트"
    ],
    "주점": [
        "주점", "호프", "술집", "이자카야", "선술집", "포차", "감성주점",
        "와인바", "바", "펍", "pub", "맥주", "소주", "막걸리", "호프집", "생맥주"
    ],
    "아시안": [
        "태국", "베트남", "쌀국수", "월남쌈", "팟타이", "똠얌", "분짜",
        "인도", "커리", "난", "탄두리", "동남아", "아시안"
    ],
    "고기": [
        "고기", "소고기", "돼지고기", "삼겹살", "목살", "갈비", "등심",
        "안심", "육회", "생고기", "한우", "흑돼지", "구이", "숯불",
        "양념구이", "생삼겹", "오겹살"
    ],
    "해산물": [
        "해산물", "회", "생선", "조개", "굴", "전복", "랍스터", "대게",
        "킹크랩", "새우", "광어", "연어", "참치", "해물", "수산", "활어", "물회"
    ],
    "디저트": [
        "디저트", "아이스크림", "빙수", "케이크", "마카롱", "와플", "팥빙수",
        "설빙", "젤라또", "초콜릿", "쿠키", "허니브레드", "크레페"
    ],
    "뷔페": [
        "뷔페", "buffet", "부페", "올유캔잇", "뷔페식당", "무한리필", "샐러드바"
    ]
}

# 카테고리 키워드를 미리 컴파일하여 성능 향상
_CATEGORY_PATTERNS = {}
for category, keywords in _CATEGORY_KEYWORDS.items():
    pattern = '|'.join(re.escape(keyword.lower()) for keyword in keywords)
    _CATEGORY_PATTERNS[category] = re.compile(pattern, re.IGNORECASE)

def _classify_category(name: str) -> Optional[str]:
    """식당 이름을 기반으로 카테고리 자동 분류 (최적화)"""
    if not name:
        return None
    
    name_lower = name.lower()
    
    # 컴파일된 패턴으로 더 빠르게 매칭
    for category, pattern in _CATEGORY_PATTERNS.items():
        if pattern.search(name_lower):
            return category
    
    return None

# ============================================
# 텍스트 정제 유틸리티
# ============================================
def _to_int(s: str) -> Optional[int]:
    """문자열을 정수로 변환"""
    if s is None:
        return None
    s = re.sub(r"[^\d]", "", str(s))
    return int(s) if s.isdigit() else None

def _normalize_spaces(s: str) -> str:
    """공백 정규화"""
    if not s:
        return ""
    s = s.replace("　", " ")  # 전각 공백 변환
    s = re.sub(r"\s+", " ", s).strip()
    return s

def _to_halfwidth(s: str) -> str:
    """전각 문자를 반각으로 변환"""
    return unicodedata.normalize('NFKC', s or "")

def _strip_weird_unicode(s: str) -> str:
    """특수 유니코드 문자 제거"""
    s = "".join(ch for ch in s if (ch.isprintable() and not unicodedata.category(ch).startswith("C")))
    # 특수 괄호 정규화
    s = s.replace("【", "[").replace("】", "]")
    s = s.replace("「", "[").replace("」", "]")
    s = s.replace("『", "[").replace("』", "]")
    return s

def _clean_text(s: Optional[str]) -> str:
    """텍스트 종합 정제"""
    s = "" if s is None else str(s)
    s = s.replace("장소명", "장소")
    s = _to_halfwidth(s)
    s = _strip_weird_unicode(s)
    return _normalize_spaces(s)

# ============================================
# 필터링 키워드
# ============================================
_EVENT_KEYWORDS = frozenset([
    "부스", "홍보부스", "체험부스", "행사", "이벤트", "축제", "페스티벌",
    "프로모션", "체험존", "전시부스", "박람회", "컨벤션", "엑스포",
    "홍보관", "현수막", "운영본부", "행사진행본부", "행사장", "무대설치",
])

_NOT_FNB_KEYWORDS = frozenset([
    "화환", "주유", "택시", "발렛", "사무용품", "대여", "프린트",
    "가맹점", "문구", "주차", "장례식장", "후생복지위원회",
    "체험", "홍보", "박람회", "컨벤션", "엑스포", "현수막",
])

def _is_event_booth_text(s: str) -> bool:
    """행사/부스 텍스트 판별"""
    s = _clean_text(s)
    if not s:
        return False
    return any(k in s for k in _EVENT_KEYWORDS)

def _looks_like_food_place(name: str) -> bool:
    """음식점으로 보이는지 판별"""
    if not name:
        return False
    if _is_event_booth_text(name):
        return False
    return not any(k in name for k in _NOT_FNB_KEYWORDS)

# ============================================
# 상호명 정제
# ============================================
_CORP_PREFIXES = frozenset([
    "주식회사", "(주)", "（주）", "㈜", "유한회사", "유한책임회사",
    "합자회사", "합명회사", "사단법인", "재단법인",
    "Inc.", "INC.", "Co.,Ltd", "Co.Ltd", "Ltd.", "LLC", "GmbH", "PLC", "PTE.LTD"
])

_BRANCH_TAILS = frozenset(["본점", "본사", "지점", "브랜치", "본관", "신관", "센터", "본부"])

# 정제 패턴 미리 컴파일
_PHONE_PATTERN = re.compile(r"\b(?:Tel|TEL|전화)\s*[:：]?\s*\d[\d\-]{6,}\b", re.IGNORECASE)
_NUMBER_PATTERN = re.compile(r"\b\d{2,4}[-\s]?\d{3,4}[-\s]?\d{3,4}\b")
_VERSION_PATTERN = re.compile(r"\bver\s*\d+(\.\d+)?\b", re.IGNORECASE)
_ID_PATTERN = re.compile(r"[_\-]\d{6,8}$")

def _clean_res_name(name: str) -> str:
    """상호명 정제 (최적화)"""
    if not name:
        return ""
    
    n = _clean_text(name)
    
    # "외 숫자" 패턴 제거
    n = re.sub(r"\s*외\s*\d+명?\s*$", "", n)
    
    # 구분자 처리
    if "@" in n:
        n = n.split("@")[0].strip()
    
    if "," in n and not re.search(r"\d", n.split(",")[0]):
        n = n.split(",")[0].strip()
    
    # 괄호 처리
    if "(" in n and ")" not in n:
        n = n.split("(")[0].strip()
    if "[" in n and "]" not in n:
        n = n.split("[")[0].strip()
    if ")" in n and "(" not in n:
        n = n.split(")")[0].strip()
    if "]" in n and "[" not in n:
        n = n.split("]")[0].strip()
    
    # 기업 접두사/접미사 제거
    for prefix in _CORP_PREFIXES:
        if n.startswith(prefix):
            n = n[len(prefix):].strip()
        if n.endswith(prefix):
            n = n[:-len(prefix)].strip()
    
    # 전화번호, 버전 정보 등 제거
    n = _PHONE_PATTERN.sub("", n)
    n = _NUMBER_PATTERN.sub("", n)
    n = _VERSION_PATTERN.sub("", n)
    n = _ID_PATTERN.sub("", n)
    
    # 지점 정보 제거
    for tail in _BRANCH_TAILS:
        pattern = f"[\\(\\[]\\s*{re.escape(tail)}\\s*[\\)\\]]$"
        n = re.sub(pattern, "", n, flags=re.IGNORECASE)
        pattern = f"\\s*[-–—]\\s*{re.escape(tail)}\\s*$"
        n = re.sub(pattern, "", n, flags=re.IGNORECASE)
    
    n = _normalize_spaces(n)
    
    # 유효성 검증
    if not re.search(r"[A-Za-z가-힣]", n):
        return ""
    
    return n

# ============================================
# 주소 판별
# ============================================
_ADDR_TOKENS = frozenset(["로", "길", "대로", "번길", "가", "동", "리", "로길", "시", "군", "구", "읍", "면"])

_ADDR_CORE_RE = re.compile(
    r"(?:(?:[가-힣]{1,10}(?:시|군|구))\s*)?"
    r"(?:[가-힣0-9\-]{1,20}(?:로|길|대로|번길)\s*\d{1,4}(?:-\d{1,4})?)"
    r"|"
    r"(?:[가-힣]{1,10}(?:동|리)\s*\d{1,4}(?:-\d{1,4})?(?:번지)?)"
    r"|"
    r"(?:\d{1,5}(?:-\d{1,4})?(?:번지|호|층))"
)

def _score_address(s: str) -> int:
    """주소 신뢰도 점수 계산"""
    if not s:
        return 0
    
    s = _clean_text(s)
    score = 0
    
    if _ADDR_CORE_RE.search(s):
        score += 4
    if re.search(r"(로|길|대로|번길)\s*\d{1,4}(?:-\d{1,4})?", s):
        score += 3
    if re.search(r"(특별시|광역시|자치시|도|시|군|구|읍|면|동|리)", s):
        score += 2
    if re.search(r"(번지|호|층)\b", s):
        score += 2
    if re.search(r"\d", s):
        score += 1
    
    # 비주소 키워드가 있으면 점수 감소
    if any(k in s for k in ["점", "지점", "본점", "센터", "본부", "본사", "브랜치"]):
        score -= 2
    
    return score

def _is_confident_address(s: str) -> bool:
    """주소로 확신할 수 있는지 판별"""
    return _score_address(s) >= 6

def _split_place_and_address(place_raw: str) -> Tuple[str, str]:
    """장소 텍스트를 상호명과 주소로 분리"""
    p = _clean_text(place_raw)
    if not p:
        return ("", "")
    
    if _is_event_booth_text(p):
        return ("", "")
    
    # [상호명]주소 형식
    bracket_match = re.match(r"^\[([^\]]+)\](.+)$", p)
    if bracket_match:
        name_part = _clean_text(bracket_match.group(1))
        addr_part = _clean_text(bracket_match.group(2))
        if _is_confident_address(addr_part):
            return (_clean_res_name(name_part), addr_part)
    
    # (상호명)주소 형식
    paren_match = re.match(r"^\(([^)]+)\)(.+)$", p)
    if paren_match:
        name_part = _clean_text(paren_match.group(1))
        addr_part = _clean_text(paren_match.group(2))
        if _is_confident_address(addr_part):
            return (_clean_res_name(name_part), addr_part)
    
    # 괄호로 구분된 경우
    m = re.match(r"^(.*?)[\s]*\((.+?)\)$", p)
    if m:
        left, inner = _clean_text(m.group(1)), _clean_text(m.group(2))
        if _is_confident_address(inner) and not _is_confident_address(left):
            return (_clean_res_name(left), inner)
        if _is_confident_address(left) and not _is_confident_address(inner):
            return (_clean_res_name(inner), left)
    
    # 탭이나 공백으로 구분
    for sep in ["\t", "  "]:
        if sep in p:
            parts = [x.strip() for x in p.split(sep) if x.strip()]
            if len(parts) == 2:
                a, b = parts
                if _is_confident_address(a) and not _is_confident_address(b):
                    return (_clean_res_name(b), a)
                if _is_confident_address(b) and not _is_confident_address(a):
                    return (_clean_res_name(a), b)
    
    # 구분자로 구분
    for sep in [" / ", " · ", " - ", " – "]:
        if sep in p:
            a, b = [x.strip() for x in p.split(sep, 1)]
            if _is_confident_address(a) and not _is_confident_address(b):
                return (_clean_res_name(b), a)
            if _is_confident_address(b) and not _is_confident_address(a):
                return (_clean_res_name(a), b)
    
    # 전체가 주소인 경우
    if _is_confident_address(p):
        return ("", p)
    
    # 상호명만 있는 경우
    return (_clean_res_name(p), "")

# ============================================
# 지오코딩 (Naver)
# ============================================
@lru_cache(maxsize=MAX_GEOCODE_CACHE)
def _geocode_naver_cached(keyword: str) -> Tuple[Optional[str], Optional[float], Optional[float]]:
    """네이버 지오코딩 API (캐싱)"""
    if not (NAVER_CLIENT_ID and NAVER_CLIENT_SECRET):
        return None, None, None
    
    headers = {
        "X-NCP-APIGW-API-KEY-ID": NAVER_CLIENT_ID,
        "X-NCP-APIGW-API-KEY": NAVER_CLIENT_SECRET,
    }
    
    queries = [f"{hint} {keyword}".strip() for hint in REGION_HINTS] or [keyword]
    
    for q in queries:
        try:
            r = requests.get(
                NAVER_GEOCODE_URL,
                headers=headers,
                params={"query": q},
                timeout=5  # 타임아웃 단축
            )
            
            if r.status_code != 200:
                continue
            
            data = r.json()
            if not data.get("addresses"):
                continue
            
            addr_data = data["addresses"][0]
            addr = addr_data.get("roadAddress") or addr_data.get("jibunAddress")
            x = addr_data.get("x")
            y = addr_data.get("y")
            
            if addr and x and y:
                return addr, float(y), float(x)
                
        except requests.Timeout:
            log.debug(f"Geocode timeout for: {q}")
            continue
        except Exception as e:
            log.debug(f"Geocode error for {q}: {e}")
            continue
    
    return None, None, None

# ============================================
# 파서 - 테이블 헤더 검색
# ============================================
PLACE_HEADERS = frozenset(["장소", "사용처", "지출처", "업체명", "상호", "상호명", "가맹점명", "장소/상호", "사용장소"])
PEOPLE_HEADERS = frozenset(["인원", "인원수", "사용인원", "참석인원", "대상인원"])
AMOUNT_HEADERS = frozenset([
    "금액", "승인금액", "금액(원)", "지출금액", "합계", "총액", "카드이용금액",
    "집행액", "집행액(원)", "사용금액", "사용금액(원)"
])
NAME_HEADERS = frozenset(["상호", "상호명", "업체명", "지출처", "사용처", "장소", "가맹점명", "상호/가맹점"])
ADDRESS_HEADERS = frozenset([
    "주소", "도로명주소", "지번주소", "주소(도로명)", "주소(지번)",
    "소재지", "사업장주소", "업체주소", "가맹점주소"
])

def _find_header_idx_and_cols(table: List[List[str]]) -> Tuple[Optional[int], int, int, int, int, int]:
    """테이블에서 헤더 행과 컬럼 인덱스 찾기"""
    
    def find_column_index(header_row: List[str], candidates: Set[str]) -> int:
        for idx, h in enumerate(header_row):
            h_clean = _clean_text(h).lower()
            for key in candidates:
                if key in h_clean:
                    return idx
        return -1
    
    max_scan_rows = min(30, len(table))
    
    for i in range(max_scan_rows):
        if i >= len(table):
            break
            
        row = table[i]
        header = [_clean_text(c) for c in row]
        
        # 직접 헤더 검색
        i_place = find_column_index(header, PLACE_HEADERS)
        i_people = find_column_index(header, PEOPLE_HEADERS)
        i_amt = find_column_index(header, AMOUNT_HEADERS)
        i_name = find_column_index(header, NAME_HEADERS)
        i_addr = find_column_index(header, ADDRESS_HEADERS)
        
        # 헤더를 찾았으면 반환
        if (i_place != -1 or i_name != -1 or i_addr != -1):
            log.debug(f"Header found at row {i}")
            return i, i_place, i_people, i_amt, i_name, i_addr
        
        # 셀 내용 분할하여 재검색
        exploded = []
        for cell in row:
            cell_clean = _clean_text(cell)
            if "\n" in cell_clean:
                exploded.extend(cell_clean.split("\n"))
            elif "/" in cell_clean:
                exploded.extend(cell_clean.split("/"))
            else:
                exploded.extend(re.split(r"\s{2,}", cell_clean))
        
        exploded = [_clean_text(e) for e in exploded if e]
        
        if exploded:
            i_place = find_column_index(exploded, PLACE_HEADERS)
            i_people = find_column_index(exploded, PEOPLE_HEADERS)
            i_amt = find_column_index(exploded, AMOUNT_HEADERS)
            i_name = find_column_index(exploded, NAME_HEADERS)
            i_addr = find_column_index(exploded, ADDRESS_HEADERS)
            
            if (i_place != -1 or i_name != -1 or i_addr != -1):
                log.debug(f"Header found at row {i} (exploded)")
                return i, i_place, i_people, i_amt, i_name, i_addr
    
    return None, -1, -1, -1, -1, -1

def _extract_rows_from_table(table: List[List[str]]) -> List[Tuple[str, Optional[int], Optional[int]]]:
    """테이블에서 데이터 추출"""
    rows = []
    header_idx, i_place, i_people, i_amt, i_name, i_addr = _find_header_idx_and_cols(table)
    
    if header_idx is None:
        return rows
    
    for row_idx in range(header_idx + 1, len(table)):
        row = table[row_idx]
        cells = [_clean_text(c) for c in row]
        
        # 데이터 추출
        people = cells[i_people] if 0 <= i_people < len(cells) else ""
        amount = cells[i_amt] if 0 <= i_amt < len(cells) else ""
        name_val = cells[i_name] if 0 <= i_name < len(cells) else ""
        addr_val = cells[i_addr] if 0 <= i_addr < len(cells) else ""
        
        # 장소 정보 결합
        place_raw = ""
        if name_val or addr_val:
            if name_val and addr_val:
                place_raw = f"{name_val} ({addr_val})"
            elif name_val:
                place_raw = name_val
            else:
                place_raw = addr_val
        
        if not place_raw and i_place >= 0:
            place_raw = cells[i_place] if i_place < len(cells) else ""
        
        if place_raw:
            rows.append((place_raw, _to_int(people), _to_int(amount)))
    
    return rows

# ============================================
# 파일 파서들
# ============================================
def parse_pdf_file(pdf_path: str) -> List[Tuple[str, Optional[int], Optional[int]]]:
    """PDF 파일 파싱"""
    results = []
    try:
        with pdfplumber.open(pdf_path) as pdf:
            for page_num, page in enumerate(pdf.pages):
                try:
                    tables = page.extract_tables() or []
                    for table in tables:
                        if table:
                            normalized = [[str(cell or "") for cell in row] for row in table]
                            results.extend(_extract_rows_from_table(normalized))
                except Exception as e:
                    log.debug(f"Error parsing page {page_num} in {pdf_path}: {e}")
                    continue
    except Exception as e:
        log.warning(f"Failed to parse PDF {pdf_path}: {e}")
    
    return results

def parse_csv_file(csv_path: str) -> List[Tuple[str, Optional[int], Optional[int]]]:
    """CSV 파일 파싱"""
    results = []
    try:
        with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
            reader = csv.reader(f)
            data = list(reader)
            results.extend(_extract_rows_from_table(data))
    except Exception as e:
        log.warning(f"Failed to parse CSV {csv_path}: {e}")
    
    return results

def parse_xlsx_file(xlsx_path: str) -> List[Tuple[str, Optional[int], Optional[int]]]:
    """XLSX 파일 파싱"""
    results = []
    
    if openpyxl is None:
        log.warning("openpyxl not installed, skipping .xlsx files")
        return results
    
    try:
        wb = openpyxl.load_workbook(xlsx_path, data_only=True, read_only=True)
        for ws in wb.worksheets:
            try:
                # 워크시트 데이터 추출
                data = []
                for row in ws.iter_rows(values_only=True):
                    data.append([str(cell or "") for cell in row])
                
                if data:
                    results.extend(_extract_rows_from_table(data))
            except Exception as e:
                log.debug(f"Error parsing worksheet {ws.title}: {e}")
                continue
        wb.close()
    except Exception as e:
        log.warning(f"Failed to parse XLSX {xlsx_path}: {e}")
    
    return results

def parse_xls_file(xls_path: str) -> List[Tuple[str, Optional[int], Optional[int]]]:
    """XLS 파일 파싱"""
    results = []
    
    if xlrd is None:
        log.warning("xlrd not installed, skipping .xls files")
        return results
    
    try:
        wb = xlrd.open_workbook(xls_path)
        for sheet in wb.sheets():
            data = []
            for rx in range(sheet.nrows):
                row = []
                for cx in range(sheet.ncols):
                    val = sheet.cell_value(rx, cx)
                    row.append(str(val) if val is not None else "")
                data.append(row)
            
            if data:
                results.extend(_extract_rows_from_table(data))
    except Exception as e:
        log.warning(f"Failed to parse XLS {xls_path}: {e}")
    
    return results

def parse_hwp_file(hwp_path: str) -> List[Tuple[str, Optional[int], Optional[int]]]:
    """HWP 파일 파싱"""
    results = []
    
    if olefile is None:
        log.warning("olefile not installed, skipping .hwp files")
        return results
    
    try:
        if not olefile.isOleFile(hwp_path):
            return results
        
        with olefile.OleFileIO(hwp_path) as ole:
            # 텍스트 스트림 찾기
            stream_name = None
            candidates = ['PrvText', 'PreviewText', 'PrvTextUTF']
            
            for cand in candidates:
                if ole.exists(cand):
                    stream_name = cand
                    break
                if ole.exists(f'BodyText/{cand}'):
                    stream_name = f'BodyText/{cand}'
                    break
            
            if not stream_name:
                return results
            
            data = ole.openstream(stream_name).read()
            
            # 텍스트 디코딩
            text = None
            for enc in ("cp949", "utf-16-le", "utf-8"):
                try:
                    text = data.decode(enc, errors="ignore")
                    break
                except:
                    continue
            
            if text:
                lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
                if lines:
                    table = []
                    for line in lines:
                        if "\t" in line:
                            table.append(line.split("\t"))
                        else:
                            table.append(re.split(r"\s{2,}", line))
                    
                    results.extend(_extract_rows_from_table(table))
    
    except Exception as e:
        log.warning(f"Failed to parse HWP {hwp_path}: {e}")
    
    return results

def parse_hwpx_file(hwpx_path: str) -> List[Tuple[str, Optional[int], Optional[int]]]:
    """HWPX 파일 파싱"""
    results = []
    
    try:
        with zipfile.ZipFile(hwpx_path, "r") as zf:
            xml_files = [n for n in zf.namelist() if n.lower().endswith(".xml")]
            
            all_text = []
            for xml_file in xml_files:
                try:
                    with zf.open(xml_file) as fp:
                        xml_data = fp.read()
                        root = ET.fromstring(xml_data)
                        
                        for elem in root.iter():
                            if elem.text:
                                text = _clean_text(elem.text)
                                if text:
                                    all_text.append(text)
                except:
                    continue
            
            if all_text:
                table = []
                for text in all_text:
                    if "\t" in text:
                        table.append(text.split("\t"))
                    else:
                        table.append(re.split(r"\s{2,}", text))
                
                results.extend(_extract_rows_from_table(table))
    
    except Exception as e:
        log.warning(f"Failed to parse HWPX {hwpx_path}: {e}")
    
    return results

# ============================================
# 파일 스캔 및 처리
# ============================================
SUPPORTED_EXTENSIONS = (".pdf", ".csv", ".xlsx", ".xls", ".hwp", ".hwpx")

def scan_source_files(base_dir: str) -> List[str]:
    """디렉토리에서 지원 파일 스캔"""
    files = []
    
    for root, _, filenames in os.walk(base_dir):
        for filename in filenames:
            if any(filename.lower().endswith(ext) for ext in SUPPORTED_EXTENSIONS):
                files.append(os.path.join(root, filename))
    
    return sorted(files)

def parse_file(filepath: str) -> List[Tuple[str, Optional[int], Optional[int]]]:
    """파일 확장자에 따라 적절한 파서 선택"""
    ext = os.path.splitext(filepath)[1].lower()
    
    parsers = {
        ".pdf": parse_pdf_file,
        ".csv": parse_csv_file,
        ".xlsx": parse_xlsx_file,
        ".xls": parse_xls_file,
        ".hwp": parse_hwp_file,
        ".hwpx": parse_hwpx_file,
    }
    
    parser = parsers.get(ext)
    if parser:
        return parser(filepath)
    
    return []

# ============================================
# 데이터 변환 및 검증
# ============================================
def process_extracted_rows(
    rows: List[Tuple[str, Optional[int], Optional[int]]],
    limit: Optional[int] = None
) -> List[Dict]:
    """추출된 행을 DB 레코드 형식으로 변환"""
    results = []
    seen_keys = set()
    
    for place_raw, people, amount in rows:
        # 텍스트 정제
        raw = _clean_text(place_raw)
        
        # 무효한 데이터 필터링
        if not raw or len(raw) < 2:
            continue
        
        if _is_event_booth_text(raw):
            continue
        
        # 상호명과 주소 분리
        name_clean, addr_clean = _split_place_and_address(raw)
        
        # 상호명 검증
        if name_clean:
            name_clean = _clean_res_name(name_clean)
            if len(name_clean) < 2:
                name_clean = ""
            
            if "@" in name_clean:
                name_clean = name_clean.split("@")[0].strip()
            
            if not _looks_like_food_place(name_clean):
                continue
        
        # 주소 검증
        if addr_clean:
            addr_clean = _clean_text(addr_clean)
            if not _is_confident_address(addr_clean):
                addr_clean = ""
        
        # 최소 하나는 있어야 함
        if not name_clean and not addr_clean:
            continue
        
        # 중복 체크
        key = (name_clean[:64], addr_clean[:255])
        if key in seen_keys:
            continue
        seen_keys.add(key)
        
        # 가격 계산
        price = None
        if isinstance(amount, int) and amount > 0:
            if people and people > 0:
                price = amount // people
            else:
                price = amount
            
            # 비현실적 가격 필터링
            if price > 1000000:
                price = None
        
        # 인원수 검증
        people_count = None
        if isinstance(people, int) and people > 0:
            # 비현실적 인원수 필터링 (1000명 초과)
            if people <= 1000:
                people_count = people
        
                # 지오코딩
        lat = lng = 0.0
        if addr_clean or name_clean:
            geocode_key = addr_clean or name_clean
            _, geo_lat, geo_lng = _geocode_naver_cached(geocode_key)
            
            if geo_lat and geo_lng:
                lat, lng = float(geo_lat), float(geo_lng)
        
        # 카테고리: 1순위 네이버 Local, 실패 시 이름 기반 키워드 분류
        category: Optional[str] = None

        if name_clean:
            try:
                category = _get_category_from_naver_local(
                    res_name=name_clean,
                    lat=lat,
                    lng=lng,
                    address=addr_clean,
                    radius=300,
                    debug=False,
                )
            except Exception as e:
                log.debug(f"Naver Local category lookup failed for '{name_clean}': {e}")

            # 네이버 Local에서 못 찾으면 기존 키워드 분류 사용
            if not category:
                category = _classify_category(name_clean)
        
        # 결과 추가
        results.append({
            "res_name": name_clean[:64] if name_clean else "",
            "address": addr_clean[:255] if addr_clean else "",
            "lat": lat,
            "lng": lng,
            "res_phone": None,
            "category": category,
            "price": price,
            "people_count": people_count,  # 인원수 추가
            "score": 0.0,
        })
        
        # 제한 체크
        if limit and len(results) >= limit:
            break
    
    return results

# ============================================
# 데이터베이스 작업
# ============================================
def upsert_restaurant_record(record: Dict) -> Tuple[bool, bool, Optional[str]]:
    """
    단일 레코드 업서트
    Returns: (created, updated, error_msg)
    """
    name = (record.get("res_name") or "")[:64].strip()
    addr = (record.get("address") or "")[:255].strip()
    
    # 유효성 검증
    if not name and not addr:
        return False, False, "Empty name and address"
    
    if name and len(name) < 2:
        return False, False, f"Name too short: {name}"
    
    # 재시도 로직
    for attempt in range(DB_RETRY_COUNT):
        try:
            # 기존 레코드 찾기
            existing = None
            
            if name and addr:
                existing = RestaurantInfo.query.filter_by(
                    res_name=name, 
                    address=addr
                ).first()
            elif addr:
                existing = RestaurantInfo.query.filter_by(
                    address=addr
                ).first()
            elif name:
                candidates = RestaurantInfo.query.filter_by(
                    res_name=name
                ).all()
                
                if len(candidates) == 1:
                    existing = candidates[0]
                elif len(candidates) > 1:
                    # 주소가 없는 레코드 우선
                    for c in candidates:
                        if not c.address:
                            existing = c
                            break
            
            if existing:
                # 업데이트
                updated = False
                
                # 좌표 업데이트
                new_lat = record.get("lat")
                new_lng = record.get("lng")
                if new_lat and new_lng and (new_lat != 0.0 or new_lng != 0.0):
                    if not existing.lat or not existing.lng or (existing.lat == 0.0 and existing.lng == 0.0):
                        existing.lat = float(new_lat)
                        existing.lng = float(new_lng)
                        updated = True
                
                # 가격 업데이트
                new_price = record.get("price")
                if new_price and isinstance(new_price, int) and new_price > 0:
                    if not existing.price:
                        existing.price = new_price
                        updated = True
                    elif hasattr(existing, 'price_count'):
                        # 평균 가격 계산
                        old_count = existing.price_count or 1
                        old_avg = existing.price_avg or existing.price or new_price
                        
                        existing.price_count = old_count + 1
                        existing.price_min = min(existing.price_min or new_price, new_price)
                        existing.price_max = max(existing.price_max or new_price, new_price)
                        existing.price_avg = int((old_avg * old_count + new_price) / existing.price_count)
                        existing.price = existing.price_avg
                        updated = True
                
                # 인원수 업데이트
                new_people = record.get("people_count")  # process_extracted_rows에서 people_count로 전달됨
                if new_people and isinstance(new_people, int) and new_people > 0:
                    if hasattr(existing, 'people'):
                        if not existing.people:
                            existing.people = new_people
                            updated = True
                        else:
                            # 기존 인원수가 있으면 평균 계산 (선택적)
                            # existing.people = (existing.people + new_people) // 2
                            # updated = True
                            pass  # 기존 값 유지
                
                # 카테고리 업데이트
                if record.get("category") and not existing.category:
                    existing.category = record["category"]
                    updated = True
                
                # 전화번호 업데이트
                if record.get("res_phone") and not existing.res_phone:
                    existing.res_phone = record["res_phone"]
                    updated = True
                
                if updated:
                    db.session.add(existing)
                    db.session.commit()
                    return False, True, None
                
                return False, False, None
            
            else:
                # 새 레코드 생성
                new_record = RestaurantInfo(
                    res_name=name,
                    address=addr,
                    lat=record.get("lat", 0.0),
                    lng=record.get("lng", 0.0),
                    res_phone=record.get("res_phone"),
                    category=record.get("category"),
                    price=record.get("price"),
                    score=record.get("score", 0.0)
                )
                
                # 인원수 필드 설정
                if record.get("people_count"):
                    new_record.people = record["people_count"]
                
                # 가격 통계 필드 설정
                if hasattr(RestaurantInfo, 'price_count'):
                    if record.get("price"):
                        new_record.price_min = record["price"]
                        new_record.price_max = record["price"]
                        new_record.price_avg = record["price"]
                        new_record.price_count = 1
                    else:
                        new_record.price_count = 0
                
                db.session.add(new_record)
                db.session.commit()
                return True, False, None
        
        except Exception as e:
            db.session.rollback()
            
            if attempt < DB_RETRY_COUNT - 1:
                time.sleep(DB_RETRY_DELAY)
                continue
            
            return False, False, str(e)[:100]
    
    return False, False, "Max retries exceeded"
# ============================================
# RestaurantInfo 보정용 (주소 / 상호명 채우기)
# ============================================

# 네이버 Reverse Geocode (좌표 → 주소)
NAVER_REVERSE_URL = "https://naveropenapi.apigw.ntruss.com/map-reversegeocode/v2/gc"

def _retry(func, max_retries: int = 3, delay: float = 1.0):
    """간단 재시도 유틸"""
    for attempt in range(max_retries):
        try:
            return func()
        except Exception as e:
            if attempt >= max_retries - 1:
                raise
            log.warning(f"[retry] attempt {attempt+1} failed: {e}, retrying...")
            time.sleep(delay)


def _reverse_geocode_naver(
    lat: float,
    lng: float,
    rate_limit: float = 0.1,
) -> Optional[str]:
    """
    네이버 Reverse Geocode API 로 (lat, lng) → 주소 추정
    - NAVER_CLIENT_ID / NAVER_CLIENT_SECRET 사용 (NCP 지도)
    """
    if not (NAVER_CLIENT_ID and NAVER_CLIENT_SECRET):
        return None

    # 너무 과도한 호출 방지
    if rate_limit > 0:
        time.sleep(rate_limit)

    headers = {
        "X-NCP-APIGW-API-KEY-ID": NAVER_CLIENT_ID,
        "X-NCP-APIGW-API-KEY": NAVER_CLIENT_SECRET,
    }
    params = {
        "coords": f"{lng},{lat}",
        "output": "json",
        "orders": "roadaddr,addr",
    }

    try:
        resp = requests.get(NAVER_REVERSE_URL, headers=headers, params=params, timeout=10)

        # 401 Unauthorized - 서비스 미구독
        if resp.status_code == 401:
            log.warning("[reverse] 401 Unauthorized - Reverse Geocoding API 구독 필요")
            return None

        if resp.status_code == 429:
            log.warning("[reverse] 429 Too Many Requests (rate limit)")
            return None

        if resp.status_code != 200:
            body = ""
            try:
                body = resp.text[:200]
            except Exception:
                body = "<no-body>"
            log.debug(
                f"[reverse] HTTP {resp.status_code} for ({lat}, {lng}), body={body!r}"
            )
            return None

        data = resp.json()
        results = data.get("results") or []
        if not results:
            return None

        r0 = results[0]
        region = r0.get("region") or {}
        land = r0.get("land") or {}

        parts: list[str] = []

        for key in ("area1", "area2", "area3", "area4"):
            comp = region.get(key) or {}
            nm = comp.get("name")
            if nm:
                parts.append(nm)

        if land.get("name"):
            parts.append(land["name"])

        num1 = land.get("number1")
        num2 = land.get("number2")
        if num1 and num2:
            parts.append(f"{num1}-{num2}")
        elif num1:
            parts.append(str(num1))

        if not parts:
            return None

        addr = " ".join(str(p) for p in parts if p)
        addr = _clean_text(addr)

        if not _is_confident_address(addr):
            log.debug(f"[reverse] low-confidence addr: {addr}")
            return None

        return addr

    except requests.Timeout:
        log.warning(f"[reverse] timeout for ({lat}, {lng})")
        return None
    except requests.RequestException as e:
        log.warning(f"[reverse] request error for ({lat}, {lng}): {e}")
        return None
    except Exception as e:
        log.warning(f"[reverse] unexpected error for ({lat}, {lng}): {e}")
        return None


def _guess_name_from_address(
    address: str,
    lat: float = 0.0,
    lng: float = 0.0,
    rate_limit: float = 0.1,
) -> Tuple[Optional[str], Optional[str], Optional[Tuple[float, float]]]:
    """
    주소(+좌표)를 이용해서
    - 상호명
    - 카테고리
    - 보정된 (lat, lng)
    추정 (네이버 Local 검색 활용)
    """
    addr = _clean_text(address)
    if not addr:
        return None, None, None

    if not (NAVER_LOCAL_SEARCH_CLIENT_ID and NAVER_LOCAL_SEARCH_CLIENT_SECRET):
        return None, None, None

    if rate_limit > 0:
        time.sleep(rate_limit)

    items, msg, status = _call_naver_local_search(addr)
    if status != 200 or not items:
        log.debug(f"[local] search failed for '{addr}': {msg} (status={status})")
        return None, None, None

    best = _choose_best_local_place(
        items=items,
        lat=lat,
        lng=lng,
        res_name="",      # 현재는 기존 상호명 없음 기준으로 탐색
        max_distance=500, # 500m 안쪽만
    )
    if not best:
        log.debug(f"[local] no suitable candidate for '{addr}'")
        return None, None, None

    # title → 상호명
    title = _strip_html_tags(best.get("title") or "")
    name = _clean_res_name(title)
    if not name:
        return None, None, None

    category = best.get("category")

    # Naver Local mapx/mapy → 좌표
    coords = None
    try:
        mapx = best.get("mapx")
        mapy = best.get("mapy")
        if mapx and mapy:
            cand_lng = float(mapx) / 1e7
            cand_lat = float(mapy) / 1e7
            coords = (cand_lat, cand_lng)
    except (ValueError, TypeError):
        coords = None

    return name, category, coords


def fix_missing_addresses(
    *,
    limit: int = 1000,
    dry_run: bool = False,
    batch_size: int = 100,
    rate_limit: float = 0.1,
) -> dict:
    """
    address 가 비어 있고, lat/lng 는 있는 RestaurantInfo 레코드에
    네이버 Reverse Geocode로 주소 채워넣기
    """
    if not (NAVER_CLIENT_ID and NAVER_CLIENT_SECRET):
        log.error("[repair-addr] NAVER_CLIENT_ID / NAVER_CLIENT_SECRET 없음 (Reverse 사용 불가)")
        return {"updated": 0, "skipped": 0, "errors": 0, "total": 0}

    q = (
        RestaurantInfo.query
        .filter(
            (RestaurantInfo.address == None) | (RestaurantInfo.address == "")
        )
        .filter(RestaurantInfo.lat != None, RestaurantInfo.lng != None)
        .filter((RestaurantInfo.lat != 0.0) | (RestaurantInfo.lng != 0.0))
    )

    total = q.count()
    log.info(f"[repair-addr] 대상 레코드: {total} rows (limit={limit})")

    updated = skipped = errors = 0
    processed = 0
    batch: list[RestaurantInfo] = []
    
    # 401 에러 카운터 추가
    unauthorized_count = 0
    max_unauthorized = 5  # 5번 연속 401이면 중단

    for row in q.yield_per(100):
        if limit > 0 and processed >= limit:
            break

        processed += 1
        try:
            addr = _reverse_geocode_naver(row.lat, row.lng, rate_limit=rate_limit)

            if not addr:
                # 401 에러가 지속되면 중단
                if unauthorized_count >= max_unauthorized:
                    log.error(
                        f"[repair-addr] Reverse Geocoding API 구독 필요 - "
                        f"{max_unauthorized}회 연속 실패로 중단"
                    )
                    break
                skipped += 1
                continue

            # 성공하면 401 카운터 리셋
            unauthorized_count = 0

            if dry_run:
                log.info(f"[DRY-RUN addr] ID={row.id if hasattr(row, 'id') else row} → {addr}")
            else:
                row.address = addr[:255]
                batch.append(row)
                if len(batch) >= batch_size:
                    for r in batch:
                        db.session.add(r)
                    db.session.commit()
                    batch.clear()

            updated += 1

        except Exception as e:
            # 401 관련 예외 감지
            if "401" in str(e) or "Unauthorized" in str(e):
                unauthorized_count += 1
                if unauthorized_count >= max_unauthorized:
                    log.error(
                        f"[repair-addr] Reverse Geocoding API 구독 필요 - "
                        f"{max_unauthorized}회 연속 실패로 중단"
                    )
                    break
            
            log.error(f"[repair-addr] error row={row}: {e}")
            db.session.rollback()
            errors += 1

    # 남은 배치 커밋
    if batch and not dry_run:
        try:
            for r in batch:
                db.session.add(r)
            db.session.commit()
        except Exception as e:
            log.error(f"[repair-addr] final batch commit error: {e}")
            db.session.rollback()

    log.info(
        f"[repair-addr] done. total={processed}, updated={updated}, "
        f"skipped={skipped}, errors={errors}"
    )

    return {
        "updated": updated,
        "skipped": skipped,
        "errors": errors,
        "total": processed,
    }

def fix_missing_names(
    *,
    limit: int = 1000,
    dry_run: bool = False,
    batch_size: int = 100,
    rate_limit: float = 0.1,
) -> dict:
    """
    res_name 이 비어 있고, address 는 있는 RestaurantInfo 레코드에
    네이버 Local 검색으로 상호명 / 카테고리 / 좌표 채우기
    """
    if not (NAVER_LOCAL_SEARCH_CLIENT_ID and NAVER_LOCAL_SEARCH_CLIENT_SECRET):
        log.error("[repair-name] NAVER_LOCAL_SEARCH_* 없음 (Local Search 사용 불가)")
        return {"updated": 0, "skipped": 0, "errors": 0, "total": 0}

    q = (
        RestaurantInfo.query
        .filter(
            (RestaurantInfo.res_name == None) | (RestaurantInfo.res_name == "")  # noqa: E711
        )
        .filter(RestaurantInfo.address != None, RestaurantInfo.address != "")      # noqa: E711
    )

    total = q.count()
    log.info(f"[repair-name] 대상 레코드: {total} rows (limit={limit})")

    updated = skipped = errors = 0
    processed = 0
    batch: list[RestaurantInfo] = []

    for row in q.yield_per(100):
        if limit > 0 and processed >= limit:
            break

        processed += 1
        try:
            name, category, coords = _retry(
                lambda: _guess_name_from_address(
                    address=row.address or "",
                    lat=row.lat or 0.0,
                    lng=row.lng or 0.0,
                    rate_limit=rate_limit,
                ),
                max_retries=3,
                delay=1.0,
            )

            if not name:
                skipped += 1
                continue

            if dry_run:
                log.info(
                    f"[DRY-RUN name] ID={row.id if hasattr(row, 'id') else row} "
                    f"name={name}, category={category}, coords={coords}"
                )
            else:
                row.res_name = name[:64]

                if category and not row.category:
                    row.category = category

                if coords:
                    lat2, lng2 = coords
                    # 기존 좌표가 비어있거나 0,0 이면 갱신
                    if (not row.lat or not row.lng) or (row.lat == 0.0 and row.lng == 0.0):
                        row.lat = float(lat2)
                        row.lng = float(lng2)

                batch.append(row)
                if len(batch) >= batch_size:
                    for r in batch:
                        db.session.add(r)
                    db.session.commit()
                    batch.clear()

            updated += 1

        except Exception as e:
            log.error(f"[repair-name] error row={row}: {e}")
            db.session.rollback()
            errors += 1

    # 남은 배치 커밋
    if batch and not dry_run:
        try:
            for r in batch:
                db.session.add(r)
            db.session.commit()
        except Exception as e:
            log.error(f"[repair-name] final batch commit error: {e}")
            db.session.rollback()

    log.info(
        f"[repair-name] done. total={processed}, updated={updated}, "
        f"skipped={skipped}, errors={errors}"
    )

    return {
        "updated": updated,
        "skipped": skipped,
        "errors": errors,
        "total": processed,
    }


def repair_restaurant_info(
    *,
    mode: str = "all",      # "all" / "addr" / "name"
    limit: int = 1000,
    dry_run: bool = False,
    batch_size: int = 100,
    rate_limit: float = 0.1,
) -> dict:
    """
    이 모듈 안에서 재사용할 수 있는 통합 보정 함수.

    예:
        from data_init import repair_restaurant_info

        # 주소만 500개 보정
        repair_restaurant_info(mode="addr", limit=500)

        # 상호명만 1000개 dry-run
        repair_restaurant_info(mode="name", limit=1000, dry_run=True)

        # 둘 다 전체 수행
        repair_restaurant_info(mode="all", limit=0)
    """
    mode = mode.lower()
    result_addr = result_name = None

    if mode in ("addr", "address"):
        result_addr = fix_missing_addresses(
            limit=limit,
            dry_run=dry_run,
            batch_size=batch_size,
            rate_limit=rate_limit,
        )
    elif mode in ("name", "names"):
        result_name = fix_missing_names(
            limit=limit,
            dry_run=dry_run,
            batch_size=batch_size,
            rate_limit=rate_limit,
        )
    else:  # "all"
        result_addr = fix_missing_addresses(
            limit=limit,
            dry_run=dry_run,
            batch_size=batch_size,
            rate_limit=rate_limit,
        )
        result_name = fix_missing_names(
            limit=limit,
            dry_run=dry_run,
            batch_size=batch_size,
            rate_limit=rate_limit,
        )

    return {
        "addresses": result_addr,
        "names": result_name,
    }

# ============================================
# 메인 처리 함수
# ============================================
def process_files_streaming(
    base_dir: str = PDF_BASE_DIR,
    limit: int = 0,
    batch_size: int = DEFAULT_BATCH_SIZE,
    show_progress: bool = True
) -> Dict[str, int]:
    """
    파일을 스트리밍 방식으로 처리하고 DB에 저장 (멀티프로세스 버전)

    Returns:
        통계 딕셔너리 (created, updated, skipped, errors)
    """
    # ✨ stats는 이 함수 안에서만 쓰이므로, 여기서 딱 한 번 정의
    stats: Dict[str, int] = {
        "created": 0,
        "updated": 0,
        "skipped": 0,
        "errors": 0,
        "total_processed": 0,
    }

    # 파일 스캔
    files = scan_source_files(base_dir)
    if not files:
        log.warning(f"No supported files found in {base_dir}")
        return stats

    print(f"\n🔍 Found {len(files)} files to process")

    # 전역 중복 체크용 (이것도 메인 프로세스에서만 관리)
    global_seen: Set[Tuple[str, str]] = set()

    # 진행 표시용
    start_time = time.time()
    last_report_time = start_time

    # 멀티프로세스 워커 수 결정
    max_workers = max(1, (multiprocessing.cpu_count() or 2) - 1)

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        # 각 파일에 대해 워커 제출
        future_to_file = {
            executor.submit(_worker_parse_and_process, filepath, None): filepath
            for filepath in files
        }

        # 완료된 future부터 순서대로 처리
        for idx, future in enumerate(as_completed(future_to_file), 1):
            filepath = future_to_file[future]

            try:
                records = future.result()
            except Exception as e:
                log.error(f"Failed to process {filepath}: {e}")
                stats["errors"] += 1
                continue

            if not records:
                continue

            # 전체 limit 적용
            if limit > 0:
                remaining_limit = limit - stats["total_processed"]
                if remaining_limit <= 0:
                    break
                if remaining_limit < len(records):
                    records = records[:remaining_limit]

            # 중복 제거 (이름+주소 기준)
            unique_records: List[Dict[str, Any]] = []
            for rec in records:
                key = (rec.get("res_name", ""), rec.get("address", ""))
                if key not in global_seen:
                    global_seen.add(key)
                    unique_records.append(rec)

            if not unique_records:
                continue

            # === 여기부터는 기존 DB 업서트 로직 그대로 사용 ===
            for batch_start in range(0, len(unique_records), batch_size):
                batch = unique_records[batch_start:batch_start + batch_size]

                for record in batch:
                    created, updated, error = upsert_restaurant_record(record)

                    if error:
                        stats["errors"] += 1
                        log.debug(f"Error: {error}")
                    elif created:
                        stats["created"] += 1
                    elif updated:
                        stats["updated"] += 1
                    else:
                        stats["skipped"] += 1

                    stats["total_processed"] += 1

                    # limit 초과 시 중단
                    if limit > 0 and stats["total_processed"] >= limit:
                        break

                if limit > 0 and stats["total_processed"] >= limit:
                    break

            # 진행 상황 출력
            if show_progress:
                current_time = time.time()
                if current_time - last_report_time >= 5.0:  # 5초마다
                    elapsed = current_time - start_time
                    rate = (
                        stats["total_processed"] / elapsed if elapsed > 0 else 0.0
                    )
                    print(
                        f"📊 Progress: {idx}/{len(files)} files | "
                        f"Total: {stats['total_processed']} | "
                        f"Created: {stats['created']} | "
                        f"Updated: {stats['updated']} | "
                        f"Rate: {rate:.1f}/sec"
                    )
                    last_report_time = current_time

            if limit > 0 and stats["total_processed"] >= limit:
                break

    # 최종 보고
    if show_progress:
        elapsed = time.time() - start_time
        print(f"\n✅ Processing complete in {elapsed:.1f} seconds")
        print("📈 Final stats:")
        print(f"   - Created: {stats['created']}")
        print(f"   - Updated: {stats['updated']}")
        print(f"   - Skipped: {stats['skipped']}")
        print(f"   - Errors: {stats['errors']}")
        print(f"   - Total processed: {stats['total_processed']}")

    return stats

# ============================================
# 호환성 함수들 (기존 API 유지)
# ============================================
def scan_and_upsert_streaming(
    base_dir: str = PDF_BASE_DIR,
    limit: int = 0,
    commit_every: int = 50
):
    """기존 함수 호환성 유지"""
    return process_files_streaming(
        base_dir=base_dir,
        limit=limit,
        batch_size=commit_every,
        show_progress=True
    )

def refresh_init_data_and_insert(
    base_dir: Optional[str] = None,
    limit: int = 1000
):
    """기존 함수 호환성 유지"""
    base = base_dir or PDF_BASE_DIR
    return process_files_streaming(
        base_dir=base,
        limit=limit,
        batch_size=DEFAULT_BATCH_SIZE,
        show_progress=True
    )

def refresh_init_data_and_insert_streaming(
    *,
    base_dir: Optional[str] = None,
    scan_limit: Optional[int] = None,
    limit: Optional[int] = None,
    commit_every: Optional[int] = None,
    chunk_size: Optional[int] = None,
    batch_size: Optional[int] = None,
    **kwargs
):
    """기존 함수 호환성 유지 (다양한 파라미터 지원)"""
    base = base_dir or PDF_BASE_DIR
    
    # 제한값 결정
    eff_limit = 0
    if isinstance(limit, int):
        eff_limit = max(0, limit)
    elif isinstance(scan_limit, int):
        eff_limit = max(0, scan_limit)
    
    # 배치 크기 결정
    eff_batch = DEFAULT_BATCH_SIZE
    for size in [commit_every, chunk_size, batch_size]:
        if isinstance(size, int) and size > 0:
            eff_batch = size
            break
    
    return process_files_streaming(
        base_dir=base,
        limit=eff_limit,
        batch_size=eff_batch,
        show_progress=True
    )

# ============================================
# 테스트 및 디버그 함수
# ============================================
def test_parsing(base_dir: str = PDF_BASE_DIR, max_files: int = 5):
    """파싱 테스트 (DB 저장 없이)"""
    files = scan_source_files(base_dir)[:max_files]
    
    print(f"\n🧪 Testing parsing on {len(files)} files...")
    
    for filepath in files:
        print(f"\n📄 File: {os.path.basename(filepath)}")
        
        rows = parse_file(filepath)
        print(f"   Extracted {len(rows)} rows")
        
        if rows:
            records = process_extracted_rows(rows[:5])  # 처음 5개만
            
            for i, rec in enumerate(records, 1):
                print(f"   #{i}: {rec.get('res_name', 'N/A')} @ {rec.get('address', 'N/A')}")
                if rec.get('category'):
                    print(f"        Category: {rec['category']}")
                if rec.get('price'):
                    print(f"        Price: {rec['price']:,}원")
                if rec.get('people_count'):
                    print(f"        People: {rec['people_count']}명")

def set_log_level(level: str):
    """로그 레벨 동적 변경"""
    numeric_level = getattr(logging, level.upper(), logging.WARNING)
    logging.getLogger().setLevel(numeric_level)
    log.setLevel(numeric_level)
    print(f"Log level set to: {level.upper()}")

# ============================================
# 엔트리 포인트
# ============================================
# ============================================
# 엔트리 포인트
# ============================================
if __name__ == "__main__":
    import sys
    from pathlib import Path
    
    # Flask 앱 컨텍스트 필요
    print("[INIT] Initializing Flask application context...")
    
    # .env 로드
    BASE_DIR = Path(__file__).resolve().parent
    ENV_PATH = BASE_DIR / '.env'
    
    if ENV_PATH.exists():
        from dotenv import load_dotenv
        load_dotenv(ENV_PATH, override=True)
        print(f"[INIT] ✓ .env loaded from: {ENV_PATH}")
    else:
        print(f"[INIT] ✗ .env not found at: {ENV_PATH}")
    
    # Flask 앱 생성 및 초기화
    from flask import Flask
    app = Flask(__name__)
    
    # DB 설정
    app.config['SQLALCHEMY_DATABASE_URI'] = os.environ.get("DATABASE_URL")
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
    app.config['SQLALCHEMY_ENGINE_OPTIONS'] = {
        "pool_pre_ping": True,
        "pool_recycle": 1800,
    }
    
    # DB 초기화
    db.init_app(app)
    
    print("[INIT] ✓ Flask app initialized")
    
    # 애플리케이션 컨텍스트 내에서 실행
    with app.app_context():
        print("[INIT] ✓ Application context activated")
        
        if len(sys.argv) > 1:
            command = sys.argv[1]
            
            if command == "test":
                print("[INIT] Running test mode...")
                test_parsing()
                
            elif command == "debug":
                print("[INIT] Running debug mode...")
                set_log_level("DEBUG")
                process_files_streaming(limit=10, show_progress=True)
                
            elif command == "repair":
                # repair 명령어 추가
                mode = sys.argv[2] if len(sys.argv) > 2 else "all"
                limit = int(sys.argv[3]) if len(sys.argv) > 3 else 1000
                dry_run = (sys.argv[4].lower() == "true") if len(sys.argv) > 4 else False
                
                print(f"[INIT] Running repair mode: {mode}, limit={limit}, dry_run={dry_run}")
                result = repair_restaurant_info(
                    mode=mode,
                    limit=limit,
                    dry_run=dry_run,
                )
                
                print(f"\n[REPAIR] Results:")
                if result.get('addresses'):
                    print(f"  Addresses: {result['addresses']}")
                if result.get('names'):
                    print(f"  Names: {result['names']}")
                    
            elif command == "process":
                # 전체 처리
                limit = int(sys.argv[2]) if len(sys.argv) > 2 else 0
                print(f"[INIT] Processing files with limit={limit}...")
                process_files_streaming(limit=limit, show_progress=True)
                
            else:
                print(f"Unknown command: {command}")
                print("Available commands:")
                print("  test          - Test parsing")
                print("  debug         - Debug mode with limited processing")
                print("  repair [mode] [limit] [dry_run] - Repair restaurant data")
                print("  process [limit] - Process all files")
                
        else:
            # 기본 실행: 전체 처리
            print("[INIT] Processing all files...")
            process_files_streaming(show_progress=True)
            
        print("[INIT] ✓ Complete")