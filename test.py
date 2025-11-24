"""
네이버 Local API를 사용한 카테고리 업데이트 전용 스크립트
- 서버 사이드 커서 문제 해결
- 메모리 효율적인 배치 처리
- 개선된 에러 핸들링 및 진행 상황 표시
"""

import os
import sys
import time
import math
import logging
from pathlib import Path
from typing import Optional, Dict, List, Tuple, Any
from dataclasses import dataclass
from contextlib import contextmanager
from dotenv import load_dotenv

# ============================================
# 환경 설정
# ============================================
BASE_DIR = Path(__file__).resolve().parent
ENV_PATH = BASE_DIR / '.env'

if ENV_PATH.exists():
    load_dotenv(ENV_PATH, override=True)
    print(f"[INIT] ✓ .env loaded from: {ENV_PATH}")
else:
    print(f"[INIT] ✗ .env not found at: {ENV_PATH}")

# 로깅 설정
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger(__name__)

# 환경 변수
NAVER_LOCAL_SEARCH_CLIENT_ID = os.getenv("NAVER_LOCAL_SEARCH_CLIENT_ID")
NAVER_LOCAL_SEARCH_CLIENT_SECRET = os.getenv("NAVER_LOCAL_SEARCH_CLIENT_SECRET")
NAVER_LOCAL_URL = "https://openapi.naver.com/v1/search/local.json"

# ============================================
# Flask 앱 및 DB 초기화
# ============================================
from flask import Flask
from models import db, RestaurantInfo

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = os.environ.get("DATABASE_URL")
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['SQLALCHEMY_ENGINE_OPTIONS'] = {
    "pool_pre_ping": True,
    "pool_recycle": 1800,
    "pool_size": 10,
    "max_overflow": 20,
}
db.init_app(app)

# ============================================
# 데이터 클래스
# ============================================
@dataclass
class UpdateStats:
    """업데이트 통계"""
    total: int = 0
    updated: int = 0
    skipped: int = 0
    errors: int = 0
    api_success: int = 0
    api_fail: int = 0
    not_found: int = 0  # API는 성공했지만 검색 결과 없음
    parse_fail: int = 0  # 카테고리 파싱 실패
    
    def to_dict(self) -> Dict[str, int]:
        return {
            "total": self.total,
            "updated": self.updated,
            "skipped": self.skipped,
            "errors": self.errors,
            "api_success": self.api_success,
            "api_fail": self.api_fail,
            "not_found": self.not_found,
            "parse_fail": self.parse_fail,
        }


@dataclass
class CategoryUpdate:
    """카테고리 업데이트 정보"""
    res_id: int
    old_category: Optional[str]
    new_category: str


# ============================================
# 네이버 Local API 관련 함수들
# ============================================
import requests
import re

# 카테고리 캐시
_LOCAL_CATEGORY_CACHE: Dict[Tuple[str, str], Optional[str]] = {}


def haversine(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    """두 위도/경도 사이 거리(m) 계산"""
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


def _strip_html_tags(text: str) -> str:
    """HTML 태그 제거"""
    if not text:
        return ""
    return re.sub(r"<[^>]*>", "", text)


def _parse_address_components(address: str) -> Dict[str, str]:
    """
    주소에서 주요 구성요소 추출
    
    예: "경기도 화성시 동탄순환대로 567-31" 
        → {"sido": "경기", "sigungu": "화성시", "detail": "동탄순환대로"}
    """
    if not address:
        return {}
    
    parts = address.strip().split()
    components = {}
    
    # 시/도 추출
    if parts:
        sido = parts[0]
        # "경기도" → "경기", "서울특별시" → "서울"
        if sido.endswith('도'):
            components['sido'] = sido[:-1]
        elif sido.endswith('특별시') or sido.endswith('광역시'):
            components['sido'] = sido.replace('특별시', '').replace('광역시', '')
        else:
            components['sido'] = sido
    
    # 시/군/구 추출
    if len(parts) > 1:
        sigungu = parts[1]
        components['sigungu'] = sigungu
        
        # 구/동 정보가 있으면 추가
        if len(parts) > 2:
            # "송산동", "동탄2동" 같은 동 정보
            if parts[2].endswith('동') or parts[2].endswith('읍') or parts[2].endswith('면'):
                components['dong'] = parts[2]
            else:
                # 도로명이나 건물명
                components['detail'] = parts[2]
    
    return components


def _build_search_query(res_name: str, address: str) -> str:
    """
    상호명과 주소로 최적의 검색 쿼리 생성
    
    전략:
    1. 기본: "상호명 + 주요 주소 정보" (너무 길지 않게)
    2. 주소에서 불필요한 상세 정보 제거
    3. 검색에 유용한 키워드만 추출
    """
    query_parts = []
    
    # 상호명 추가 (필수)
    if res_name:
        query_parts.append(res_name.strip())
    
    if not address:
        return " ".join(query_parts)
    
    # 주소 파싱
    addr_components = _parse_address_components(address)
    
    # 주소에서 검색에 유용한 부분만 추출
    # 예: "경기도 화성시 동탄순환대로 567-31" → "화성시 동탄순환대로"
    addr_parts = address.strip().split()
    useful_parts = []
    
    for part in addr_parts:
        # 시/군/구는 항상 포함
        if part.endswith('시') or part.endswith('군') or part.endswith('구'):
            useful_parts.append(part)
        # 동/읍/면도 포함
        elif part.endswith('동') or part.endswith('읍') or part.endswith('면'):
            useful_parts.append(part)
        # 주요 도로명 포함 (로, 대로, 길로 끝나는 것)
        elif part.endswith('로') or part.endswith('대로') or part.endswith('길'):
            useful_parts.append(part)
            break  # 도로명 이후는 보통 번지수이므로 중단
        # "경기도", "서울특별시" 같은 시/도는 제외 (너무 넓음)
    
    # 최대 3개 부분만 사용 (너무 길면 검색 실패 가능)
    query_parts.extend(useful_parts[:3])
    
    return " ".join(query_parts)


def _call_naver_local_search(query: str, display: int = 10) -> Tuple[Optional[List[Dict[str, Any]]], str, int]:
    """
    네이버 지역 검색 API 호출
    """
    if not NAVER_LOCAL_SEARCH_CLIENT_ID or not NAVER_LOCAL_SEARCH_CLIENT_SECRET:
        return None, "NAVER_LOCAL_SEARCH_CLIENT_ID / NAVER_LOCAL_SEARCH_CLIENT_SECRET 필요", 500

    headers = {
        "X-Naver-Client-Id": NAVER_LOCAL_SEARCH_CLIENT_ID,
        "X-Naver-Client-Secret": NAVER_LOCAL_SEARCH_CLIENT_SECRET,
    }

    params = {
        "query": query,
        "display": min(max(display, 1), 10),  # 최대 10개
        "start": 1,
        "sort": "random",
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
    address: str = "",
    max_distance: int = 1000,
) -> Optional[Dict[str, Any]]:
    """
    검색 결과 중 가장 적합한 장소 선택
    
    우선순위:
    1. 상호명 정확도 (가장 중요)
    2. 주소 유사도
    3. 거리 (1km 이내, 좌표 있을 때만)
    """
    target_name = (res_name or "").lower()
    target_addr = (address or "").lower()
    
    # 좌표 유효성 체크
    has_valid_coords = (lat != 0.0 or lng != 0.0) and lat is not None and lng is not None
    
    best: Optional[Tuple[Dict[str, Any], int, int, float]] = None  # (item, name_score, addr_score, dist)

    for it in items:
        title = _strip_html_tags(it.get("title") or "")
        lower_title = title.lower()
        
        item_addr = (it.get("address") or "").lower()
        
        # 거리 계산 (좌표가 있는 경우)
        dist = 0.0
        if has_valid_coords:
            mapx = it.get("mapx")
            mapy = it.get("mapy")
            if mapx and mapy:
                try:
                    cand_lng = float(mapx) / 1e7
                    cand_lat = float(mapy) / 1e7
                    dist = haversine(lat, lng, cand_lat, cand_lng)
                    
                    # 1km 이상 떨어진 곳은 제외
                    if dist > max_distance:
                        continue
                except (ValueError, TypeError):
                    dist = 0.0

        # 상호명 매칭 점수
        name_score = 0
        if target_name:
            if target_name in lower_title:
                name_score = 10  # 정확히 포함
            elif lower_title in target_name:
                name_score = 8   # 부분 일치
            else:
                # 키워드 일부라도 포함되면 점수
                keywords = target_name.split()
                matched = sum(1 for kw in keywords if len(kw) >= 2 and kw in lower_title)
                if matched > 0:
                    name_score = 5 + matched
        
        # 주소 매칭 점수
        addr_score = 0
        if target_addr and item_addr:
            # 시/군/구 일치 확인
            target_parts = target_addr.split()
            
            # 공통 지역명 찾기 (시, 구, 동 등)
            common_regions = 0
            for tp in target_parts:
                if len(tp) >= 2 and (tp.endswith('시') or tp.endswith('구') or tp.endswith('동')):
                    if tp in item_addr:
                        common_regions += 1
            
            addr_score = common_regions * 3
            
            # 도로명이나 건물명 일치도 체크
            for tp in target_parts:
                if len(tp) >= 3 and (tp.endswith('로') or tp.endswith('길') or tp.endswith('대로')):
                    if tp in item_addr:
                        addr_score += 2

        # 매칭 점수가 너무 낮으면 스킵
        total_score = name_score + addr_score
        if total_score < 5:
            continue

        if best is None:
            best = (it, name_score, addr_score, dist)
            continue

        _, prev_name_score, prev_addr_score, prev_dist = best
        prev_total = prev_name_score + prev_addr_score
        
        # 총점이 높으면 선택
        if total_score > prev_total:
            best = (it, name_score, addr_score, dist)
        # 총점 같으면 거리가 가까운 것 선택
        elif total_score == prev_total and has_valid_coords and dist < prev_dist:
            best = (it, name_score, addr_score, dist)

    if best is None:
        return None
    return best[0]


def _get_category_from_naver_local(
    res_name: str,
    lat: float,
    lng: float,
    address: str = "",
    radius: int = 1000,
    debug: bool = False,
) -> Optional[str]:
    """
    네이버 지역검색에서 카테고리 가져오기 (캐싱 포함)
    """
    name_key = (res_name or "").strip()
    addr_key = (address or "").strip()
    cache_key = (name_key, addr_key)

    # 캐시 확인
    if cache_key in _LOCAL_CATEGORY_CACHE:
        return _LOCAL_CATEGORY_CACHE[cache_key]

    # 개선된 검색 쿼리 생성
    query = _build_search_query(res_name, address)
    
    if not query:
        _LOCAL_CATEGORY_CACHE[cache_key] = None
        return None

    if debug:
        log.debug(f"[LOCAL] query='{query}', center=({lat}, {lng}), radius={radius}m")

    # API 호출
    items, msg, status = _call_naver_local_search(query)
    if status != 200 or not items:
        if debug:
            log.debug(f"[LOCAL] 검색 실패: {msg} (status={status})")
        _LOCAL_CATEGORY_CACHE[cache_key] = None
        return None

    # 최적 장소 선택 (주소 정보 포함)
    best_item = _choose_best_local_place(
        items, lat, lng, name_key, 
        address=address,
        max_distance=radius
    )
    if not best_item:
        if debug:
            log.debug("[LOCAL] 적절한 후보 없음")
        _LOCAL_CATEGORY_CACHE[cache_key] = None
        return None

    category = best_item.get("category")
    if debug:
        title = _strip_html_tags(best_item.get("title") or "")
        log.debug(f"[LOCAL] 선택된 가게: {title}, category={category}")

    _LOCAL_CATEGORY_CACHE[cache_key] = category
    return category


# 카테고리 매핑 (네이버 → 시스템)
CATEGORY_MAPPING = {
    # 메인 카테고리
    "한식": "한식",
    "중식": "중식",
    "일식": "일식",
    "양식": "양식",
    "아시안": "아시안",
    "퓨전": "양식",
    
    # 카페/디저트
    "카페": "카페",
    "디저트": "디저트",
    "커피": "카페",
    "커피숍": "카페",
    "커피전문점": "카페",
    "베이커리": "베이커리",
    "제과": "베이커리",
    "제빵": "베이커리",
    "빵집": "베이커리",
    "도넛": "베이커리",
    "아이스크림": "디저트",
    "빙수": "디저트",
    
    # 특화 카테고리
    "치킨": "치킨",
    "피자": "피자",
    "패스트푸드": "패스트푸드",
    "햄버거": "패스트푸드",
    "분식": "분식",
    
    # 주점
    "주점": "주점",
    "술집": "주점",
    "호프": "주점",
    "bar": "주점",
    "바": "주점",
    "이자카야": "주점",
    "포장마차": "주점",
    
    # 고기
    "고기": "고기",
    "육류": "고기",
    "구이": "고기",
    "소고기": "고기",
    "돼지고기": "고기",
    "삼겹살": "고기",
    "갈비": "고기",
    
    # 해산물
    "해산물": "해산물",
    "수산": "해산물",
    "회": "해산물",
    "생선": "해산물",
    "조개": "해산물",
    
    # 뷔페
    "뷔페": "뷔페",
    "부페": "뷔페",
    "buffet": "뷔페",
}


def _parse_naver_category(raw_category: str) -> Optional[str]:
    """
    네이버 Local API 카테고리 정리
    예: "한식>베이커리" → "한식"
        "음식점>카페,디저트>카페" → "카페"
        "음식점>일식>초밥,롤" → "일식"
    """
    if not raw_category:
        return None
    
    # '>' 로 분리된 카테고리 계층 구조
    parts = raw_category.split('>')
    
    # 각 파트를 순회하면서 매칭
    for part in parts:
        part = part.strip().lower()
        
        # 쉼표로 구분된 하위 카테고리 처리
        if ',' in part:
            sub_parts = part.split(',')
            for sub in sub_parts:
                sub = sub.strip()
                for key, value in CATEGORY_MAPPING.items():
                    if key.lower() in sub:
                        return value
        else:
            # 직접 매칭
            for key, value in CATEGORY_MAPPING.items():
                if key.lower() in part:
                    return value
    
    # 매칭 실패시 첫 번째 의미있는 카테고리 반환
    for part in parts:
        part = part.strip()
        if part and part not in ["음식점", "restaurant", "식당"]:
            # 쉼표 있으면 첫 번째만
            if ',' in part:
                part = part.split(',')[0].strip()
            return part[:10]  # 최대 10자
    
    return None


# ============================================
# 배치 처리 관련
# ============================================
@contextmanager
def batch_processor(batch_size: int = 100):
    """배치 처리를 위한 컨텍스트 매니저"""
    batch = []
    
    def add(item):
        batch.append(item)
        if len(batch) >= batch_size:
            yield batch[:]
            batch.clear()
    
    try:
        yield add
        # 남은 배치 처리
        if batch:
            yield batch[:]
    finally:
        batch.clear()


def fetch_target_ids(force: bool = False) -> List[int]:
    """
    처리 대상 레코드의 ID 목록을 먼저 가져옴
    이렇게 하면 yield_per() 커서 문제를 피할 수 있음
    """
    if force:
        # 강제 모드: 모든 레코드
        q = RestaurantInfo.query.filter(
            RestaurantInfo.res_name != None,
            RestaurantInfo.res_name != ""
        )
    else:
        # 일반 모드: 카테고리 없는 레코드만
        q = RestaurantInfo.query.filter(
            (RestaurantInfo.category == None) | (RestaurantInfo.category == ""),
            RestaurantInfo.res_name != None,
            RestaurantInfo.res_name != ""
        )
    
    # res_id만 가져오기 (primary key)
    result = q.with_entities(RestaurantInfo.res_id).all()
    return [row[0] for row in result]


def process_batch(
    ids: List[int],
    radius: int,
    dry_run: bool,
    stats: UpdateStats,
    rate_limit: float = 0.1,
    show_sample: int = 0,
    verbose: bool = False,
) -> List[CategoryUpdate]:
    """
    ID 배치 처리
    
    Args:
        verbose: True면 모든 변경 사항을 실시간 출력
    """
    updates = []
    
    for idx, res_id in enumerate(ids):
        try:
            # 레코드 조회 (SQLAlchemy 2.0 호환)
            row = db.session.get(RestaurantInfo, res_id)
            if not row:
                stats.skipped += 1
                continue
            
            stats.total += 1
            
            # API 호출 제한
            if rate_limit > 0:
                time.sleep(rate_limit)
            
            # verbose 모드에서 검색 쿼리 표시
            if verbose and idx < 5:
                search_query = _build_search_query(row.res_name, row.address or "")
                print(f"  [{stats.total:4d}] 검색: '{search_query}'")
            
            # 네이버 Local API 호출
            raw_category = _get_category_from_naver_local(
                res_name=row.res_name,
                lat=row.lat or 0.0,
                lng=row.lng or 0.0,
                address=row.address or "",
                radius=radius,
                debug=False,
            )
            
            if not raw_category:
                stats.api_fail += 1
                stats.not_found += 1
                stats.skipped += 1
                if show_sample > 0 and idx < show_sample:
                    log.info(
                        f"[{'DRY-RUN' if dry_run else 'PROCESS'}] "
                        f"ID={row.res_id} name={row.res_name} → 검색 결과 없음"
                    )
                if verbose and idx < 10:  # verbose 모드에서 처음 10개만 표시
                    print(f"         → ❌ 검색 결과 없음")
                continue
            
            stats.api_success += 1
            
            # 카테고리 정리
            clean_category = _parse_naver_category(raw_category)
            
            if not clean_category:
                stats.parse_fail += 1
                stats.skipped += 1
                if show_sample > 0 and idx < show_sample:
                    log.info(
                        f"[{'DRY-RUN' if dry_run else 'PROCESS'}] "
                        f"ID={row.res_id} name={row.res_name} → "
                        f"raw={raw_category}, parsed=None (파싱 실패)"
                    )
                if verbose and idx < 10:
                    print(f"         → ⚠️  raw={raw_category} (파싱 실패)")
                continue
            
            # 변경 내용 실시간 표시
            if verbose:
                old_cat = row.category or "(없음)"
                status = "→" if row.category != clean_category else "="
                if idx < 5:
                    print(f"         → ✅ {old_cat:10s} {status} {clean_category:10s}")
                else:
                    print(f"  [{stats.total:4d}] {row.res_name:20s} | {old_cat:10s} {status} {clean_category:10s}")
            
            # 샘플 로그
            if show_sample > 0 and idx < show_sample:
                log.info(
                    f"[{'DRY-RUN' if dry_run else 'PROCESS'}] "
                    f"ID={row.res_id} name={row.res_name} → "
                    f"raw={raw_category}, parsed={clean_category}"
                )
            
            if dry_run:
                stats.updated += 1
            else:
                # 업데이트 정보 저장
                updates.append(CategoryUpdate(
                    res_id=row.res_id,
                    old_category=row.category,
                    new_category=clean_category
                ))
                stats.updated += 1
        
        except Exception as e:
            log.error(f"[process_batch] ID={res_id} 처리 오류: {e}")
            stats.errors += 1
    
    return updates


def apply_updates(updates: List[CategoryUpdate], verbose: bool = False) -> int:
    """
    카테고리 업데이트를 DB에 적용
    """
    applied = 0
    
    try:
        for update in updates:
            row = db.session.get(RestaurantInfo, update.res_id)
            if row:
                old = row.category
                row.category = update.new_category
                
                if verbose and old and old != update.new_category:
                    print(f"  [UPDATE] ID={update.res_id:5d} | {old:10s} → {update.new_category:10s}")
                elif verbose:
                    print(f"  [NEW]    ID={update.res_id:5d} | (없음)    → {update.new_category:10s}")
                
                if old and old != update.new_category:
                    log.debug(
                        f"[apply_updates] ID={update.res_id} "
                        f"{old} → {update.new_category}"
                    )
                
                applied += 1
        
        db.session.commit()
        return applied
    
    except Exception as e:
        log.error(f"[apply_updates] 커밋 오류: {e}")
        db.session.rollback()
        raise


def format_progress(
    processed: int,
    total: int,
    stats: UpdateStats,
    elapsed: float,
) -> str:
    """진행 상황 포맷팅"""
    rate = processed / elapsed if elapsed > 0 else 0.0
    progress_pct = (processed / total * 100) if total > 0 else 0
    
    return (
        f"📊 진행: {processed:,}/{total:,} ({progress_pct:.1f}%) | "
        f"✅업데이트: {stats.updated:,} | "
        f"❌검색실패: {stats.not_found:,} | "
        f"⚠️파싱실패: {stats.parse_fail:,} | "
        f"💥오류: {stats.errors:,} | "
        f"속도: {rate:.1f}/초"
    )


# ============================================
# 메인 업데이트 함수
# ============================================
def update_categories_from_naver_local(
    *,
    limit: int = 0,
    dry_run: bool = False,
    batch_size: int = 100,
    rate_limit: float = 0.1,
    radius: int = 1000,
    force: bool = False,
    verbose: bool = False,
) -> Dict[str, int]:
    """
    네이버 Local API를 사용하여 카테고리 업데이트
    
    Args:
        limit: 처리할 최대 레코드 수 (0이면 전체 처리)
        dry_run: True면 실제 DB 업데이트 없이 시뮬레이션만
        batch_size: DB 커밋 배치 크기
        rate_limit: API 호출 간 대기 시간 (초)
        radius: Local API 검색 반경 (미터)
        force: True면 기존 카테고리도 덮어씀
        verbose: True면 모든 변경 사항을 실시간 출력
    
    Returns:
        dict: {"updated": int, "skipped": int, "errors": int, "total": int, 
               "api_success": int, "api_fail": int}
    """
    
    # API 크레덴셜 체크
    if not (NAVER_LOCAL_SEARCH_CLIENT_ID and NAVER_LOCAL_SEARCH_CLIENT_SECRET):
        log.error("[update-category] NAVER_LOCAL_SEARCH_* 환경변수 필요")
        print("\n❌ 오류: 네이버 Local API 크레덴셜이 설정되지 않았습니다.")
        print("   .env 파일에 다음 변수를 설정하세요:")
        print("   - NAVER_LOCAL_SEARCH_CLIENT_ID")
        print("   - NAVER_LOCAL_SEARCH_CLIENT_SECRET")
        return UpdateStats().to_dict()
    
    # 1단계: 대상 ID 목록 가져오기
    print("\n🔍 대상 레코드 조회 중...")
    target_ids = fetch_target_ids(force=force)
    total_records = len(target_ids)
    
    # 전체 레코드 수도 조회
    all_records_count = RestaurantInfo.query.count()
    
    log.info(f"[update-category] 대상 레코드: {total_records} rows (force={force})")
    
    if total_records == 0:
        print("\n✅ 처리할 레코드가 없습니다.")
        print(f"   (전체 레코드: {all_records_count:,}개)")
        log.info("[update-category] 처리할 레코드가 없습니다.")
        return UpdateStats().to_dict()
    
    # limit 적용
    if limit > 0:
        target_ids = target_ids[:limit]
        total_to_process = limit
    else:
        total_to_process = total_records
    
    # 통계 초기화
    stats = UpdateStats()
    
    print(f"\n🏷️  카테고리 업데이트 시작 (네이버 Local API)")
    print(f"   - 전체 레코드: {all_records_count:,}개")
    print(f"   - 카테고리 없음: {total_records:,}개")
    print(f"   - 처리할 개수: {total_to_process:,}개")
    print(f"   - 배치 크기: {batch_size:,}개")
    print(f"   - 검색 반경: {radius}m")
    print(f"   - 강제 모드: {'예' if force else '아니오'}")
    print(f"   - Dry-run: {'예' if dry_run else '아니오'}")
    print(f"   - 상세 출력: {'예' if verbose else '아니오'}")
    print()
    
    if verbose:
        print("=" * 80)
        print("변경 내용:")
        print("=" * 80)
    
    start_time = time.time()
    last_report_time = start_time
    
    # 2단계: 배치 처리
    show_sample = 20 if dry_run else 0
    
    for i in range(0, len(target_ids), batch_size):
        batch_ids = target_ids[i:i + batch_size]
        
        # 배치 처리
        updates = process_batch(
            ids=batch_ids,
            radius=radius,
            dry_run=dry_run,
            stats=stats,
            rate_limit=rate_limit,
            show_sample=show_sample if i == 0 else 0,  # 첫 배치만 샘플 표시
            verbose=verbose,
        )
        
        # DB 업데이트 (dry_run이 아닐 때만)
        if not dry_run and updates:
            try:
                applied = apply_updates(updates, verbose=verbose)
                log.debug(f"[update-category] 배치 커밋: {applied}개 업데이트")
            except Exception as e:
                log.error(f"[update-category] 배치 커밋 실패: {e}")
                stats.errors += len(updates)
        
        # 진행 상황 출력 (5초마다 또는 verbose 모드가 아닐 때)
        current_time = time.time()
        if not verbose and current_time - last_report_time >= 5.0:
            elapsed = current_time - start_time
            print(format_progress(stats.total, total_to_process, stats, elapsed))
            last_report_time = current_time
        
        # limit 도달 시 중단
        if limit > 0 and stats.total >= limit:
            break
    
    elapsed = time.time() - start_time
    
    if verbose:
        print("=" * 80)
    
    # 최종 결과 출력
    print(f"\n✅ 카테고리 업데이트 완료 ({elapsed:.1f}초)")
    print(f"   - 처리됨: {stats.total:,}")
    print(f"   - ✅ 업데이트 성공: {stats.updated:,}")
    print(f"   - ❌ 검색 결과 없음: {stats.not_found:,}")
    print(f"   - ⚠️  카테고리 파싱 실패: {stats.parse_fail:,}")
    print(f"   - 💥 시스템 오류: {stats.errors:,}")
    print(f"   - 건너뜀 (총): {stats.skipped:,}")
    if stats.total > 0:
        success_rate = (stats.updated / stats.total * 100)
        print(f"   - 성공률: {success_rate:.1f}%")
    
    # 실패 원인 분석
    if stats.not_found > 0 or stats.parse_fail > 0:
        print(f"\n💡 참고:")
        if stats.not_found > 0:
            print(f"   - 검색 결과 없음 ({stats.not_found}개): 상호명이나 주소가 부정확하거나")
            print(f"     네이버에 등록되지 않은 업소일 수 있습니다.")
            print(f"     → --radius 값을 늘려보거나 --force로 재시도해보세요.")
        if stats.parse_fail > 0:
            print(f"   - 파싱 실패 ({stats.parse_fail}개): 네이버 카테고리를 우리 시스템 카테고리로")
            print(f"     변환할 수 없는 경우입니다. 카테고리 매핑 규칙을 추가할 수 있습니다.")
    
    log.info(
        f"[update-category] 완료. "
        f"total={stats.total}, updated={stats.updated}, "
        f"skipped={stats.skipped}, errors={stats.errors}, "
        f"not_found={stats.not_found}, parse_fail={stats.parse_fail}"
    )
    
    return stats.to_dict()


# ============================================
# 메인 실행
# ============================================
if __name__ == "__main__":
    print("[INIT] 카테고리 업데이트 스크립트 시작...")
    print("[INIT] ✓ Flask app initialized")
    
    # 기본 설정
    limit = 0
    dry_run = False
    force = False
    radius = 1000  # 기본 1km
    batch_size = 100
    verbose = False
    
    # 명령줄 인자 파싱
    for arg in sys.argv[1:]:
        if arg.isdigit():
            limit = int(arg)
        elif arg == "--dry-run":
            dry_run = True
        elif arg == "--force":
            force = True
        elif arg == "--verbose" or arg == "-v":
            verbose = True
        elif arg.startswith("--radius="):
            radius = int(arg.split("=")[1])
        elif arg.startswith("--batch="):
            batch_size = int(arg.split("=")[1])
        elif arg in ["-h", "--help"]:
            print("\n사용법: python update_categories.py [옵션]")
            print("\n옵션:")
            print("  숫자           처리할 최대 레코드 수 (예: 1000)")
            print("  --dry-run      실제 업데이트 없이 시뮬레이션만")
            print("  --force        기존 카테고리도 덮어쓰기")
            print("  --verbose, -v  모든 변경 사항을 실시간 출력")
            print("  --radius=N     검색 반경 설정 (미터, 기본값: 1000)")
            print("  --batch=N      배치 크기 설정 (기본값: 100)")
            print("  -h, --help     도움말 표시")
            print("\n예시:")
            print("  python update_categories.py")
            print("  python update_categories.py 100 --dry-run --verbose")
            print("  python update_categories.py --force --batch=50")
            print("  python update_categories.py 1000 -v --radius=2000")
            print("\n참고:")
            print("  - 카테고리가 없는 레코드만 업데이트합니다")
            print("  - 주소 기반 검색으로 1km 반경 내에서 매칭합니다")
            print("  - 좌표가 정확하지 않아도 주소로 검색합니다")
            sys.exit(0)
    
    # Flask 앱 컨텍스트 내에서 실행
    with app.app_context():
        print("[INIT] ✓ Application context activated")
        
        result = update_categories_from_naver_local(
            limit=limit,
            dry_run=dry_run,
            force=force,
            radius=radius,
            batch_size=batch_size,
            verbose=verbose,
        )
        
        print(f"\n[FINAL] 최종 결과:")
        print(f"   Total: {result['total']:,}")
        print(f"   ✅ Updated: {result['updated']:,}")
        print(f"   ❌ Not Found: {result['not_found']:,}")
        print(f"   ⚠️  Parse Failed: {result['parse_fail']:,}")
        print(f"   💥 Errors: {result['errors']:,}")
        print(f"   Skipped: {result['skipped']:,}")
        
        print("\n[INIT] ✓ Complete")