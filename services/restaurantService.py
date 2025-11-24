# services/restaurantService.py

import os
import math
import re
import requests
from sqlalchemy import func

from models import db, RestaurantInfo

# ✅ Naver Local Search API 설정 - 환경변수에서 가져오기
NAVER_CLIENT_ID = os.environ.get("NAVER_CLIENT_ID", "")
NAVER_CLIENT_SECRET = os.environ.get("NAVER_CLIENT_SECRET", "")
NAVER_LOCAL_URL = "https://openapi.naver.com/v1/search/local.json"


def _strip_html_tags(text: str) -> str:
    if not text:
        return ""
    # <b>...</b> 같은 태그 제거
    return re.sub(r"<[^>]*>", "", text)


def _haversine(lat1, lng1, lat2, lng2):
    """
    두 위도/경도 사이 거리(m) 계산 (Haversine formula)
    """
    R = 6371000  # meters
    rad = math.radians

    dlat = rad(lat2 - lat1)
    dlng = rad(lng2 - lng1)

    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(rad(lat1)) * math.cos(rad(lat2)) * math.sin(dlng / 2) ** 2
    )
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c


def _call_naver_local(res_name: str, display: int = 5):
    """
    네이버 검색 > 지역 API 호출
    https://openapi.naver.com/v1/search/local.json?query=...
    """
    if not NAVER_CLIENT_ID or not NAVER_CLIENT_SECRET:
        return None, "NAVER_CLIENT_ID / NAVER_CLIENT_SECRET 환경변수가 필요합니다.", 500

    headers = {
        "X-Naver-Client-Id": NAVER_CLIENT_ID,
        "X-Naver-Client-Secret": NAVER_CLIENT_SECRET,
    }

    params = {
        "query": res_name,
        "display": min(max(display, 1), 5),
        "start": 1,
        "sort": "random",  # 정확도순
    }

    try:
        resp = requests.get(
            NAVER_LOCAL_URL, headers=headers, params=params, timeout=5
        )
    except Exception as e:
        return None, f"Naver Local API 요청 실패: {e}", 502

    if resp.status_code != 200:
        return None, f"Naver Local API 오류: HTTP {resp.status_code}", 502

    data = resp.json()
    items = data.get("items", [])
    if not items:
        return None, "검색 결과가 없습니다.", 404

    return items, "ok", 200


def _choose_best_place(items, lat: float, lng: float, res_name: str, max_distance: int = 300):
    """
    Naver Local 결과 중에서
    - 우리 좌표와 가장 가까우면서
    - 상호명이 비슷한 후보 선택

    mapx/mapy는 WGS84 * 10^7 형식
    """
    target_name = (res_name or "").lower()
    best = None  # (item, dist, name_score, lat, lng)

    for it in items:
        mapx = it.get("mapx")
        mapy = it.get("mapy")
        if not mapx or not mapy:
            continue

        try:
            # "1269873882" → 126.9873882
            lng2 = float(mapx) / 1e7
            lat2 = float(mapy) / 1e7
        except ValueError:
            continue

        dist = _haversine(lat, lng, lat2, lng2)
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
            best = (it, dist, name_score, lat2, lng2)
            continue

        _, prev_dist, prev_score, *_ = best

        # 이름 매칭 점수가 높을수록 우선, 같으면 더 가까운 것
        if name_score > prev_score or (name_score == prev_score and dist < prev_dist):
            best = (it, dist, name_score, lat2, lng2)

    if best is None:
        return None, None, None, None

    item, dist_m, _, best_lat, best_lng = best
    return item, dist_m, best_lat, best_lng


def fetch_restaurant_detail_from_naver_service(data: dict):
    """
    입력(JSON body):
      {
        "res_name": "상호명",   # 필수
        "lat": 37.xxx,         # 필수
        "lng": 126.xxx,        # 필수
        "radius": 300          # 선택(기본 300m)
      }

    동작:
      - Naver Local Search로 가게 후보 검색
      - 좌표 기준으로 가장 가까운 후보 선택
      - RestaurantInfo에 upsert

    리턴:
      (result_dict | None, message: str, status_code: int)
    """
    res_name = (data.get("res_name") or "").strip()
    lat = data.get("lat")
    lng = data.get("lng")
    radius = data.get("radius", 300)

    if not res_name:
        return None, "res_name는 필수입니다.", 400

    try:
        lat = float(lat)
        lng = float(lng)
    except (TypeError, ValueError):
        return None, "lat/lng가 올바른 숫자가 아닙니다.", 400

    # 1) 네이버 지역 검색 호출
    items, msg, status = _call_naver_local(res_name)
    if status != 200:
        return None, msg, status

    # 2) 최적 후보 선택
    best_item, dist_m, place_lat, place_lng = _choose_best_place(
        items, lat, lng, res_name, max_distance=radius
    )
    if not best_item:
        return None, "조건(반경) 안에서 가게를 찾지 못했습니다.", 404

    # 3) 필드 파싱
    title = _strip_html_tags(best_item.get("title") or "")
    category = best_item.get("category") or None
    phone = best_item.get("telephone") or None
    road_addr = best_item.get("roadAddress") or ""
    jibun_addr = best_item.get("address") or ""
    address = road_addr or jibun_addr or ""

    # 4) RestaurantInfo upsert
    existing = (
        RestaurantInfo.query
        .filter(
            func.lower(RestaurantInfo.res_name) == func.lower(title),
            RestaurantInfo.address == address,
        )
        .first()
    )

    created = False
    if existing is None:
        ri = RestaurantInfo(
            res_name=title,
            address=address,
            lat=place_lat,
            lng=place_lng,
            res_phone=phone,
            category=category,
            price=None,
            score=None,
            price_min=None,
            price_max=None,
            price_avg=None,
            price_count=0,
        )
        db.session.add(ri)
        created = True
    else:
        existing.res_name = title
        existing.address = address
        existing.lat = place_lat
        existing.lng = place_lng
        existing.res_phone = phone
        existing.category = category
        ri = existing

    try:
        db.session.commit()
    except Exception as e:
        db.session.rollback()
        return None, f"DB 저장 중 오류: {e}", 500

    result = {
        "res_id": ri.res_id,
        "res_name": ri.res_name,
        "address": ri.address,
        "lat": ri.lat,
        "lng": ri.lng,
        "res_phone": ri.res_phone,
        "category": ri.category,
        "price": ri.price,
        "score": ri.score,
        "price_min": ri.price_min,
        "price_max": ri.price_max,
        "price_avg": ri.price_avg,
        "price_count": ri.price_count,
        "distance_m": dist_m,
        # 필요하면 raw 확인용
        "naver_raw": best_item,
    }

    if created:
        return result, "새 RestaurantInfo로 저장되었습니다. (Naver)", 201
    else:
        return result, "기존 RestaurantInfo가 갱신되었습니다. (Naver)", 200