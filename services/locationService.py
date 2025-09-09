from flask import session
from sqlalchemy import func, literal
from models import db, RestaurantInfo
import math

def set_location_service(data):
    lat = data.get("lat")
    lng = data.get("lng")
    if lat is None or lng is None:
        return None, "위도(lat)와 경도(lng)는 필수입니다.", 400
    try:
        lat = float(lat)
        lng = float(lng)
    except:
        return None, "위도/경도는 숫자여야 합니다.", 400
    if not (-90 <= lat <= 90 and -180 <= lng <= 180):
        return None, "위도/경도 범위가 올바르지 않습니다.", 400
    session["lat"] = lat
    session["lng"] = lng
    return {"lat": lat, "lng": lng}, "위치 저장 완료", 200

def get_restaurant_markers_service(limit=None):
    q = RestaurantInfo.query
    if limit:
        q = q.limit(limit)
    result = [
        {
            "res_id": r.res_id,
            "res_name": r.res_name,
            "lat": r.lat,
            "lng": r.lng,
            "address": r.address,
            "category": r.category,
            "score": r.score,
        }
        for r in q.all()
    ]
    return result, 200

def get_restaurant_detail_by_coords_service(lat, lng, tolerance=0.00005):
    if lat is None or lng is None:
        return None, "위도와 경도 필수", 400
    try:
        lat = float(lat)
        lng = float(lng)
    except:
        return None, "숫자여야 함", 400
    r = (
        RestaurantInfo.query
        .filter(RestaurantInfo.lat.between(lat - tolerance, lat + tolerance))
        .filter(RestaurantInfo.lng.between(lng - tolerance, lng + tolerance))
        .first()
    )
    if not r:
        return None, "해당 위치의 식당 없음", 404
    return {
        "res_id": r.res_id,
        "res_name": r.res_name,
        "address": r.address,
        "lat": r.lat,
        "lng": r.lng,
        "category": r.category,
        "price": r.price,
        "score": r.score,
        "res_phone": r.res_phone,
    }, "식당 정보 조회 성공", 200


# 하버사인 거리(km)
def _haversine_km(lat1, lng1, lat2, lng2):
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlng = math.radians(lng2 - lng1)
    a = (math.sin(dlat/2)**2
         + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2))
         * math.sin(dlng/2)**2)
    c = 2 * math.asin(math.sqrt(a))
    return R * c

def get_restaurants_nearby_service(lat, lng, radius_km=3.0, limit=200):
    # 입력 검증
    if lat is None or lng is None:
        return "위도/경도는 필수입니다.", 400
    try:
        lat = float(lat); lng = float(lng); radius_km = float(radius_km)
    except:
        return "위도/경도/반경은 숫자여야 합니다.", 400
    if not (-90 <= lat <= 90 and -180 <= lng <= 180):
        return "위도/경도 범위가 올바르지 않습니다.", 400
    if radius_km <= 0:
        return "반경(km)은 0보다 커야 합니다.", 400

    # 1차: 바운딩 박스(위도 1도 ≈ 111km, 경도는 위도에 따라 변함)
    lat_delta = radius_km / 111.0
    lng_delta = radius_km / (111.0 * max(1e-6, math.cos(math.radians(lat))))

    candidates = (RestaurantInfo.query
                  .filter(RestaurantInfo.lat.between(lat - lat_delta, lat + lat_delta))
                  .filter(RestaurantInfo.lng.between(lng - lng_delta, lng + lng_delta))
                  .limit(limit * 5)   # 후보를 넉넉히
                  .all())

    # 2차: 파이썬에서 정확 거리 계산 + 반경 필터 + 거리순 정렬
    results = []
    for r in candidates:
        d = _haversine_km(lat, lng, r.lat, r.lng)
        if d <= radius_km:
            results.append({
                "res_id": r.res_id,
                "res_name": r.res_name,
                "address": r.address,
                "lat": r.lat,
                "lng": r.lng,
                "category": r.category,
                "price": r.price,
                "score": r.score,
                "res_phone": r.res_phone,
                "distance_km": round(d, 3),
            })

    results.sort(key=lambda x: x["distance_km"])
    # 최종 상한
    results = results[:limit]

    return results, 200