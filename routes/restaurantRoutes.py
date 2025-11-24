# routes/restaurantRoutes.py

from flask import Blueprint, request, jsonify
from services.restaurantService import fetch_restaurant_detail_from_naver_service
from models import db, RestaurantInfo
from sqlalchemy import func, or_

restaurant_bp = Blueprint("restaurant", __name__)


# ✅ 좌표 + 상호명 기반 상세정보 가져오기 (Naver Local)
@restaurant_bp.route("/fetch-detail", methods=["POST"])
def fetch_restaurant_detail():
    """
    네이버 Local API를 통해 음식점 상세정보를 가져와 DB에 저장
    
    Request Body:
    {
        "res_name": "음식점명",
        "lat": 37.xxx,
        "lng": 126.xxx,
        "radius": 300  # 선택, 기본 300m
    }
    """
    try:
        data = request.json or {}

        result, msg, status = fetch_restaurant_detail_from_naver_service(data)

        if status not in (200, 201):
            return jsonify({"message": msg}), status

        return jsonify({
            "message": msg,
            "data": result,
        }), status

    except Exception as e:
        return jsonify({"message": "음식점 정보 조회 실패", "error": str(e)}), 500


# ✅ 음식점 목록 조회 (검색/필터링)
@restaurant_bp.route("/", methods=["GET"])
def get_restaurants():
    """
    음식점 목록 조회 (검색, 필터링, 페이징)
    
    Query Parameters:
    - search: 음식점명 또는 주소 검색
    - category: 카테고리 필터
    - min_score: 최소 평점
    - max_price: 최대 가격
    - lat, lng, radius: 위치 기반 검색 (반경 미터)
    - page: 페이지 번호 (기본 1)
    - per_page: 페이지당 항목 수 (기본 20, 최대 100)
    - sort: 정렬 기준 (score_desc, price_asc, price_desc, name_asc, distance)
    """
    try:
        # 쿼리 파라미터 파싱
        search = request.args.get('search', '').strip()
        category = request.args.get('category', '').strip()
        min_score = request.args.get('min_score', type=float)
        max_price = request.args.get('max_price', type=int)
        
        # 위치 기반 검색
        lat = request.args.get('lat', type=float)
        lng = request.args.get('lng', type=float)
        radius = request.args.get('radius', 1000, type=int)  # 기본 1km
        
        # 페이징
        page = request.args.get('page', 1, type=int)
        per_page = min(request.args.get('per_page', 20, type=int), 100)
        
        # 정렬
        sort = request.args.get('sort', 'name_asc')

        # 쿼리 빌드
        query = RestaurantInfo.query

        # 검색 필터
        if search:
            search_pattern = f"%{search}%"
            query = query.filter(
                or_(
                    RestaurantInfo.res_name.ilike(search_pattern),
                    RestaurantInfo.address.ilike(search_pattern)
                )
            )

        # 카테고리 필터
        if category:
            query = query.filter(RestaurantInfo.category.ilike(f"%{category}%"))

        # 평점 필터
        if min_score is not None:
            query = query.filter(RestaurantInfo.score >= min_score)

        # 가격 필터
        if max_price is not None:
            query = query.filter(
                or_(
                    RestaurantInfo.price_avg <= max_price,
                    RestaurantInfo.price_avg.is_(None)
                )
            )

        # ✅ 위치 기반 검색인 경우 별도 처리
        if lat is not None and lng is not None and sort == 'distance':
            # 좌표가 있는 것만 먼저 필터링
            query = query.filter(
                RestaurantInfo.lat.isnot(None),
                RestaurantInfo.lng.isnot(None)
            )
            
            # 모든 결과를 가져와서 거리 계산
            all_restaurants = query.all()
            
            from services.restaurantService import _haversine
            restaurants_with_distance = []
            
            for r in all_restaurants:
                distance = _haversine(lat, lng, r.lat, r.lng)
                if distance <= radius:
                    restaurants_with_distance.append({
                        "restaurant": r,
                        "distance": distance
                    })
            
            # 거리순 정렬
            restaurants_with_distance.sort(key=lambda x: x['distance'])
            
            # 페이징 직접 처리
            total = len(restaurants_with_distance)
            pages = (total + per_page - 1) // per_page
            start = (page - 1) * per_page
            end = start + per_page
            
            paginated_items = restaurants_with_distance[start:end]
            
            # 결과 포맷팅
            restaurants = []
            for item in paginated_items:
                r = item["restaurant"]
                restaurants.append({
                    "res_id": r.res_id,
                    "res_name": r.res_name,
                    "address": r.address,
                    "lat": r.lat,
                    "lng": r.lng,
                    "res_phone": r.res_phone,
                    "category": r.category,
                    "price": r.price,
                    "score": r.score,
                    "price_min": r.price_min,
                    "price_max": r.price_max,
                    "price_avg": r.price_avg,
                    "price_count": r.price_count,
                    "distance_m": round(item["distance"], 2),
                })
            
            return jsonify({
                "restaurants": restaurants,
                "pagination": {
                    "page": page,
                    "per_page": per_page,
                    "total": total,
                    "pages": pages,
                    "has_next": page < pages,
                    "has_prev": page > 1,
                }
            }), 200
        
        # ✅ 일반 검색 (위치 기반이 아닌 경우)
        else:
            # 정렬
            if sort == 'score_desc':
                query = query.order_by(RestaurantInfo.score.desc().nullslast())
            elif sort == 'price_asc':
                query = query.order_by(RestaurantInfo.price_avg.asc().nullslast())
            elif sort == 'price_desc':
                query = query.order_by(RestaurantInfo.price_avg.desc().nullslast())
            elif sort == 'name_asc':
                query = query.order_by(RestaurantInfo.res_name.asc())
            elif sort == 'name_desc':
                query = query.order_by(RestaurantInfo.res_name.desc())
            else:
                query = query.order_by(RestaurantInfo.res_id.desc())

            # 페이징 실행
            pagination = query.paginate(
                page=page,
                per_page=per_page,
                error_out=False
            )

            # 결과 포맷팅
            restaurants = []
            for r in pagination.items:
                restaurant_data = {
                    "res_id": r.res_id,
                    "res_name": r.res_name,
                    "address": r.address,
                    "lat": r.lat,
                    "lng": r.lng,
                    "res_phone": r.res_phone,
                    "category": r.category,
                    "price": r.price,
                    "score": r.score,
                    "price_min": r.price_min,
                    "price_max": r.price_max,
                    "price_avg": r.price_avg,
                    "price_count": r.price_count,
                }
                restaurants.append(restaurant_data)

            return jsonify({
                "restaurants": restaurants,
                "pagination": {
                    "page": pagination.page,
                    "per_page": pagination.per_page,
                    "total": pagination.total,
                    "pages": pagination.pages,
                    "has_next": pagination.has_next,
                    "has_prev": pagination.has_prev,
                }
            }), 200

    except Exception as e:
        return jsonify({"message": "음식점 목록 조회 실패", "error": str(e)}), 500


# ✅ 특정 음식점 상세 조회
@restaurant_bp.route("/<int:res_id>", methods=["GET"])
def get_restaurant_detail(res_id):
    """
    특정 음식점의 상세 정보 조회
    """
    try:
        restaurant = db.session.get(RestaurantInfo, res_id)
        
        if not restaurant:
            return jsonify({"message": "음식점을 찾을 수 없습니다."}), 404

        return jsonify({
            "restaurant": {
                "res_id": restaurant.res_id,
                "res_name": restaurant.res_name,
                "address": restaurant.address,
                "lat": restaurant.lat,
                "lng": restaurant.lng,
                "res_phone": restaurant.res_phone,
                "category": restaurant.category,
                "price": restaurant.price,
                "score": restaurant.score,
                "price_min": restaurant.price_min,
                "price_max": restaurant.price_max,
                "price_avg": restaurant.price_avg,
                "price_count": restaurant.price_count,
            }
        }), 200

    except Exception as e:
        return jsonify({"message": "음식점 조회 실패", "error": str(e)}), 500


# ✅ 주변 음식점 검색 (위치 기반)
@restaurant_bp.route("/nearby", methods=["GET"])
def get_nearby_restaurants():
    """
    현재 위치 기반 주변 음식점 검색
    
    Query Parameters:
    - lat: 위도 (필수)
    - lng: 경도 (필수)
    - radius: 반경(m, 기본 1000)
    - limit: 최대 결과 수 (기본 20)
    - category: 카테고리 필터
    """
    try:
        lat = request.args.get('lat', type=float)
        lng = request.args.get('lng', type=float)
        radius = request.args.get('radius', 1000, type=int)
        limit = min(request.args.get('limit', 20, type=int), 100)
        category = request.args.get('category', '').strip()

        if lat is None or lng is None:
            return jsonify({"message": "lat, lng 파라미터가 필요합니다."}), 400

        # DB에서 음식점 조회
        query = RestaurantInfo.query.filter(
            RestaurantInfo.lat.isnot(None),
            RestaurantInfo.lng.isnot(None)
        )

        if category:
            query = query.filter(RestaurantInfo.category.ilike(f"%{category}%"))

        restaurants = query.all()

        # 거리 계산 및 필터링
        from services.restaurantService import _haversine
        nearby = []
        
        for r in restaurants:
            distance = _haversine(lat, lng, r.lat, r.lng)
            if distance <= radius:
                nearby.append({
                    "res_id": r.res_id,
                    "res_name": r.res_name,
                    "address": r.address,
                    "lat": r.lat,
                    "lng": r.lng,
                    "res_phone": r.res_phone,
                    "category": r.category,
                    "price": r.price,
                    "score": r.score,
                    "price_avg": r.price_avg,
                    "distance_m": round(distance, 2),
                })

        # 거리순 정렬
        nearby.sort(key=lambda x: x['distance_m'])
        
        # limit 적용
        nearby = nearby[:limit]

        return jsonify({
            "restaurants": nearby,
            "count": len(nearby),
            "search_radius": radius,
            "center": {"lat": lat, "lng": lng}
        }), 200

    except Exception as e:
        return jsonify({"message": "주변 음식점 검색 실패", "error": str(e)}), 500


# ✅ 음식점 통계 조회
@restaurant_bp.route("/stats", methods=["GET"])
def get_restaurant_stats():
    """
    음식점 전체 통계
    """
    try:
        total = db.session.query(func.count(RestaurantInfo.res_id)).scalar()
        
        avg_score = db.session.query(
            func.avg(RestaurantInfo.score)
        ).filter(RestaurantInfo.score.isnot(None)).scalar()
        
        avg_price = db.session.query(
            func.avg(RestaurantInfo.price_avg)
        ).filter(RestaurantInfo.price_avg.isnot(None)).scalar()

        # 카테고리별 개수
        categories = db.session.query(
            RestaurantInfo.category,
            func.count(RestaurantInfo.res_id).label('count')
        ).filter(
            RestaurantInfo.category.isnot(None)
        ).group_by(
            RestaurantInfo.category
        ).order_by(
            func.count(RestaurantInfo.res_id).desc()
        ).limit(10).all()

        return jsonify({
            "total_restaurants": total,
            "average_score": round(float(avg_score), 2) if avg_score else None,
            "average_price": round(float(avg_price), 2) if avg_price else None,
            "top_categories": [
                {"category": cat, "count": cnt}
                for cat, cnt in categories
            ]
        }), 200

    except Exception as e:
        return jsonify({"message": "통계 조회 실패", "error": str(e)}), 500