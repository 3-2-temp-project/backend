from flask import Blueprint, request, jsonify
from services.locationService import (
    set_location_service,
    get_restaurant_markers_service,
    get_restaurants_nearby_service,
    geocode_address_service
)

location_bp = Blueprint("location", __name__)

# 위치 설정
@location_bp.route("/location", methods=["POST"])
def set_location():
    data = request.json
    result, msg, status = set_location_service(data)
    if status != 200:
        return jsonify({"message": msg}), status
    return jsonify({"message": msg, "data": result}), status

# 주소 받아와 위치 설정
@location_bp.route("/location/address", methods=["POST"])
def set_location_by_address():
    data = request.json
    address = data.get("address")

    result, msg, status = geocode_address_service(address)
    if status != 200:
        return jsonify({"message": msg}), status
    return jsonify({"message": msg, "data": result}), status

# 모든 지도 마커 조회
@location_bp.route("/restaurants/markers", methods=["GET"])
def get_restaurant_markers():
    result, status = get_restaurant_markers_service()
    return jsonify(result), status

from services.locationService import (
    set_location_service,
    get_restaurant_markers_service,
    get_restaurants_nearby_service,
    get_restaurant_detail_by_coords_service,   
)

# 마커 클릭 → 식당 상세 조회
@location_bp.route("/restaurant/detail", methods=["GET"])
def get_restaurant_detail_by_coords():
    lat = request.args.get("lat", type=float)
    lng = request.args.get("lng", type=float)

    result, msg, status = get_restaurant_detail_by_coords_service(lat, lng)
    if status != 200:
        return jsonify({"message": msg}), status

    return jsonify({"message": msg, "data": result}), 200



@location_bp.route("/restaurants/nearby", methods=["GET"])
def get_restaurants_nearby():
    radius = request.args.get("radius", default=3, type=float)
    result, status = get_restaurants_nearby_service(radius)
    if status != 200:
        return jsonify({"message": result}), status
    return jsonify(result), status
