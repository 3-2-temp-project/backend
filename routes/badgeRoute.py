from flask import Blueprint, request, jsonify
from services.badgeService import (
    create_badge_service,
    get_badges_by_restaurant_service,
    get_badge_detail_service,
    update_badge_service,
    delete_badge_service,
)

# 이후 사용자 인증이 필요한 API에는 @login_required를 붙여 사용 가능

badge_bp = Blueprint("badge", __name__)

# 뱃지 생성
@badge_bp.route("/badges", methods=["POST"])
def create_badge():
    data = request.json
    result, msg, status = create_badge_service(data)
    if status != 201:
        return jsonify({"message": msg}), status
    return jsonify({"message": msg, "data": result}), status

# 특정 식당의 뱃지 목록(최근 발급순)
@badge_bp.route("/badges/restaurant/<int:res_id>", methods=["GET"])
def list_badges_by_restaurant(res_id):
    page = request.args.get("page", default=1, type=int)
    per_page = request.args.get("per_page", default=20, type=int)

    result, msg, status = get_badges_by_restaurant_service(res_id, page, per_page)
    if status != 200:
        return jsonify({"message": msg}), status
    return jsonify(result), status

# 뱃지 상세
@badge_bp.route("/badges/<int:badge_id>", methods=["GET"])
def get_badge_detail(badge_id):
    result, msg, status = get_badge_detail_service(badge_id)
    if status != 200:
        return jsonify({"message": msg}), status
    return jsonify({"message": msg, "data": result}), status

# 뱃지 수정 (관리자 전용 가정)
@badge_bp.route("/badges/<int:badge_id>", methods=["PATCH"])
def update_badge(badge_id):
    data = request.json
    is_admin = bool(request.args.get("is_admin", default=0, type=int))

    result, msg, status = update_badge_service(badge_id, data, is_admin=is_admin)
    if status != 200:
        return jsonify({"message": msg}), status
    return jsonify({"message": msg, "data": result}), status

# 뱃지 삭제 (관리자 전용 가정)
@badge_bp.route("/badges/<int:badge_id>", methods=["DELETE"])
def delete_badge(badge_id):
    is_admin = bool(request.args.get("is_admin", default=0, type=int))

    result, msg, status = delete_badge_service(badge_id, is_admin=is_admin)
    if status != 200:
        return jsonify({"message": msg}), status
    return jsonify({"message": msg}), status
