from flask import Blueprint, request, jsonify
from services.visitService import (
    create_visit_service,
    get_visits_by_restaurant_service,
    get_visits_in_range_service,
    get_visit_detail_service,
    update_visit_service,
    delete_visit_service,
    get_visit_counts_by_day_service,
)

# 이후 사용자 인증이 필요한 API에는 @login_required를 붙여 사용 가능

visit_bp = Blueprint("visit", __name__)

# 방문 생성
@visit_bp.route("/visits", methods=["POST"])
def create_visit():
    data = request.json
    result, msg, status = create_visit_service(data)
    if status != 201:
        return jsonify({"message": msg}), status
    return jsonify({"message": msg, "data": result}), status

# 특정 식당의 방문 목록(최근 방문순 / oldest 지원)
@visit_bp.route("/visits/restaurant/<int:res_id>", methods=["GET"])
def list_visits_by_restaurant(res_id):
    page = request.args.get("page", default=1, type=int)
    per_page = request.args.get("per_page", default=20, type=int)
    order = request.args.get("order", default="recent", type=str)

    result, msg, status = get_visits_by_restaurant_service(res_id, page, per_page, order)
    if status != 200:
        return jsonify({"message": msg}), status
    return jsonify(result), status

# 기간별 방문 조회(옵션: 특정 식당)
@visit_bp.route("/visits/range", methods=["GET"])
def list_visits_in_range():
    start_date = request.args.get("start_date", type=str)  # YYYY-MM-DD
    end_date = request.args.get("end_date", type=str)      # YYYY-MM-DD
    res_id = request.args.get("res_id", type=int)
    page = request.args.get("page", default=1, type=int)
    per_page = request.args.get("per_page", default=20, type=int)

    result, msg, status = get_visits_in_range_service(start_date, end_date, res_id, page, per_page)
    if status != 200:
        return jsonify({"message": msg}), status
    return jsonify(result), status

# 방문 상세
@visit_bp.route("/visits/<int:vi_id>", methods=["GET"])
def get_visit_detail(vi_id):
    result, msg, status = get_visit_detail_service(vi_id)
    if status != 200:
        return jsonify({"message": msg}), status
    return jsonify({"message": msg, "data": result}), status

# 방문 수정 (관리자 전용 가정)
@visit_bp.route("/visits/<int:vi_id>", methods=["PATCH"])
def update_visit(vi_id):
    data = request.json
    is_admin = bool(request.args.get("is_admin", default=0, type=int))

    result, msg, status = update_visit_service(vi_id, data, is_admin=is_admin)
    if status != 200:
        return jsonify({"message": msg}), status
    return jsonify({"message": msg, "data": result}), status

# 방문 삭제 (관리자 전용 가정)
@visit_bp.route("/visits/<int:vi_id>", methods=["DELETE"])
def delete_visit(vi_id):
    is_admin = bool(request.args.get("is_admin", default=0, type=int))

    result, msg, status = delete_visit_service(vi_id, is_admin=is_admin)
    if status != 200:
        return jsonify({"message": msg}), status
    return jsonify({"message": msg}), status

# 방문 집계(최근 N일, 일자별 카운트)
@visit_bp.route("/visits/restaurant/<int:res_id>/counts", methods=["GET"])
def get_visit_counts_by_day(res_id):
    days = request.args.get("days", default=30, type=int)
    result, msg, status = get_visit_counts_by_day_service(res_id, days=days)
    if status != 200:
        return jsonify({"message": msg}), status
    return jsonify({"message": msg, "data": result}), status
