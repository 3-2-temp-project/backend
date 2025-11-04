from flask import Blueprint, request, jsonify
from services.suggestionService import (
    create_suggestion_service,
    get_suggestions_recent_service,
    get_suggestions_by_user_service,
    search_suggestions_service,
    get_suggestion_detail_service,
    update_suggestion_service,
    delete_suggestion_service,
)

# 이후 사용자 인증이 필요한 API에는 @login_required를 붙여 사용 가능

suggestion_bp = Blueprint("suggestion", __name__)

# 제보 생성
@suggestion_bp.route("/suggestions", methods=["POST"])
def create_suggestion():
    data = request.json
    result, msg, status = create_suggestion_service(data)
    if status != 201:
        return jsonify({"message": msg}), status
    return jsonify({"message": msg, "data": result}), status

# 제보 목록(최근순)
@suggestion_bp.route("/suggestions", methods=["GET"])
def list_suggestions_recent():
    page = request.args.get("page", default=1, type=int)
    per_page = request.args.get("per_page", default=20, type=int)

    result, msg, status = get_suggestions_recent_service(page, per_page)
    if status != 200:
        return jsonify({"message": msg}), status
    return jsonify(result), status

# 특정 사용자 제보 목록
@suggestion_bp.route("/suggestions/user/<int:user_id>", methods=["GET"])
def list_suggestions_by_user(user_id):
    page = request.args.get("page", default=1, type=int)
    per_page = request.args.get("per_page", default=20, type=int)

    result, msg, status = get_suggestions_by_user_service(user_id, page, per_page)
    if status != 200:
        return jsonify({"message": msg}), status
    return jsonify(result), status

# 제보 검색(상호/주소)
@suggestion_bp.route("/suggestions/search", methods=["GET"])
def search_suggestions():
    q = request.args.get("q", type=str)
    page = request.args.get("page", default=1, type=int)
    per_page = request.args.get("per_page", default=20, type=int)

    result, msg, status = search_suggestions_service(q, page, per_page)
    if status != 200:
        return jsonify({"message": msg}), status
    return jsonify(result), status

# 제보 상세
@suggestion_bp.route("/suggestions/<int:suggestion_id>", methods=["GET"])
def get_suggestion_detail(suggestion_id):
    result, msg, status = get_suggestion_detail_service(suggestion_id)
    if status != 200:
        return jsonify({"message": msg}), status
    return jsonify({"message": msg, "data": result}), status

# 제보 수정
@suggestion_bp.route("/suggestions/<int:suggestion_id>", methods=["PATCH"])
def update_suggestion(suggestion_id):
    data = request.json
    actor_user_id = request.args.get("actor_user_id", type=int)
    is_admin = bool(request.args.get("is_admin", default=0, type=int))

    result, msg, status = update_suggestion_service(
        suggestion_id,
        data,
        actor_user_id=actor_user_id,
        is_admin=is_admin,
    )
    if status != 200:
        return jsonify({"message": msg}), status
    return jsonify({"message": msg, "data": result}), status

# 제보 삭제
@suggestion_bp.route("/suggestions/<int:suggestion_id>", methods=["DELETE"])
def delete_suggestion(suggestion_id):
    actor_user_id = request.args.get("actor_user_id", type=int)
    is_admin = bool(request.args.get("is_admin", default=0, type=int))

    result, msg, status = delete_suggestion_service(
        suggestion_id,
        actor_user_id=actor_user_id,
        is_admin=is_admin,
    )
    if status != 200:
        return jsonify({"message": msg}), status
    return jsonify({"message": msg}), status
