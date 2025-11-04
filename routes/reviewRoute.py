from flask import Blueprint, request, jsonify
from services.reviewService import (
    create_review_service,
    get_reviews_by_restaurant_service,
    get_review_detail_service,
    update_review_service,
    delete_review_service,
    get_restaurant_review_summary_service
)

review_bp = Blueprint("review", __name__)

# 리뷰 생성
@review_bp.route("/reviews", methods=["POST"])
def create_review():
    data = request.json
    result, msg, status = create_review_service(data)
    if status != 201:
        return jsonify({"message": msg}), status
    return jsonify({"message": msg, "data": result}), status

# 특정 식당의 리뷰 목록
@review_bp.route("/reviews/restaurant/<int:res_id>", methods=["GET"])
def get_reviews_by_restaurant(res_id):
    page = request.args.get("page", default=1, type=int)
    per_page = request.args.get("per_page", default=20, type=int)
    order = request.args.get("order", default="recent", type=str)

    result, msg, status = get_reviews_by_restaurant_service(res_id, page, per_page, order)
    if status != 200:
        return jsonify({"message": msg}), status
    return jsonify(result), status

# 리뷰 상세
@review_bp.route("/reviews/<int:review_id>", methods=["GET"])
def get_review_detail(review_id):
    result, msg, status = get_review_detail_service(review_id)
    if status != 200:
        return jsonify({"message": msg}), status
    return jsonify({"message": msg, "data": result}), status

# 리뷰 수정
@review_bp.route("/reviews/<int:review_id>", methods=["PATCH"])
def update_review(review_id):
    data = request.json
    actor_user_id = request.args.get("actor_user_id", type=int)
    is_admin = bool(request.args.get("is_admin", default=0, type=int))

    result, msg, status = update_review_service(
        review_id,
        data,
        actor_user_id=actor_user_id,
        is_admin=is_admin
    )
    if status != 200:
        return jsonify({"message": msg}), status
    return jsonify({"message": msg, "data": result}), status

# 리뷰 삭제
@review_bp.route("/reviews/<int:review_id>", methods=["DELETE"])
def delete_review(review_id):
    actor_user_id = request.args.get("actor_user_id", type=int)
    is_admin = bool(request.args.get("is_admin", default=0, type=int))

    result, msg, status = delete_review_service(
        review_id,
        actor_user_id=actor_user_id,
        is_admin=is_admin
    )
    if status != 200:
        return jsonify({"message": msg}), status
    return jsonify({"message": msg}), status

# 리뷰 요약(평균/개수/분포)
@review_bp.route("/reviews/restaurant/<int:res_id>/summary", methods=["GET"])
def get_restaurant_review_summary(res_id):
    result, msg, status = get_restaurant_review_summary_service(res_id)
    if status != 200:
        return jsonify({"message": msg}), status
    return jsonify({"message": msg, "data": result}), status
