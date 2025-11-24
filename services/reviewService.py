from flask import session
import requests
from sqlalchemy import func, literal
from models import db, Review, RestaurantInfo, User
import os
import math


# 공통: 평점 검증(1~5)
def _validate_rating(v):
    try:
        v = int(v)
    except Exception:
        return None, "rating은 1~5 정수여야 합니다."
    if not (1 <= v <= 5):
        return None, "rating은 1~5 범위여야 합니다."
    return v, None


# 공통: 식당 평균 평점 갱신
def _recalc_restaurant_score(res_id):
    avg = db.session.query(func.avg(Review.rating)).filter(Review.res_id == res_id).scalar()
    r = RestaurantInfo.query.get(res_id)
    if r:
        r.score = float(avg) if avg is not None else None
        db.session.add(r)
        db.session.commit()


# 공통: 리뷰 직렬화(닉네임 포함)
def _serialize_review(r: Review):
    # 혹시 과거 데이터에서 user_nickname 이 비어 있으면 User 테이블에서 한 번 더 가져오기
    nickname = r.user_nickname
    if nickname is None:
        user = User.query.get(r.user_id)
        if user:
            nickname = user.user_nickname

    return {
        "id": r.id,
        "res_id": r.res_id,
        "user_id": r.user_id,
        "user_nickname": nickname,
        "content": r.content,
        "rating": r.rating,
        "photo_url": r.photo_url,
        "created_at": r.created_at.isoformat() if r.created_at else None
    }


# 리뷰 생성
def create_review_service(data):
    res_id = data.get("res_id")
    user_id = data.get("user_id")
    content = (data.get("content") or "").strip()
    rating_raw = data.get("rating")
    photo_url = data.get("photo_url")

    if not res_id:
        return None, "res_id는 필수입니다.", 400
    if not user_id:
        return None, "user_id는 필수입니다.", 400
    if not content:
        return None, "content는 비어 있을 수 없습니다.", 400

    rating, err = _validate_rating(rating_raw)
    if err:
        return None, err, 400

    # FK 존재 확인
    restaurant = RestaurantInfo.query.get(res_id)
    if restaurant is None:
        return None, "해당 res_id 식당이 존재하지 않습니다.", 404

    user = User.query.get(user_id)
    if user is None:
        return None, "해당 user_id 사용자가 존재하지 않습니다.", 404

    try:
        # ✅ 여기서 user.user_nickname 을 리뷰 테이블 컬럼에 같이 저장
        review = Review(
            res_id=res_id,
            user_id=user_id,
            user_nickname=user.user_nickname,
            content=content,
            rating=rating,
            photo_url=photo_url
        )
        db.session.add(review)
        db.session.commit()

        _recalc_restaurant_score(res_id)

        return _serialize_review(review), "리뷰가 등록되었습니다.", 201

    except Exception as e:
        db.session.rollback()
        return None, f"리뷰 생성 중 오류: {str(e)}", 500


# 리뷰 목록(식당 기준)
def get_reviews_by_restaurant_service(res_id, page=1, per_page=20, order="recent"):
    if RestaurantInfo.query.get(res_id) is None:
        return None, "해당 res_id 식당이 존재하지 않습니다.", 404

    q = Review.query.filter(Review.res_id == res_id)

    if order == "oldest":
        q = q.order_by(Review.created_at.asc())
    elif order == "highest":
        q = q.order_by(Review.rating.desc(), Review.created_at.desc())
    elif order == "lowest":
        q = q.order_by(Review.rating.asc(), Review.created_at.desc())
    else:
        q = q.order_by(Review.created_at.desc())

    p = q.paginate(page=page, per_page=per_page, error_out=False)

    items = [_serialize_review(r) for r in p.items]

    return {
        "items": items,
        "page": page,
        "per_page": per_page,
        "total": p.total,
        "pages": p.pages
    }, "리뷰 목록", 200


# 리뷰 상세
def get_review_detail_service(review_id):
    r = Review.query.get(review_id)
    if not r:
        return None, "해당 리뷰가 존재하지 않습니다.", 404
    return _serialize_review(r), "리뷰 상세", 200


# 리뷰 수정
def update_review_service(review_id, data, actor_user_id=None, is_admin=False):
    r = Review.query.get(review_id)
    if not r:
        return None, "해당 리뷰가 존재하지 않습니다.", 404

    # 본인 또는 관리자
    if not is_admin and actor_user_id is not None and r.user_id != actor_user_id:
        return None, "본인 리뷰만 수정할 수 있습니다.", 403

    changed = False

    if "content" in data:
        new_content = (data.get("content") or "").strip()
        if not new_content:
            return None, "content는 비어 있을 수 없습니다.", 400
        r.content = new_content
        changed = True

    if "rating" in data:
        rating, err = _validate_rating(data.get("rating"))
        if err:
            return None, err, 400
        r.rating = rating
        changed = True

    if "photo_url" in data:
        r.photo_url = data.get("photo_url")
        changed = True

    # 필요하면 닉네임 동기화 옵션
    if data.get("sync_nickname"):
        user = User.query.get(r.user_id)
        if user:
            r.user_nickname = user.user_nickname
            changed = True

    if not changed:
        return None, "변경할 필드가 없습니다.", 400

    try:
        db.session.add(r)
        db.session.commit()
        _recalc_restaurant_score(r.res_id)
        return _serialize_review(r), "리뷰가 수정되었습니다.", 200
    except Exception as e:
        db.session.rollback()
        return None, f"리뷰 수정 중 오류: {str(e)}", 500


# 리뷰 삭제
def delete_review_service(review_id, actor_user_id=None, is_admin=False):
    r = Review.query.get(review_id)
    if not r:
        return None, "해당 리뷰가 존재하지 않습니다.", 404

    if not is_admin and actor_user_id is not None and r.user_id != actor_user_id:
        return None, "본인 리뷰만 삭제할 수 있습니다.", 403

    try:
        res_id = r.res_id
        db.session.delete(r)
        db.session.commit()
        _recalc_restaurant_score(res_id)
        return None, "리뷰가 삭제되었습니다.", 200
    except Exception as e:
        db.session.rollback()
        return None, f"리뷰 삭제 중 오류: {str(e)}", 500


# 리뷰 요약(평균/개수/분포)
def get_restaurant_review_summary_service(res_id):
    if RestaurantInfo.query.get(res_id) is None:
        return None, "해당 res_id 식당이 존재하지 않습니다.", 404

    avg, cnt = db.session.query(
        func.avg(Review.rating), func.count(Review.id)
    ).filter(Review.res_id == res_id).one()

    hist = {i: 0 for i in range(1, 6)}
    rows = db.session.query(Review.rating, func.count(Review.id)).filter(
        Review.res_id == res_id
    ).group_by(Review.rating).all()
    for rating, c in rows:
        hist[int(rating)] = int(c)

    return {
        "res_id": res_id,
        "avg_rating": float(avg) if avg is not None else None,
        "count": int(cnt),
        "histogram": hist
    }, "리뷰 요약", 200
