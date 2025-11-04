from flask import session
import requests
from sqlalchemy import func, literal
from models import db, Badge, RestaurantInfo
import os
import math


# 뱃지 생성
def create_badge_service(data):
    res_id = data.get("res_id")
    badge_type = (data.get("badge_type") or "").strip()
    description = data.get("description")

    if not res_id:
        return None, "res_id는 필수입니다.", 400
    if not badge_type:
        return None, "badge_type은 비어 있을 수 없습니다.", 400

    # FK 존재 확인
    if RestaurantInfo.query.get(res_id) is None:
        return None, "해당 res_id 식당이 존재하지 않습니다.", 404

    try:
        b = Badge(
            res_id=res_id,
            badge_type=badge_type,
            description=description
        )
        db.session.add(b)
        db.session.commit()

        return {
            "id": b.id,
            "res_id": b.res_id,
            "badge_type": b.badge_type,
            "description": b.description,
            "issued_at": b.issued_at.isoformat() if b.issued_at else None
        }, "뱃지가 발급되었습니다.", 201
    except Exception as e:
        db.session.rollback()
        return None, f"뱃지 생성 중 오류: {str(e)}", 500


# 특정 식당의 뱃지 목록(최근 발급순)
def get_badges_by_restaurant_service(res_id, page=1, per_page=20):
    if RestaurantInfo.query.get(res_id) is None:
        return None, "해당 res_id 식당이 존재하지 않습니다.", 404

    q = Badge.query.filter(Badge.res_id == res_id).order_by(Badge.issued_at.desc())
    p = q.paginate(page=page, per_page=per_page, error_out=False)

    items = []
    for b in p.items:
        items.append({
            "id": b.id,
            "res_id": b.res_id,
            "badge_type": b.badge_type,
            "description": b.description,
            "issued_at": b.issued_at.isoformat() if b.issued_at else None
        })

    return {
        "items": items,
        "page": page,
        "per_page": per_page,
        "total": p.total,
        "pages": p.pages
    }, "뱃지 목록", 200


# 뱃지 상세
def get_badge_detail_service(badge_id):
    b = Badge.query.get(badge_id)
    if not b:
        return None, "해당 뱃지가 존재하지 않습니다.", 404
    return {
        "id": b.id,
        "res_id": b.res_id,
        "badge_type": b.badge_type,
        "description": b.description,
        "issued_at": b.issued_at.isoformat() if b.issued_at else None
    }, "뱃지 상세", 200


# 뱃지 수정(관리자 전용 가정)
def update_badge_service(badge_id, data, is_admin=False):
    b = Badge.query.get(badge_id)
    if not b:
        return None, "해당 뱃지가 존재하지 않습니다.", 404

    if not is_admin:
        return None, "관리자만 수정할 수 있습니다.", 403

    changed = False

    if "badge_type" in data:
        new_type = (data.get("badge_type") or "").strip()
        if not new_type:
            return None, "badge_type은 비어 있을 수 없습니다.", 400
        b.badge_type = new_type
        changed = True

    if "description" in data:
        b.description = data.get("description")
        changed = True

    if "res_id" in data:
        new_res_id = data.get("res_id")
        if not new_res_id:
            return None, "res_id는 비어 있을 수 없습니다.", 400
        if RestaurantInfo.query.get(new_res_id) is None:
            return None, "해당 res_id 식당이 존재하지 않습니다.", 404
        b.res_id = new_res_id
        changed = True

    if not changed:
        return None, "변경할 필드가 없습니다.", 400

    try:
        db.session.add(b)
        db.session.commit()
        return {
            "id": b.id,
            "res_id": b.res_id,
            "badge_type": b.badge_type,
            "description": b.description,
            "issued_at": b.issued_at.isoformat() if b.issued_at else None
        }, "뱃지가 수정되었습니다.", 200
    except Exception as e:
        db.session.rollback()
        return None, f"뱃지 수정 중 오류: {str(e)}", 500


# 뱃지 삭제(관리자 전용 가정)
def delete_badge_service(badge_id, is_admin=False):
    b = Badge.query.get(badge_id)
    if not b:
        return None, "해당 뱃지가 존재하지 않습니다.", 404

    if not is_admin:
        return None, "관리자만 삭제할 수 있습니다.", 403

    try:
        db.session.delete(b)
        db.session.commit()
        return None, "뱃지가 삭제되었습니다.", 200
    except Exception as e:
        db.session.rollback()
        return None, f"뱃지 삭제 중 오류: {str(e)}", 500
