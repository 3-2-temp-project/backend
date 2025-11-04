from flask import session
import requests
from sqlalchemy import func, literal
from models import db, Suggestion, User, RestaurantInfo
import os
import math


# 제보 생성
def create_suggestion_service(data):
    res_name = (data.get("res_name") or "").strip()
    address = data.get("address")
    user_id = data.get("user_id")
    photo_url = data.get("photo_url")
    description = data.get("description")

    if not res_name:
        return None, "res_name는 필수입니다.", 400
    if not user_id:
        return None, "user_id는 필수입니다.", 400

    # FK 존재 확인
    if User.query.get(user_id) is None:
        return None, "해당 user_id 사용자가 존재하지 않습니다.", 404

    try:
        sug = Suggestion(
            res_name=res_name,
            address=address,
            user_id=user_id,
            photo_url=photo_url,
            description=description
        )
        db.session.add(sug)
        db.session.commit()

        return {
            "id": sug.id,
            "res_name": sug.res_name,
            "address": sug.address,
            "user_id": sug.user_id,
            "photo_url": sug.photo_url,
            "description": sug.description,
            "created_at": sug.created_at.isoformat() if sug.created_at else None
        }, "제보가 등록되었습니다.", 201
    except Exception as e:
        db.session.rollback()
        return None, f"제보 생성 중 오류: {str(e)}", 500


# 제보 목록(최근순)
def get_suggestions_recent_service(page=1, per_page=20):
    q = Suggestion.query.order_by(Suggestion.created_at.desc())
    p = q.paginate(page=page, per_page=per_page, error_out=False)

    items = []
    for s in p.items:
        items.append({
            "id": s.id,
            "res_name": s.res_name,
            "address": s.address,
            "user_id": s.user_id,
            "photo_url": s.photo_url,
            "description": s.description,
            "created_at": s.created_at.isoformat() if s.created_at else None
        })

    return {
        "items": items,
        "page": page,
        "per_page": per_page,
        "total": p.total,
        "pages": p.pages
    }, "제보 목록", 200


# 특정 사용자 제보 목록
def get_suggestions_by_user_service(user_id, page=1, per_page=20):
    if User.query.get(user_id) is None:
        return None, "해당 user_id 사용자가 존재하지 않습니다.", 404

    q = Suggestion.query.filter(Suggestion.user_id == user_id).order_by(Suggestion.created_at.desc())
    p = q.paginate(page=page, per_page=per_page, error_out=False)

    items = []
    for s in p.items:
        items.append({
            "id": s.id,
            "res_name": s.res_name,
            "address": s.address,
            "user_id": s.user_id,
            "photo_url": s.photo_url,
            "description": s.description,
            "created_at": s.created_at.isoformat() if s.created_at else None
        })

    return {
        "items": items,
        "page": page,
        "per_page": per_page,
        "total": p.total,
        "pages": p.pages
    }, "사용자 제보 목록", 200


# 제보 검색(상호/주소)
def search_suggestions_service(qstr, page=1, per_page=20):
    qstr = (qstr or "").strip()
    if not qstr:
        return None, "검색어(q)는 필수입니다.", 400

    q = (Suggestion.query
         .filter(
             (Suggestion.res_name.ilike(f"%{qstr}%")) |
             (Suggestion.address.ilike(f"%{qstr}%"))
         )
         .order_by(Suggestion.created_at.desc()))
    p = q.paginate(page=page, per_page=per_page, error_out=False)

    items = []
    for s in p.items:
        items.append({
            "id": s.id,
            "res_name": s.res_name,
            "address": s.address,
            "user_id": s.user_id,
            "photo_url": s.photo_url,
            "description": s.description,
            "created_at": s.created_at.isoformat() if s.created_at else None
        })

    return {
        "items": items,
        "page": page,
        "per_page": per_page,
        "total": p.total,
        "pages": p.pages
    }, "제보 검색 결과", 200


# 제보 상세
def get_suggestion_detail_service(suggestion_id):
    s = Suggestion.query.get(suggestion_id)
    if not s:
        return None, "해당 제보가 존재하지 않습니다.", 404

    return {
        "id": s.id,
        "res_name": s.res_name,
        "address": s.address,
        "user_id": s.user_id,
        "photo_url": s.photo_url,
        "description": s.description,
        "created_at": s.created_at.isoformat() if s.created_at else None
    }, "제보 상세", 200


# 제보 수정
def update_suggestion_service(suggestion_id, data, actor_user_id=None, is_admin=False):
    s = Suggestion.query.get(suggestion_id)
    if not s:
        return None, "해당 제보가 존재하지 않습니다.", 404

    # 본인 또는 관리자
    if not is_admin and actor_user_id is not None and s.user_id != actor_user_id:
        return None, "본인 제보만 수정할 수 있습니다.", 403

    changed = False

    if "res_name" in data:
        new_res_name = (data.get("res_name") or "").strip()
        if not new_res_name:
            return None, "res_name는 비어 있을 수 없습니다.", 400
        s.res_name = new_res_name
        changed = True

    if "address" in data:
        s.address = data.get("address")
        changed = True

    if "photo_url" in data:
        s.photo_url = data.get("photo_url")
        changed = True

    if "description" in data:
        s.description = data.get("description")
        changed = True

    if not changed:
        return None, "변경할 필드가 없습니다.", 400

    try:
        db.session.add(s)
        db.session.commit()
        return {
            "id": s.id,
            "res_name": s.res_name,
            "address": s.address,
            "user_id": s.user_id,
            "photo_url": s.photo_url,
            "description": s.description,
            "created_at": s.created_at.isoformat() if s.created_at else None
        }, "제보가 수정되었습니다.", 200
    except Exception as e:
        db.session.rollback()
        return None, f"제보 수정 중 오류: {str(e)}", 500


# 제보 삭제
def delete_suggestion_service(suggestion_id, actor_user_id=None, is_admin=False):
    s = Suggestion.query.get(suggestion_id)
    if not s:
        return None, "해당 제보가 존재하지 않습니다.", 404

    if not is_admin and actor_user_id is not None and s.user_id != actor_user_id:
        return None, "본인 제보만 삭제할 수 있습니다.", 403

    try:
        db.session.delete(s)
        db.session.commit()
        return None, "제보가 삭제되었습니다.", 200
    except Exception as e:
        db.session.rollback()
        return None, f"제보 삭제 중 오류: {str(e)}", 500
