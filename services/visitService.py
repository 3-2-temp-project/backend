from flask import session
import requests
from sqlalchemy import func, literal
from models import db, Visit, RestaurantInfo
import os
import math
import datetime


# 방문 생성
def create_visit_service(data):
    res_id = data.get("res_id")
    visit_date_raw = data.get("visit_date")  # "YYYY-MM-DD"

    if not res_id:
        return None, "res_id는 필수입니다.", 400
    if not visit_date_raw:
        return None, "visit_date는 필수입니다. (YYYY-MM-DD)", 400

    # FK 존재 확인
    if RestaurantInfo.query.get(res_id) is None:
        return None, "해당 res_id 식당이 존재하지 않습니다.", 404

    # 날짜 파싱
    try:
        visit_date = datetime.date.fromisoformat(str(visit_date_raw))
    except Exception:
        return None, "visit_date 형식이 올바르지 않습니다. (예: 2025-01-01)", 400

    try:
        v = Visit(
            res_id=res_id,
            visit_date=visit_date
        )
        db.session.add(v)
        db.session.commit()

        return {
            "vi_id": v.vi_id,
            "res_id": v.res_id,
            "visit_date": v.visit_date.isoformat()
        }, "방문이 등록되었습니다.", 201
    except Exception as e:
        db.session.rollback()
        return None, f"방문 생성 중 오류: {str(e)}", 500


# 특정 식당의 방문 목록(최근 방문순)
def get_visits_by_restaurant_service(res_id, page=1, per_page=20, order="recent"):
    if RestaurantInfo.query.get(res_id) is None:
        return None, "해당 res_id 식당이 존재하지 않습니다.", 404

    q = Visit.query.filter(Visit.res_id == res_id)
    if order == "oldest":
        q = q.order_by(Visit.visit_date.asc(), Visit.vi_id.asc())
    else:
        q = q.order_by(Visit.visit_date.desc(), Visit.vi_id.desc())

    p = q.paginate(page=page, per_page=per_page, error_out=False)

    items = []
    for v in p.items:
        items.append({
            "vi_id": v.vi_id,
            "res_id": v.res_id,
            "visit_date": v.visit_date.isoformat()
        })

    return {
        "items": items,
        "page": page,
        "per_page": per_page,
        "total": p.total,
        "pages": p.pages
    }, "방문 목록", 200


# 기간별 방문 조회(옵션: 특정 식당)
def get_visits_in_range_service(start_date, end_date, res_id=None, page=1, per_page=20):
    if not start_date or not end_date:
        return None, "start_date와 end_date는 필수입니다. (YYYY-MM-DD)", 400

    try:
        sd = datetime.date.fromisoformat(str(start_date))
        ed = datetime.date.fromisoformat(str(end_date))
    except Exception:
        return None, "날짜 형식이 올바르지 않습니다. (예: 2025-01-01)", 400

    if sd > ed:
        return None, "start_date는 end_date보다 이후일 수 없습니다.", 400

    q = Visit.query.filter(Visit.visit_date.between(sd, ed))

    if res_id is not None:
        if RestaurantInfo.query.get(res_id) is None:
            return None, "해당 res_id 식당이 존재하지 않습니다.", 404
        q = q.filter(Visit.res_id == res_id)

    q = q.order_by(Visit.visit_date.desc(), Visit.vi_id.desc())
    p = q.paginate(page=page, per_page=per_page, error_out=False)

    items = []
    for v in p.items:
        items.append({
            "vi_id": v.vi_id,
            "res_id": v.res_id,
            "visit_date": v.visit_date.isoformat()
        })

    return {
        "items": items,
        "page": page,
        "per_page": per_page,
        "total": p.total,
        "pages": p.pages
    }, "기간별 방문 목록", 200


# 방문 상세
def get_visit_detail_service(vi_id):
    v = Visit.query.get(vi_id)
    if not v:
        return None, "해당 방문이 존재하지 않습니다.", 404
    return {
        "vi_id": v.vi_id,
        "res_id": v.res_id,
        "visit_date": v.visit_date.isoformat()
    }, "방문 상세", 200


# 방문 수정(관리자 전용 가정)
def update_visit_service(vi_id, data, is_admin=False):
    v = Visit.query.get(vi_id)
    if not v:
        return None, "해당 방문이 존재하지 않습니다.", 404

    if not is_admin:
        return None, "관리자만 수정할 수 있습니다.", 403

    changed = False

    if "res_id" in data:
        new_res_id = data.get("res_id")
        if not new_res_id:
            return None, "res_id는 비어 있을 수 없습니다.", 400
        if RestaurantInfo.query.get(new_res_id) is None:
            return None, "해당 res_id 식당이 존재하지 않습니다.", 404
        v.res_id = new_res_id
        changed = True

    if "visit_date" in data:
        try:
            v.visit_date = datetime.date.fromisoformat(str(data.get("visit_date")))
        except Exception:
            return None, "visit_date 형식이 올바르지 않습니다. (예: 2025-01-01)", 400
        changed = True

    if not changed:
        return None, "변경할 필드가 없습니다.", 400

    try:
        db.session.add(v)
        db.session.commit()
        return {
            "vi_id": v.vi_id,
            "res_id": v.res_id,
            "visit_date": v.visit_date.isoformat()
        }, "방문이 수정되었습니다.", 200
    except Exception as e:
        db.session.rollback()
        return None, f"방문 수정 중 오류: {str(e)}", 500


# 방문 삭제(관리자 전용 가정)
def delete_visit_service(vi_id, is_admin=False):
    v = Visit.query.get(vi_id)
    if not v:
        return None, "해당 방문이 존재하지 않습니다.", 404

    if not is_admin:
        return None, "관리자만 삭제할 수 있습니다.", 403

    try:
        db.session.delete(v)
        db.session.commit()
        return None, "방문이 삭제되었습니다.", 200
    except Exception as e:
        db.session.rollback()
        return None, f"방문 삭제 중 오류: {str(e)}", 500


# 방문 집계(최근 N일, 일자별 카운트)
def get_visit_counts_by_day_service(res_id, days=30):
    if RestaurantInfo.query.get(res_id) is None:
        return None, "해당 res_id 식당이 존재하지 않습니다.", 404

    try:
        days = int(days)
    except:
        return None, "days는 정수여야 합니다.", 400
    if days <= 0:
        return None, "days는 0보다 커야 합니다.", 400

    end = datetime.date.today()
    start = end - datetime.timedelta(days=days - 1)

    rows = (db.session.query(
                Visit.visit_date.label("d"),
                func.count(Visit.vi_id).label("cnt")
            )
            .filter(Visit.res_id == res_id)
            .filter(Visit.visit_date.between(start, end))
            .group_by(Visit.visit_date)
            .order_by(Visit.visit_date.asc())
            .all())

    # 결과를 날짜 키로 매꿔서 반환
    counts = {}
    cur = start
    row_map = {r.d: int(r.cnt) for r in rows}
    while cur <= end:
        counts[cur.isoformat()] = row_map.get(cur, 0)
        cur += datetime.timedelta(days=1)

    return {
        "res_id": res_id,
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "counts_by_day": counts
    }, "방문 일자별 집계", 200
