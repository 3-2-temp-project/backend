# init_data.py
import os
import re
import csv
import math
import logging
import unicodedata
from functools import lru_cache
from typing import Optional, List, Tuple, Dict

import requests
import pdfplumber

# ---------- Optional parsers ----------
try:
    import openpyxl  # .xlsx
except ImportError:
    openpyxl = None

try:
    import xlrd  # .xls
except ImportError:
    xlrd = None

try:
    import olefile  # .hwp (OLE) preview text
except ImportError:
    olefile = None

import zipfile            # .hwpx (ZIP)
import xml.etree.ElementTree as ET  # XML text extraction for HWPX

from models import db, RestaurantInfo

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
log = logging.getLogger(__name__)

PDF_BASE_DIR = os.getenv("PDF_BASE_DIR", "pdf_data")
NAVER_GEOCODE_URL = "https://maps.apigw.ntruss.com/map-geocode/v2/geocode"
NAVER_CLIENT_ID = os.getenv("NAVER_CLIENT_ID")
NAVER_CLIENT_SECRET = os.getenv("NAVER_CLIENT_SECRET")

REGION_HINTS: List[str] = [h.strip() for h in os.getenv("GEOCODE_REGION_HINT", "").split("|") if h.strip()]
ALLOW_NO_GEOCODE = os.getenv("ALLOW_NO_GEOCODE", "true").lower() in ("1", "true", "yes", "y")

# ---------------- utils ----------------
def _to_int(s: str) -> Optional[int]:
    if s is None:
        return None
    s = re.sub(r"[^\d]", "", str(s))
    return int(s) if s.isdigit() else None

def _normalize_spaces(s: str) -> str:
    s = (s or "").replace("　", " ")
    s = re.sub(r"\s+", " ", s).strip()
    return s

def _to_halfwidth(s: str) -> str:
    return unicodedata.normalize('NFKC', s or "")

def _strip_weird_unicode(s: str) -> str:
    s = "".join(ch for ch in s if (ch.isprintable() and not unicodedata.category(ch).startswith("C")))
    s = s.replace("【", "[").replace("】", "]").replace("「","[").replace("」","]").replace("『","[").replace("』","]")
    return s

def _clean_text(s: Optional[str]) -> str:
    s = "" if s is None else str(s)
    s = s.replace("장소명", "장소")
    s = _to_halfwidth(s)
    s = _strip_weird_unicode(s)
    return _normalize_spaces(s)

# --- 행사/부스/비음식 키워드 ---
_EVENT_KEYWORDS = [
    "부스", "홍보부스", "체험부스", "행사", "이벤트", "축제", "페스티벌",
    "프로모션", "체험존", "전시부스", "박람회", "컨벤션", "엑스포",
    "홍보관", "현수막", "운영본부", "행사진행본부", "행사장", "무대설치",
]
def _is_event_booth_text(s: str) -> bool:
    s = _clean_text(s)
    if not s:
        return False
    return any(k in s for k in _EVENT_KEYWORDS)

_NOT_FNB_KEYWORDS = [
    "화환", "주유", "택시", "발렛", "사무용품", "대여", "프린트",
    "가맹점", "문구", "주차", "장례식장", "후생복지위원회",
    "체험", "홍보", "박람회", "컨벤션", "엑스포", "현수막",
]
def _looks_like_food_place(name: str) -> bool:
    if not name:
        return False
    if _is_event_booth_text(name):
        return False
    return not any(k in name for k in _NOT_FNB_KEYWORDS)

# ---------------- 이름 정제 ----------------
_CORP_PREFIXES = [
    "주식회사", "(주)", "（주）", "㈜", "유한회사", "유한책임회사",
    "합자회사", "합명회사", "사단법인", "재단법인",
    "Inc.", "INC.", "Co.,Ltd", "Co.Ltd", "Ltd.", "LLC", "GmbH", "PLC", "PTE.LTD"
]
_BRANCH_TAILS = ["본점", "본사", "지점", "브랜치", "본관", "신관", "센터", "본부"]
def _clean_res_name(name: str) -> str:
    if not name:
        return ""
    n = _clean_text(name)

    m = re.fullmatch(r"^\((.+)\)$", n)
    if m:
        n = _clean_text(m.group(1))

    for p in _CORP_PREFIXES:
        n = re.sub(rf"^\s*{re.escape(p)}\s*", "", n, flags=re.IGNORECASE)
        n = re.sub(rf"\s*{re.escape(p)}\s*$", "", n, flags=re.IGNORECASE)

    n = re.sub(r"\b(?:Tel|TEL|전화)\s*[:：]?\s*\d[\d\-]{6,}\b", "", n)
    n = re.sub(r"\b\d{2,4}[-\s]?\d{3,4}[-\s]?\d{3,4}\b", "", n)

    n = re.sub(r"[\(\[]\s*(%s)\s*[\)\]]$" % "|".join(map(re.escape, _BRANCH_TAILS)), "", n, flags=re.IGNORECASE)
    n = re.sub(r"\s*[-–—]\s*(%s)\s*$" % "|".join(map(re.escape, _BRANCH_TAILS)), "", n, flags=re.IGNORECASE)

    n = re.sub(r"[_\-]\d{6,8}$", "", n)
    n = re.sub(r"\bver\s*\d+(\.\d+)?\b", "", n, flags=re.IGNORECASE)

    n = _strip_weird_unicode(n)
    n = _normalize_spaces(n)

    if not re.search(r"[A-Za-z가-힣]", n):
        return ""
    return n

# ---------------- 주소 판별(강화) ----------------
_ADDR_TOKENS = ["로", "길", "대로", "번길", "가", "동", "리", "로길", "시", "군", "구", "읍", "면"]
_ADDR_CORE_RE = re.compile(
    r"(?:(?:[가-힣]{1,10}(?:시|군|구))\s*)?"
    r"(?:[가-힣0-9\-]{1,20}(?:로|길|대로|번길)\s*\d{1,4}(?:-\d{1,4})?)"
    r"|"
    r"(?:[가-힣]{1,10}(?:동|리)\s*\d{1,4}(?:-\d{1,4})?(?:번지)?)"
    r"|"
    r"(?:\d{1,5}(?:-\d{1,4})?(?:번지|호|층))"
)

def _score_address(s: str) -> int:
    if not s:
        return 0
    s = _clean_text(s)
    score = 0
    if _ADDR_CORE_RE.search(s):
        score += 4
    if re.search(r"(로|길|대로|번길)\s*\d{1,4}(?:-\d{1,4})?", s):
        score += 3
    if re.search(r"(특별시|광역시|자치시|도|시|군|구|읍|면|동|리)", s):
        score += 2
    if re.search(r"(번지|호|층)\b", s):
        score += 2
    if re.search(r"\d", s):
        score += 1
    if any(k in s for k in ["점", "지점", "본점", "센터", "본부", "본사", "브랜치"]):
        score -= 2
    return score

def _is_confident_address(s: str) -> bool:
    return _score_address(s) >= 6

def _split_place_and_address(place_raw: str) -> tuple[str, str]:
    """
    - 주소라고 '확신'하는 경우에만 address를 채운다.
    - 확신 불가 → address 비우고 res_name만 반환.
    """
    p = _clean_text(place_raw)
    if not p:
        return ("", "")

    if _is_event_booth_text(p):
        return ("", "")

    m = re.match(r"^(.*?)[\s]*\((.+?)\)$", p)
    if m:
        left, inner = _clean_text(m.group(1)), _clean_text(m.group(2))
        if _is_confident_address(inner) and not _is_confident_address(left):
            return (_clean_res_name(left), inner)
        if _is_confident_address(left) and not _is_confident_address(inner):
            return (_clean_res_name(inner), left)

    for sep in ["\t", "  "]:
        if sep in p:
            parts = [x.strip() for x in p.split(sep) if x.strip()]
            if len(parts) == 2:
                a, b = parts
                if _is_confident_address(a) and not _is_confident_address(b):
                    return (_clean_res_name(b), a)
                if _is_confident_address(b) and not _is_confident_address(a):
                    return (_clean_res_name(a), b)

    for sep in [" / ", " · ", " - ", " – "]:
        if sep in p:
            a, b = [x.strip() for x in p.split(sep, 1)]
            if _is_confident_address(a) and not _is_confident_address(b):
                return (_clean_res_name(b), a)
            if _is_confident_address(b) and not _is_confident_address(a):
                return (_clean_res_name(a), b)

    if _is_confident_address(p):
        return ("", p)

    return (_clean_res_name(p), "")

# ---------------- 쓰레기 라인 필터 ----------------
def _looks_like_garbage_row(s: str) -> bool:
    t = _clean_text(s)
    if not t:
        return True

    tokens = re.split(r"[ ,\t/|]+", t)
    tokens = [x for x in tokens if x]
    if not tokens:
        return True

    num_like = sum(1 for x in tokens if re.fullmatch(r"-?\d+(?:\.\d+)?", x))
    if num_like / max(1, len(tokens)) >= 0.7:
        return True

    letters = re.findall(r"[A-Za-z가-힣]", t)
    digits = re.findall(r"\d", t)
    if len(letters) <= 1 and len(digits) >= 4:
        return True

    return False

# ---------------- geocode (Naver) ----------------
def _geocode_naver(keyword: str) -> Tuple[Optional[str], Optional[float], Optional[float]]:
    if not (NAVER_CLIENT_ID and NAVER_CLIENT_SECRET):
        return None, None, None

    headers = {
        "X-NCP-APIGW-API-KEY-ID": NAVER_CLIENT_ID,
        "X-NCP-APIGW-API-KEY": NAVER_CLIENT_SECRET,
    }
    queries = [f"{hint} {keyword}".strip() for hint in REGION_HINTS] or [keyword]

    for q in queries:
        try:
            r = requests.get(NAVER_GEOCODE_URL, headers=headers, params={"query": q}, timeout=7)
            if r.status_code != 200:
                log.warning(f"[GEOCODE] {q} HTTP {r.status_code}")
                continue
            data = r.json()
            if not data.get("addresses"):
                continue
            a0 = data["addresses"][0]
            addr = a0.get("roadAddress") or a0.get("jibunAddress") or a0.get("englishAddress")
            x = a0.get("x"); y = a0.get("y")
            if not (addr and x and y):
                continue
            lat, lng = float(y), float(x)
            return addr, lat, lng
        except Exception as e:
            log.warning(f"[GEOCODE] fail '{q}': {e}")
            continue
    return None, None, None

@lru_cache(maxsize=512)
def _geocode_naver_cached(keyword: str) -> Tuple[Optional[str], Optional[float], Optional[float]]:
    return _geocode_naver(keyword)

# ---------------- parsers ----------------
PLACE_HEADERS   = ["장소", "사용처", "지출처", "업체명", "상호", "상호명", "가맹점명", "장소/상호", "사용장소"]
PEOPLE_HEADERS  = ["인원", "인원수", "사용인원", "참석인원", "대상인원"]
AMOUNT_HEADERS  = [
    "금액", "승인금액", "금액(원)", "지출금액", "합계", "총액", "카드이용금액",
    "집행액", "집행액(원)", "사용금액", "사용금액(원)"
]
# 🔥 NEW: 명시적 이름/주소 헤더 후보
NAME_HEADERS    = ["상호", "상호명", "업체명", "지출처", "사용처", "장소", "가맹점명", "상호/가맹점"]
ADDRESS_HEADERS = [
    "주소", "도로명주소", "지번주소", "주소(도로명)", "주소(지번)",
    "소재지", "사업장주소", "업체주소", "가맹점주소"
]

def _find_header_idx_and_cols(table: List[List[str]]) -> Tuple[Optional[int], int, int, int, int, int]:
    """
    return: (header_row_idx, i_place, i_people, i_amt, i_name, i_addr)
    - i_name/i_addr가 발견되면 추출 단계에서 '{상호} ({주소})'로 결합하여 강제 파싱
    """
    def col_idx_by_any(header_row: List[str], candidates: List[str]) -> int:
        for idx, h in enumerate(header_row):
            h = _clean_text(h)
            for key in candidates:
                if key in h:
                    return idx
        return -1

    scan_upto = min(30, len(table))
    for i in range(scan_upto):
        raw = table[i]

        def try_row(cols: List[str]) -> Tuple[int,int,int,int,int]:
            i_place  = col_idx_by_any(cols, PLACE_HEADERS)
            i_people = col_idx_by_any(cols, PEOPLE_HEADERS)
            i_amt    = col_idx_by_any(cols, AMOUNT_HEADERS)
            i_name   = col_idx_by_any(cols, NAME_HEADERS)
            i_addr   = col_idx_by_any(cols, ADDRESS_HEADERS)
            return i_place, i_people, i_amt, i_name, i_addr

        header = [_clean_text(c) for c in raw]
        i_place, i_people, i_amt, i_name, i_addr = try_row(header)
        if (i_place != -1 or i_name != -1 or i_addr != -1) and (i_people != -1 or i_amt != -1 or True):
            log.info(f"[PARSE] header row@{i}: {header} → place={i_place}, people={i_people}, amount={i_amt}, name={i_name}, addr={i_addr}")
            return i, i_place, i_people, i_amt, i_name, i_addr

        # explode combined headers
        exploded: List[str] = []
        for c in raw:
            c = _clean_text(c)
            if "\n" in c:
                parts = [ _clean_text(p) for p in c.split("\n") if _clean_text(p) ]
                if len(parts) > 1:
                    exploded.extend(parts); continue
            if "/" in c:
                parts = [ _clean_text(p) for p in c.split("/") if _clean_text(p) ]
                if len(parts) > 1:
                    exploded.extend(parts); continue
            parts = re.split(r"\s{2,}", c)
            parts = [ _clean_text(p) for p in parts if _clean_text(p) ]
            exploded.extend(parts if parts else [c])

        i_place, i_people, i_amt, i_name, i_addr = try_row(exploded)
        if (i_place != -1 or i_name != -1 or i_addr != -1) and (i_people != -1 or i_amt != -1 or True):
            log.info(f"[PARSE] header row@{i} (exploded): {exploded} → place={i_place}, people={i_people}, amount={i_amt}, name={i_name}, addr={i_addr}")
            return i, i_place, i_people, i_amt, i_name, i_addr

    log.warning(f"[PARSE] header not found in first {scan_upto} rows")
    return None, -1, -1, -1, -1, -1

def _extract_rows_from_table(table: List[List[str]]) -> List[Tuple[str, Optional[int], Optional[int]]]:
    """
    표 → (place_raw, people, amount)
    - name/addr 컬럼이 있으면 place_raw를 '{상호} ({주소})'로 구성하여 _split이 확정적으로 인식
    - addr 컬럼에서 온 주소는 신뢰도 검사 없이 사용되도록 위 포맷으로 전달
    """
    rows: List[Tuple[str, Optional[int], Optional[int]]] = []
    header_idx, i_place, i_people, i_amt, i_name, i_addr = _find_header_idx_and_cols(table)
    if header_idx is None:
        return rows

    for row in table[header_idx+1:]:
        cells = [_clean_text(c) for c in row]

        # 숫자 컬럼 추출
        people = cells[i_people] if 0 <= i_people < len(cells) and i_people != -1 else ""
        amount = cells[i_amt]    if 0 <= i_amt    < len(cells) and i_amt    != -1 else ""

        # 1) name/addr 컬럼 우선
        name_val = cells[i_name] if 0 <= i_name < len(cells) and i_name != -1 else ""
        addr_val = cells[i_addr] if 0 <= i_addr < len(cells) and i_addr != -1 else ""
        name_val = _clean_text(name_val)
        addr_val = _clean_text(addr_val)

        place_raw = ""
        if name_val or addr_val:
            # 주소가 빈 경우도 있지만, name만 있어도 이후 지오코딩 키로 활용됨
            if name_val and addr_val:
                place_raw = f"{name_val} ({addr_val})"  # ← 주소 신뢰도 검사 우회
            elif name_val:
                place_raw = name_val
            else:
                place_raw = addr_val

        # 2) fallback: place 계열 컬럼
        if not place_raw:
            place_col = cells[i_place] if 0 <= i_place < len(cells) and i_place != -1 else ""
            place_raw = _clean_text(place_col)

        if not place_raw:
            continue

        rows.append((place_raw, _to_int(people), _to_int(amount)))
    return rows

def parse_pdf_file(pdf_path: str) -> List[Tuple[str, Optional[int], Optional[int]]]:
    out: List[Tuple[str, Optional[int], Optional[int]]] = []
    try:
        with pdfplumber.open(pdf_path) as pdf:
            for p in pdf.pages:
                try:
                    tables = p.extract_tables() or []
                except Exception:
                    tables = []
                for t in tables:
                    t_norm = [[(c if isinstance(c, str) else (c or "")) for c in row] for row in t]
                    out.extend(_extract_rows_from_table(t_norm))
    except Exception as e:
        log.warning(f"[PDF] parse fail {pdf_path}: {e}")
    return out

def parse_csv_file(csv_path: str) -> List[Tuple[str, Optional[int], Optional[int]]]:
    rows: List[Tuple[str, Optional[int], Optional[int]]] = []
    try:
        with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
            reader = csv.reader(f)
            data = [list(r) for r in reader]
        rows.extend(_extract_rows_from_table(data))
    except Exception as e:
        log.warning(f"[CSV] parse fail {csv_path}: {e}")
    return rows

# === 워크시트 → 2D 배열 (병합셀 확장) ===
def _sheet_to_2d(ws) -> List[List[str]]:
    max_row = ws.max_row or 0
    max_col = ws.max_column or 0

    data = [["" for _ in range(max_col)] for _ in range(max_row)]
    for r in range(1, max_row + 1):
        for c in range(1, max_col + 1):
            v = ws.cell(row=r, column=c).value
            data[r-1][c-1] = "" if v is None else str(v)

    try:
        for mr in getattr(ws, "merged_cells").ranges:
            min_row, min_col, max_row2, max_col2 = mr.min_row, mr.min_col, mr.max_row, mr.max_col
            top_left_val = data[min_row-1][min_col-1]
            for rr in range(min_row-1, max_row2):
                for cc in range(min_col-1, max_col2):
                    data[rr][cc] = top_left_val
    except Exception:
        pass

    norm: List[List[str]] = []
    for row in data:
        row = [_clean_text(x) for x in row]
        end = len(row)
        while end > 0 and (row[end-1] == "" or row[end-1] is None):
            end -= 1
        row = row[:end]
        if any(cell for cell in row):
            norm.append(row)
    return norm

def parse_xlsx_file(xlsx_path: str) -> List[Tuple[str, Optional[int], Optional[int]]]:
    rows: List[Tuple[str, Optional[int], Optional[int]]] = []
    if openpyxl is None:
        log.info("[XLSX] openpyxl not installed; skip .xlsx")
        return rows
    try:
        wb = openpyxl.load_workbook(xlsx_path, data_only=True)
        for ws in wb.worksheets:
            try:
                data = _sheet_to_2d(ws)
                if not data:
                    continue

                # 1) 기본 시도
                rows1 = _extract_rows_from_table(data)
                if rows1:
                    rows.extend(rows1)
                    continue

                # 2) 셀 내부 분해(개행/슬래시/2칸 이상 공백)
                split_rows: List[List[str]] = []
                for r in data:
                    new_r: List[str] = []
                    for cell in r:
                        c = _clean_text(cell)
                        if "\n" in c:
                            parts = [ _clean_text(p) for p in c.split("\n") if _clean_text(p) ]
                            if len(parts) > 1:
                                new_r.extend(parts); continue
                        if "/" in c:
                            parts = [ _clean_text(p) for p in c.split("/") if _clean_text(p) ]
                            if len(parts) > 1:
                                new_r.extend(parts); continue
                        parts = re.split(r"\s{2,}", c)
                        parts = [ _clean_text(p) for p in parts if _clean_text(p) ]
                        new_r.extend(parts if parts else [c])
                    split_rows.append(new_r)

                rows2 = _extract_rows_from_table(split_rows)
                if rows2:
                    rows.extend(rows2)
                    continue

                # 3) 행 머지 재분절
                reseg: List[List[str]] = []
                for r in data:
                    merged = " ".join([c for c in r if c])
                    if merged:
                        parts = re.split(r"\s{2,}", merged)
                        reseg.append([_clean_text(p) for p in parts if _clean_text(p)])
                rows3 = _extract_rows_from_table(reseg)
                if rows3:
                    rows.extend(rows3)

            except Exception as e:
                log.warning(f"[XLSX] worksheet parse fail {xlsx_path}::{ws.title}: {e}")
    except Exception as e:
        log.warning(f"[XLSX] parse fail {xlsx_path}: {e}")
    return rows

def parse_xls_file(xls_path: str) -> List[Tuple[str, Optional[int], Optional[int]]]:
    rows: List[Tuple[str, Optional[int], Optional[int]]] = []
    if xlrd is None:
        log.info("[XLS] xlrd not installed; skip .xls")
        return rows
    try:
        # xlrd==1.2.0 필요
        wb = xlrd.open_workbook(xls_path)
        for sheet in wb.sheets():
            data: List[List[str]] = []
            for rx in range(sheet.nrows):
                row = []
                for cx in range(sheet.ncols):
                    val = sheet.cell_value(rx, cx)
                    row.append("" if val is None else str(val))
                data.append(row)
            rows.extend(_extract_rows_from_table(data))
    except Exception as e:
        log.warning(f"[XLS] parse fail {xls_path}: {e}")
    return rows

def parse_hwp_file(hwp_path: str) -> List[Tuple[str, Optional[int], Optional[int]]]:
    rows: List[Tuple[str, Optional[int], Optional[int]]] = []
    if olefile is None:
        log.info("[HWP] olefile not installed; skip .hwp")
        return rows
    try:
        if not olefile.isOleFile(hwp_path):
            log.info(f"[HWP] not OLE container: {hwp_path}")
            return rows
        with olefile.OleFileIO(hwp_path) as ole:
            candidates = [u'PrvText', u'PreviewText', u'PrvTextUTF']
            stream_name = None
            for cand in candidates:
                if ole.exists(cand):
                    stream_name = cand; break
                if ole.exists('BodyText/' + cand):
                    stream_name = 'BodyText/' + cand; break
            if not stream_name:
                log.info(f"[HWP] preview text stream not found: {hwp_path}")
                return rows
            data = ole.openstream(stream_name).read()
            text = None
            for enc in ("cp949", "utf-16-le", "utf-8"):
                try:
                    text = data.decode(enc, errors="ignore"); break
                except Exception:
                    continue
            if text is None:
                log.info(f"[HWP] cannot decode preview text: {hwp_path}")
                return rows
            lines = [ln.strip() for ln in text.splitlines() if _clean_text(ln.strip())]
            if not lines:
                return rows
            def split_line(ln: str) -> List[str]:
                if "\t" in ln:
                    return [c.strip() for c in ln.split("\t")]
                parts = re.split(r"\s{2,}", ln)
                return [c.strip() for c in parts]
            table = [split_line(ln) for ln in lines if ln]
            rows.extend(_extract_rows_from_table(table))
    except Exception as e:
        log.warning(f"[HWP] parse fail {hwp_path}: {e}")
    return rows

# --------- HWPX ---------
def _hwpx_iter_texts(xml_bytes: bytes) -> List[str]:
    try:
        root = ET.fromstring(xml_bytes)
    except Exception:
        return []
    texts: List[str] = []
    for elem in root.iter():
        if elem.text and _clean_text(elem.text):
            texts.append(_clean_text(elem.text))
    return texts

def parse_hwpx_file(hwpx_path: str) -> List[Tuple[str, Optional[int], Optional[int]]]:
    rows: List[Tuple[str, Optional[int], Optional[int]]] = []
    try:
        with zipfile.ZipFile(hwpx_path, "r") as zf:
            xml_members = [n for n in zf.namelist() if n.lower().endswith(".xml")]
            lines: List[str] = []
            for name in xml_members:
                try:
                    with zf.open(name) as fp:
                        xml_bytes = fp.read()
                    texts = _hwpx_iter_texts(xml_bytes)
                    lines.extend(texts)
                except Exception:
                    continue

            def split_line(ln: str) -> List[str]:
                if "\t" in ln:
                    return [c.strip() for c in ln.split("\t")]
                parts = re.split(r"\s{2,}", ln)
                return [c.strip() for c in parts if c.strip()]

            table = [split_line(ln) for ln in lines if _clean_text(ln)]
            if table:
                rows.extend(_extract_rows_from_table(table))
    except zipfile.BadZipFile:
        log.warning(f"[HWPX] bad zip: {hwpx_path}")
    except Exception as e:
        log.warning(f"[HWPX] parse fail {hwpx_path}: {e}")
    return rows

# ---------------- scanner ----------------
_SUPPORTED_EXTS = (".pdf", ".csv", ".xlsx", ".xls", ".hwp", ".hwpx")
def _list_source_files(base_dir: str) -> List[str]:
    found: List[str] = []
    for root, _, files in os.walk(base_dir):
        for fn in files:
            lo = fn.lower()
            if any(lo.endswith(ext) for ext in _SUPPORTED_EXTS):
                found.append(os.path.join(root, fn))
    return sorted(found)

def _parse_file_to_rows(path: str) -> List[Tuple[str, Optional[int], Optional[int]]]:
    lo = path.lower()
    if lo.endswith(".pdf"):   return parse_pdf_file(path)
    if lo.endswith(".csv"):   return parse_csv_file(path)
    if lo.endswith(".xlsx"):  return parse_xlsx_file(path)
    if lo.endswith(".xls"):   return parse_xls_file(path)
    if lo.endswith(".hwp"):   return parse_hwp_file(path)
    if lo.endswith(".hwpx"):  return parse_hwpx_file(path)
    return []

# ---------------- rows -> data dicts ----------------
def _rows_to_data_dicts(rows_in: List[Tuple[str, Optional[int], Optional[int]]], limit: int) -> List[Dict]:
    results: List[Dict] = []
    seen_pair = set()

    for place_raw, people, amount in rows_in:
        raw = _clean_text(place_raw)
        if _looks_like_garbage_row(raw):
            continue
        if _is_event_booth_text(raw):
            continue

        name_guess, addr_guess = _split_place_and_address(raw)

        # --- 주소/상호 확정 ---
        name_clean = _clean_res_name(name_guess) if name_guess else ""
        addr_clean = _clean_text(addr_guess) if addr_guess else ""

        # 이름이 있을 때만 비음식 필터
        if name_clean and not _looks_like_food_place(name_clean):
            name_clean = ""  # 이름 버림(주소가 확실하면 주소만)

        # 주소 신뢰도: 명시적 주소 컬럼에서 온 경우엔 이미 (name (addr)) 포맷이므로 통과됨
        if addr_clean and not _is_confident_address(addr_clean):
            # 주소 포맷이 약해도 상호와 함께 들어온 경우 지오코딩으로 보강될 수 있음
            pass

        # 가격: 인원 있으면 1인당, 없으면 총액
        price = None
        if isinstance(amount, int):
            price = math.floor(amount / people) if (people and people > 0) else amount

        # 지오코딩 키: 주소>상호>원문 순으로
        geocode_key = addr_clean or name_clean or raw
        addr_got, lat, lng = _geocode_naver_cached(geocode_key) if geocode_key else (None, None, None)
        if not (isinstance(lat, (int, float)) and isinstance(lng, (int, float))):
            lat, lng = 0.0, 0.0
        else:
            lat, lng = float(lat), float(lng)

        res_name = _normalize_spaces(name_clean)[:64] if name_clean else ""
        address  = _normalize_spaces(addr_clean)[:255] if addr_clean else ""

        if (res_name == "") and (address == ""):
            # 상호만 들어온 경우라도 지오코딩 못 하면 버림
            continue

        key = (res_name, address)
        if key in seen_pair:
            continue
        seen_pair.add(key)

        results.append({
            "res_name": res_name,
            "address": address,
            "lat": lat,
            "lng": lng,
            "res_phone": None,
            "category": None,
            "price": price,
            "score": 0.0,
        })

        if limit and len(results) >= limit:
            break
    return results

# ---------------- build init_data ----------------
def _gather_candidates_from_files(base_dir: str) -> List[Tuple[str, Optional[int], Optional[int]]]:
    files = _list_source_files(base_dir)
    log.info(f"[SCAN] sources: {len(files)} files under '{base_dir}'")
    out: List[Tuple[str, Optional[int], Optional[int]]] = []
    for p in files:
        out.extend(_parse_file_to_rows(p))

    uniq: List[Tuple[str, Optional[int], Optional[int]]] = []
    seen = set()
    for tup in out:
        key = (tup[0], tup[1], tup[2])
        if key in seen:
            continue
        seen.add(key)
        uniq.append(tup)
    log.info(f"[PARSE] extracted rows: {len(uniq)}")
    if uniq[:5]:
        log.info(f"[PARSE] sample: {uniq[:5]}")
    return uniq

def build_init_data_from_sources(base_dir: str = PDF_BASE_DIR, limit: int = 1000) -> List[Dict]:
    candidates = _gather_candidates_from_files(base_dir)
    if not candidates:
        log.info("[INIT] no parsed rows; returning empty list")
        return []
    results = _rows_to_data_dicts(candidates, limit=limit)
    log.info(f"[INIT] built {len(results)} rows for init_data")
    return results

# ---------------- 전역 init_data(지연 생성) ----------------
init_data: List[Dict] = []

def refresh_init_data(base_dir: str = PDF_BASE_DIR, limit: int = 1000) -> List[Dict]:
    global init_data
    init_data = build_init_data_from_sources(base_dir, limit=limit)
    return init_data

# ---------------- DB 업서트 ----------------
def _upsert_row_dict(d: Dict) -> Tuple[bool, bool]:
    name = _normalize_spaces(d.get("res_name") or "")[:64]
    addr = _normalize_spaces(d.get("address")  or "")[:255]

    if (name == "") and (addr == ""):
        return (False, False)

    d["res_name"] = name
    d["address"]  = addr

    if name != "" and addr != "":
        row = RestaurantInfo.query.filter_by(res_name=name, address=addr).first()
    elif addr != "":
        row = RestaurantInfo.query.filter_by(address=addr).first()
    else:
        row = RestaurantInfo.query.filter_by(res_name=name).first()

    if row:
        touched = False
        new_lat = d.get("lat"); new_lng = d.get("lng")
        if isinstance(new_lat, (int, float)) and isinstance(new_lng, (int, float)):
            if (new_lat != 0.0 or new_lng != 0.0):
                if (row.lat is None) or (row.lng is None) or (row.lat != new_lat or row.lng != new_lng):
                    row.lat = float(new_lat); row.lng = float(new_lng); touched = True

        if d.get("price") is not None and row.price != d["price"]:
            row.price = d["price"]; touched = True

        for field in ["category", "res_phone", "score"]:
            if d.get(field) is not None and getattr(row, field) != d[field]:
                setattr(row, field, d[field]); touched = True

        if (getattr(row, "res_name", None) is None) and (name is not None):
            row.res_name = name; touched = True
        if (getattr(row, "address", None) is None) and (addr is not None):
            row.address = addr; touched = True

        if touched:
            db.session.add(row)
            return (False, True)
        return (False, False)

    db.session.add(RestaurantInfo(**d))
    return (True, False)

# ---------------- 스트리밍 업서트 ----------------
def scan_and_upsert_streaming(base_dir: str = PDF_BASE_DIR, limit: int = 0, commit_every: int = 200):
    files = _list_source_files(base_dir)
    log.info(f"[STREAM] sources: {len(files)} files under '{base_dir}'")

    created = updated = skipped = 0
    built_total = 0
    batch = 0

    seen_pair_global = set()

    for idx, path in enumerate(files, 1):
        try:
            rows = _parse_file_to_rows(path)
        except Exception as e:
            log.warning(f"[STREAM] parse fail {path}: {e}")
            continue

        if not rows:
            log.info(f"[STREAM] no rows: {path}")
            continue

        left = (limit - built_total) if (limit and limit > 0) else 10**9
        data_dicts = _rows_to_data_dicts(rows, limit=left)
        before = len(data_dicts)

        pruned = []
        for d in data_dicts:
            key = (_normalize_spaces(d.get("res_name") or ""), _normalize_spaces(d.get("address") or ""))
            if key in seen_pair_global:
                continue
            seen_pair_global.add(key)
            pruned.append(d)
        data_dicts = pruned

        log.info(f"[STREAM] {os.path.basename(path)} → rows:{len(rows)} → cand:{before} → uniq:{len(data_dicts)}")

        for d in data_dicts:
            c, u = _upsert_row_dict(d)
            if c: created += 1
            elif u: updated += 1
            else: skipped += 1

            batch += 1
            built_total += 1
            if commit_every and batch >= commit_every:
                try:
                    db.session.commit()
                except Exception as e:
                    db.session.rollback()
                    log.exception(f"[STREAM] commit error: {e}")
                batch = 0

            if limit and built_total >= limit:
                break

        log.info(f"[STREAM] progress {idx}/{len(files)}  total_built={built_total} (created={created}, updated={updated}, skipped={skipped})")
        if limit and built_total >= limit:
            break

    try:
        db.session.commit()
        log.info(f"✅ STREAM done: 추가 {created}건, 업데이트 {updated}건, 스킵 {skipped}건, 총 {built_total}건 반영")
    except Exception as e:
        db.session.rollback()
        log.exception(f"❌ STREAM commit 실패: {e}")

# ---------------- 호환용 엔트리 ----------------
def refresh_init_data_and_insert(base_dir: Optional[str] = None, limit: int = 1000):
    base = base_dir or PDF_BASE_DIR
    log.info("[INIT] streaming upsert start")
    scan_and_upsert_streaming(base_dir=base, limit=limit, commit_every=200)
    log.info("[INIT] streaming upsert done")

def refresh_init_data_and_insert_streaming(
    *,
    base_dir: Optional[str] = None,
    scan_limit: Optional[int] = None,
    limit: Optional[int] = None,
    commit_every: Optional[int] = None,
    chunk_size: Optional[int] = None,
    batch_size: Optional[int] = None,
    require_both: Optional[bool] = None,
    allow_no_geocode: Optional[bool] = None
):
    base = base_dir or PDF_BASE_DIR

    if isinstance(limit, int):
        eff_limit = max(0, limit)
    elif isinstance(scan_limit, int):
        eff_limit = max(0, scan_limit)
    else:
        eff_limit = 0

    if isinstance(commit_every, int) and commit_every > 0:
        eff_commit_every = commit_every
    elif isinstance(chunk_size, int) and chunk_size > 0:
        eff_commit_every = chunk_size
    elif isinstance(batch_size, int) and batch_size > 0:
        eff_commit_every = batch_size
    else:
        eff_commit_every = 200

    log.info(f"[INIT] streaming upsert start (eff_limit={eff_limit}, eff_commit_every={eff_commit_every})")
    scan_and_upsert_streaming(base_dir=base, limit=eff_limit, commit_every=eff_commit_every)
    log.info("[INIT] streaming upsert done")

def insert_initial_restaurants(base_dir: Optional[str] = None, limit: int = 1000):
    if not init_data:
        refresh_init_data(base_dir or PDF_BASE_DIR, limit=limit)

    created, updated, skipped = 0, 0, 0
    for data in init_data:
        try:
            c, u = _upsert_row_dict(data)
            if c: created += 1
            elif u: updated += 1
            else: skipped += 1
        except Exception as e:
            log.exception(f"[INIT] upsert error: {e}")
    try:
        db.session.commit()
        print(f"✅ 식당 정보 시드 완료: 추가 {created}건, 업데이트 {updated}건, 스킵 {skipped}건")
    except Exception as e:
        db.session.rollback()
        import traceback; traceback.print_exc()
        print(f"❌ DB commit 실패: {repr(e)}  (created={created}, updated={updated}, skipped={skipped})")
