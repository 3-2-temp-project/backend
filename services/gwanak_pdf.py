from __future__ import annotations
import os, re, time, random
from typing import Callable, Optional, Tuple, List, Dict
from urllib.parse import urljoin, urlparse, urlunparse, unquote, parse_qs, urlunparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
import requests
from bs4 import BeautifulSoup

BASE_LIST_URL = "https://www.ga21c.seoul.kr/kr/costBBS.do"
DEFAULT_SAVE_DIR = os.path.join(os.getcwd(), "pdf_data", "관악구")

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
CONNECT_TIMEOUT = 15
READ_TIMEOUT = 45

ALLOWED_EXTS = {".xlsx", ".xls", ".pdf", ".hwp", ".hwpx", ".zip"}
PDF_MAGIC = b"%PDF-"
ZIP_MAGIC = b"PK\x03\x04"

ATTENDANCE_PAT = re.compile(
    r"(참석자\s*명단|참가자\s*명단|참여자\s*명단|출석부|출석\s*현황|참석\s*현황|attend(?:ee|ance))",
    re.I,
)

# onclick: fn_download('atch','sn','name') 등 down을 포함한 함수 전반 허용
_ONCLICK_RE = re.compile(
    r"""(?P<fn>\w*down\w*)\s*\(\s*'(?P<atch>[^']+)'\s*,\s*'(?P<sn>[^']+)'\s*(?:,\s*'(?P<nm>[^']*)'\s*)?\)""",
    re.I
)

def _safe_name(s: str) -> str:
    s = re.sub(r"[\\/:*?\"<>|]+", "_", (s or "").strip())
    s = re.sub(r"\s+", " ", s)
    return s[:180] if s else "파일"

def _unique_path(dirpath: str, filename: str) -> str:
    os.makedirs(dirpath, exist_ok=True)
    base = _safe_name(filename)
    out  = os.path.join(dirpath, base)
    if not os.path.exists(out):
        return out
    stem, ext = os.path.splitext(base)
    i = 2
    while True:
        cand = os.path.join(dirpath, f"{stem}({i}){ext}")
        if not os.path.exists(cand):
            return cand
        i += 1

def _parse_date_in_text(text: str) -> Optional[date]:
    m = re.search(r"(\d{4})[-./](\d{1,2})[-./](\d{1,2})", text or "")
    if not m:
        return None
    yy, mo, dd = int(m.group(1)), int(m.group(2)), int(m.group(3))
    try:
        return date(yy, mo, dd)
    except Exception:
        return None

def _cutoff_month_start(months: int) -> date:
    today = date.today()
    y, m = today.year, today.month
    m -= (months - 1)
    while m <= 0:
        y -= 1
        m += 12
    return date(y, m, 1)

def _looks_like_attendance(s: Optional[str]) -> bool:
    return bool(s and ATTENDANCE_PAT.search(s))

def _normalize_view_url(u: str) -> str:
    try:
        pu = urlparse(u)
        if ("costBBSview" in pu.path) and (not pu.path.startswith("/kr/")):
            new_path = "/kr" + pu.path if not pu.path.startswith("/kr") else pu.path
            return urlunparse((pu.scheme, pu.netloc, new_path, pu.params, pu.query, pu.fragment))
    except Exception:
        pass
    return u

def _build_session(session: Optional[requests.Session] = None) -> requests.Session:
    s = session or requests.Session()
    s.headers.update({
        "User-Agent": UA,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8",
        "Connection": "keep-alive",
    })
    return s

def _fetch(session: requests.Session, url: str, params=None, referer: Optional[str]=None,
           stream: bool=False, method: str="GET", data=None) -> Optional[requests.Response]:
    headers = {}
    if referer:
        headers["Referer"] = referer
        try:
            pu = urlparse(referer)
            headers["Origin"] = f"{pu.scheme}://{pu.netloc}"
        except Exception:
            pass
    for i in range(1, 4):
        try:
            if method.upper() == "POST":
                r = session.post(url, params=params, data=data, headers=headers,
                                 stream=stream, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT))
            else:
                r = session.get(url, params=params, headers=headers,
                                stream=stream, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT))
            if r.status_code == 200:
                return r
        except Exception:
            pass
        time.sleep(0.3 * i + random.random() * 0.3)
    return None

def _fetch_list_page(session: requests.Session, page_index: int) -> Optional[str]:
    r = _fetch(session, BASE_LIST_URL, params={"page": page_index}, referer=BASE_LIST_URL)
    return r.text if r else None

def _parse_list(list_html: str) -> List[Dict]:
    soup = BeautifulSoup(list_html, "html.parser")
    rows: List[Dict] = []

    for tr in soup.select("table tr"):
        a = tr.select_one("a[href*='costBBSview']")
        if not a:
            continue
        href = (a.get("href") or "").strip()
        title = a.get_text(" ", strip=True)
        row_text = " ".join(td.get_text(" ", strip=True) for td in tr.find_all("td"))
        d = _parse_date_in_text(row_text)
        absu = _normalize_view_url(urljoin(BASE_LIST_URL, href))
        rows.append({"title": title, "href": absu, "date": d})

    if not rows:
        for li in soup.select("li"):
            a = li.select_one("a[href*='costBBSview']")
            if not a:
                continue
            href = (a.get("href") or "").strip()
            title = a.get_text(" ", strip=True)
            li_text = li.get_text(" ", strip=True)
            d = _parse_date_in_text(li_text)
            absu = _normalize_view_url(urljoin(BASE_LIST_URL, href))
            rows.append({"title": title, "href": absu, "date": d})

    return rows

def _collect_recent_posts(session: requests.Session, months: int, max_pages: int = 20) -> List[Dict]:
    cut = _cutoff_month_start(months)
    page, posts, saw_newer = 1, [], False
    while True:
        html = _fetch_list_page(session, page)
        if not html:
            break
        items = _parse_list(html)
        if not items:
            break
        older_only = True
        for it in items:
            d = it.get("date")
            if d is None or d >= cut:
                posts.append(it)
                older_only = False
                saw_newer = True
        if older_only and saw_newer:
            break
        page += 1
        if page > max_pages:
            break
        time.sleep(0.12 + random.random() * 0.18)
    return posts

def _fetch_post_html(session: requests.Session, view_url: str) -> Optional[str]:
    r = _fetch(session, view_url, referer=BASE_LIST_URL)
    return r.text if r else None

def _onclick_to_candidate_urls(base_url: str, atch: str, sn: str, nm: Optional[str]) -> List[str]:
    qs_nm = f"&fileNm={nm}" if nm else ""
    base_candidates = [
        f"/kr/cmmn/file/fileDown.do?atchFileId={atch}&fileSn={sn}{qs_nm}",
        f"/kr/cmmn/file/download.do?atchFileId={atch}&fileSn={sn}{qs_nm}",
        f"/cmmn/file/fileDown.do?atchFileId={atch}&fileSn={sn}{qs_nm}",
        f"/cmmn/file/download.do?atchFileId={atch}&fileSn={sn}{qs_nm}",
    ]
    return [urljoin(base_url, c) for c in base_candidates]

def _collect_attachment_urls(post_html: str, base_url: str) -> List[Tuple[str, str]]:
    soup = BeautifulSoup(post_html, "html.parser")
    pairs: List[Tuple[str, str]] = []

    # href 직접 링크
    for a in soup.select("a[href]"):
        href = (a.get("href") or "").strip()
        if not href:
            continue
        absu = urljoin(base_url, href)
        low = absu.lower()
        if ("download.do" in low and "bbs_id=cost" in low) or re.search(r"\.(xlsx|xls|pdf|hwp|hwpx|zip)(\?|$)", low):
            label = a.get_text(" ", strip=True) or os.path.basename(urlparse(absu).path)
            if _looks_like_attendance(label) or _looks_like_attendance(absu):
                continue
            pairs.append((absu, label))

    # onclick 패턴
    for a in soup.select("a[onclick]"):
        onclick = (a.get("onclick") or "").strip()
        m = _ONCLICK_RE.search(onclick)
        if not m:
            continue
        atch, sn, nm = m.group("atch"), m.group("sn"), m.group("nm")
        label = a.get_text(" ", strip=True) or (nm or "첨부파일")
        for url in _onclick_to_candidate_urls(base_url, atch, sn, nm):
            pairs.append((url, label))

    # uniq
    seen, out = set(), []
    for u, t in pairs:
        if u in seen:
            continue
        seen.add(u)
        out.append((u, t))
    return out

def _decode_disp_filename(headers) -> Optional[str]:
    disp = headers.get("Content-Disposition") or headers.get("content-disposition") or ""
    m = re.search(r"filename\*=\s*(?:UTF-8''|UTF-8'ko-kr')?([^;]+)", disp, re.I)
    if m:
        raw = m.group(1).strip().strip('"')
        for enc in ("utf-8", "cp949"):
            try:
                return unquote(raw, encoding=enc, errors="strict")
            except Exception:
                pass
        return unquote(raw)
    m = re.search(r'filename\s*=\s*"([^"]+)"', disp, re.I) or re.search(r'filename\s*=\s*([^;]+)', disp, re.I)
    if m:
        raw = m.group(1).strip().strip('"')
        for enc in ("utf-8", "cp949"):
            try:
                return raw.encode("latin-1", "strict").decode(enc, "strict")
            except Exception:
                pass
        return raw
    return None

def _ensure_allowed_or_guess(resp: requests.Response, url_hint_name: str):
    ctype = (resp.headers.get("Content-Type") or "").lower()
    name  = _decode_disp_filename(resp.headers) \
            or os.path.basename(urlparse(url_hint_name).path) \
            or "download"
    root, ext = os.path.splitext(name)
    ext = ext.lower()

    stream = resp.iter_content(chunk_size=8192)
    try:
        first = next(stream)
    except StopIteration:
        first = b""

    if ext in ALLOWED_EXTS:
        return name, ext, first, stream
    if first.startswith(ZIP_MAGIC) or "zip" in ctype or "vnd.openxmlformats" in ctype or "excel" in ctype:
        guessed = ".xlsx"
        lname = name.lower()
        if "hwpx" in lname or "hwp.x" in lname:
            guessed = ".hwpx"
        return (root or "download") + guessed, guessed, first, stream
    if first.startswith(PDF_MAGIC) or "pdf" in ctype:
        return (root or "download") + ".pdf", ".pdf", first, stream
    if "hwp" in ctype:
        return (root or "download") + ".hwp", ".hwp", first, stream
    return None

def _try_get_then_post(session: requests.Session, att_url: str, referer: str, stream: bool=True) -> Optional[requests.Response]:
    r = _fetch(session, att_url, referer=referer, stream=stream, method="GET")
    if r and r.status_code == 200:
        return r
    pu = urlparse(att_url)
    data = parse_qs(pu.query) if pu.query else None
    r2 = _fetch(
        session,
        url=urlunparse((pu.scheme, pu.netloc, pu.path, "", "", "")),
        referer=referer,
        stream=stream,
        method="POST",
        data=data
    )
    if r2 and r2.status_code == 200:
        return r2
    return None

def _download_one(
    session: requests.Session,
    att_url: str,
    referer: str,
    post_title: str,
    link_label: str,
    save_dir: str,
    prefix_name: str,
    log_fn: Callable[[str], None],
) -> int:
    try:
        r = _try_get_then_post(session, att_url, referer=referer, stream=True)
        if not r:
            log_fn(f"FAIL {att_url}")
            return 0

        disp_name = _decode_disp_filename(r.headers)
        if _looks_like_attendance(link_label) or _looks_like_attendance(att_url) or _looks_like_attendance(disp_name):
            return 0

        checked = _ensure_allowed_or_guess(r, att_url)
        if not checked:
            log_fn(f"FAIL {att_url}")
            return 0

        fname, _, first, stream = checked
        prefer     = _safe_name(link_label) if link_label else _safe_name(fname)
        final_name = f"{prefix_name} {_safe_name(post_title)}__{prefer}"

        # prefer에 확장자가 없고 fname에는 있으면 붙여준다
        if not os.path.splitext(final_name)[1]:
            _, ext0 = os.path.splitext(fname)
            if ext0:
                final_name += ext0

        save_to    = _unique_path(save_dir, final_name)
        with open(save_to, "wb") as f:
            if first:
                f.write(first)
            for chunk in stream:
                if chunk:
                    f.write(chunk)
        log_fn(f"OK {os.path.basename(save_to)}")
        return 1
    except Exception:
        log_fn(f"FAIL {att_url}")
        return 0

def _process_post(
    session: requests.Session,
    item: Dict,
    save_dir: str,
    prefix_name: str,
    log_fn: Callable[[str], None],
) -> int:
    title = item.get("title") or "제목없음"
    href  = item.get("href")
    html  = _fetch_post_html(session, href)
    if not html:
        return 0
    pairs = _collect_attachment_urls(html, href)
    if not pairs:
        return 0
    saved = 0
    for u, label in pairs:
        saved += _download_one(session, u, referer=href, post_title=title, link_label=label,
                               save_dir=save_dir, prefix_name=prefix_name, log_fn=log_fn)
        time.sleep(0.03 + random.random() * 0.07)
    return saved

def run_gwanak(
    months: int = 2,
    save_dir: Optional[str] = None,
    threads: int = 6,
    prefix_name: str = "관악구의회",
    session: Optional[requests.Session] = None,
    logger: Optional[Callable[[str], None]] = None,
) -> Dict[str, int]:
    """
    관악구의회 업무추진비 게시판에서 최근 N개월 첨부 다운로드.
    return: {"posts": 수집글수, "files": 저장파일수}
    """
    save_dir = save_dir or DEFAULT_SAVE_DIR
    os.makedirs(save_dir, exist_ok=True)

    sess = _build_session(session)
    log_fn = logger if logger else (lambda msg: print(msg, flush=True))

    posts = _collect_recent_posts(sess, months)
    ok = 0
    if posts:
        if threads and threads > 1:
            with ThreadPoolExecutor(max_workers=threads) as ex:
                futs = [ex.submit(_process_post, sess, it, save_dir, prefix_name, log_fn) for it in posts]
                for fu in as_completed(futs):
                    try:
                        ok += int(fu.result() or 0)
                    except Exception:
                        pass
        else:
            for it in posts:
                ok += _process_post(sess, it, save_dir, prefix_name, log_fn)

    return {"posts": len(posts), "files": ok}

if __name__ == "__main__":
    def _env_int(name: str, default: int) -> int:
        v = os.getenv(name, "").strip()
        if not v:
            return default
        try:
            return max(1, min(32, int(v)))
        except Exception:
            return default

    months  = _env_int("MONTHS", 2)
    threads = _env_int("THREADS", 6)
    save    = os.getenv("GWANAK_SAVE_DIR", DEFAULT_SAVE_DIR)
    res = run_gwanak(months=months, save_dir=save, threads=threads)
    print(f"SUMMARY posts={res['posts']} files={res['files']}", flush=True)
