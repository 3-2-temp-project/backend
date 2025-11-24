#!/usr/bin/env python3
"""
지자체 의회 업무추진비 공개 페이지 파일 다운로드 스크립트 v5.0
현재 날짜 기준 3개월치 파일 다운로드 (현재월 포함)

v5.0 주요 개선사항:
- 병렬 처리 도입 (멀티프로세싱): 여러 사이트 동시 처리로 실행 시간 단축
- 파일명 인코딩/디코딩 처리 강화: UTF-8, EUC-KR, CP949 등 다양한 인코딩 지원 개선
- 사이트명 추출 로직 강화: "www" 대신 정확한 지자체명 반영
- 다운로드 재시도 로직 강화: "바로보기" 링크 처리 개선
- 로깅 및 진행 표시 개선: tqdm 진행바 추가
"""

import os
import re
import time
import json
import logging
import hashlib
import random
import concurrent.futures
from datetime import datetime, timedelta
from urllib.parse import urljoin, urlparse, unquote, parse_qs, urlencode, quote
import requests
from bs4 import BeautifulSoup
from typing import Dict, List, Tuple, Optional, Set, Any, Union
import warnings
warnings.filterwarnings('ignore')
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor


EXECUTOR_KIND_DEFAULT = os.getenv("DOWNLOADER_EXECUTOR", "process").lower()
RUNNING_IN_FLASK_DEFAULT = os.getenv("RUN_FROM_FLASK", "0").lower() in ("1", "true", "yes", "y")

def process_sites_parallel(self, urls: List[str]) -> List[Dict]:
    all_stats = []

    # 순차
    if self.max_workers <= 1:
        for i, url in enumerate(urls, 1):
            logger.info(f"\n{'▶'*3} 진행: {i}/{len(urls)} ({i/len(urls)*100:.1f}%)")
            stats = self.process_site(url)
            all_stats.append(stats)
            self.stats[stats['site_name']] = stats
            if i < len(urls):
                time.sleep(random.uniform(1.0, 2.0))
        return all_stats

    # ★ Flask 내부에서는 프로세스 풀 금지 → 스레드 풀 사용
    if self.running_in_flask or self.executor_kind == "thread":
        logger.info(f"🧵 ThreadPoolExecutor 사용 (workers={self.max_workers})")
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            if TQDM_AVAILABLE:
                for stats in tqdm(executor.map(self.process_site, urls),
                                  total=len(urls), desc="사이트 처리 중", unit="site"):
                    all_stats.append(stats)
            else:
                for stats in executor.map(self.process_site, urls):
                    all_stats.append(stats)
        for st in all_stats:
            self.stats[st['site_name']] = st
        return all_stats

    # (옵션) 독립 프로세스 모드 — CLI에서만 (Flask X)
    # 바운드 메서드 피클링을 피하기 위해 모듈 최상위 함수 사용
    def _worker_payload(url):
        return {"url": url, "use_selenium": self.use_selenium}

    logger.info(f"⚙️ ProcessPoolExecutor 사용 (workers={self.max_workers})")
    with ProcessPoolExecutor(max_workers=self.max_workers) as executor:
        # tqdm 분기
        it = executor.map(_worker_process_site, map(_worker_payload, urls))
        results = list(tqdm(it, total=len(urls), desc="사이트 처리 중", unit="site")) if TQDM_AVAILABLE else list(it)
        all_stats.extend(results)
    for st in all_stats:
        self.stats[st['site_name']] = st
    return all_stats

# 모듈 최상위(클래스 밖)에 추가 — 프로세스용 워커
def _worker_process_site(payload: dict) -> dict:
    url = payload["url"]
    use_selenium = payload.get("use_selenium", False)
    # 각 프로세스는 독립 다운로더(내부에서 다시 멀티 안씀)
    dl = CouncilFileDownloader(use_selenium=use_selenium, max_workers=1)
    return dl.process_site(url)

# 진행 표시 도구
try:
    from tqdm import tqdm
    TQDM_AVAILABLE = True
except ImportError:
    TQDM_AVAILABLE = False

# Selenium 옵션
try:
    from selenium import webdriver
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.chrome.options import Options
    from selenium.common.exceptions import TimeoutException, NoSuchElementException
    SELENIUM_AVAILABLE = True
except ImportError:
    SELENIUM_AVAILABLE = False

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('download_log.txt', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# 멀티프로세싱 로그 격리
from multiprocessing import current_process
class ProcessNameFilter(logging.Filter):
    def filter(self, record):
        record.processName = current_process().name
        return True

for handler in logger.handlers:
    handler.addFilter(ProcessNameFilter())

class FileDeduplicator:
    """파일 중복 제거 관리자"""
    
    def __init__(self):
        self.seen_urls = set()
        self.seen_filenames = set()
        self.seen_hashes = set()
        self.url_to_filename = {}
        
    def normalize_url(self, url: str) -> str:
        """URL 정규화"""
        if not url:
            return ""
        
        # 개행문자, 불필요한 공백 제거
        url = re.sub(r'[\r\n\t]', '', url)
        url = re.sub(r'\s+', ' ', url)
        
        # 파라미터 정렬
        parsed = urlparse(url)
        if parsed.query:
            params = parse_qs(parsed.query)
            sorted_params = sorted(params.items())
            query = urlencode(sorted_params, doseq=True)
            url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}?{query}"
        
        # 불필요한 파라미터 제거
        remove_params = ['timestamp', 'ts', '_', 'random', 'cache']
        parsed = urlparse(url)
        if parsed.query:
            params = parse_qs(parsed.query)
            for param in remove_params:
                params.pop(param, None)
            if params:
                query = urlencode(params, doseq=True)
                url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}?{query}"
            else:
                url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
        
        return url.lower().strip()
    
    def is_duplicate_url(self, url: str) -> bool:
        """URL 중복 체크"""
        normalized = self.normalize_url(url)
        if normalized in self.seen_urls:
            return True
        self.seen_urls.add(normalized)
        return False
    
    def is_duplicate_filename(self, filename: str, url: str = None) -> bool:
        """파일명 중복 체크"""
        key = filename.lower().strip()
        
        # 동일 URL에서 온 파일은 허용
        if url:
            normalized_url = self.normalize_url(url)
            if normalized_url in self.url_to_filename:
                if self.url_to_filename[normalized_url] == key:
                    return False
            self.url_to_filename[normalized_url] = key
        
        if key in self.seen_filenames:
            return True
        self.seen_filenames.add(key)
        return False
    
    def get_file_hash(self, filepath: str) -> str:
        """파일 해시 계산"""
        hasher = hashlib.md5()
        try:
            with open(filepath, 'rb') as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    hasher.update(chunk)
            return hasher.hexdigest()
        except Exception:
            return None
    
    def is_duplicate_content(self, filepath: str) -> bool:
        """파일 내용 중복 체크"""
        file_hash = self.get_file_hash(filepath)
        if not file_hash:
            return False
        
        if file_hash in self.seen_hashes:
            return True
        self.seen_hashes.add(file_hash)
        return False
    
    def get_stats(self) -> Dict:
        """중복 제거 통계"""
        return {
            'unique_urls': len(self.seen_urls),
            'unique_filenames': len(self.seen_filenames),
            'unique_contents': len(self.seen_hashes)
        }

class CouncilFileDownloader:
    def __init__(self, use_selenium=False, max_workers=4):
        
        # User-Agent 목록 확장 (self.session 생성보다 먼저 정의되어야 함)
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Edge/120.0.0.0'
        ]
        
        self.session = self._create_session()
        
        # 중복 제거 관리자
        self.deduplicator = FileDeduplicator()
        
        # 병렬 처리 설정
        self.max_workers = max_workers
        
        # Selenium 드라이버
        self.driver = None
        self.use_selenium = use_selenium
        if use_selenium and SELENIUM_AVAILABLE:
            try:
                self._init_selenium()
                logger.info("Selenium 드라이버 초기화 성공")
            except Exception as e:
                logger.warning(f"Selenium 드라이버 초기화 실패: {e}")
        
        self.current_date = datetime.now()
        self.target_months = self.get_target_months()
        
        self.base_download_dir = f"downloads_{self.current_date.strftime('%Y%m%d_%H%M')}"
        if not os.path.exists(self.base_download_dir):
            os.makedirs(self.base_download_dir)
        
        # 사이트-지자체명 매핑 로드
        self.site_name_mapping = self._load_site_name_mapping()
        
        self.stats = {}
        
    def _create_session(self) -> requests.Session:
        """세션 생성 및 기본 설정"""
        session = requests.Session()
        session.headers.update({
            'User-Agent': random.choice(self.user_agents),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Cache-Control': 'max-age=0'
        })
        return session
    
    def _init_selenium(self):
        """Selenium 초기화"""
        if not SELENIUM_AVAILABLE:
            return
            
        chrome_options = Options()
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('--disable-blink-features=AutomationControlled')
        chrome_options.add_argument(f'user-agent={random.choice(self.user_agents)}')
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        self.driver = webdriver.Chrome(options=chrome_options)
    
    def _load_site_name_mapping(self) -> Dict[str, str]:
        """사이트-지자체명 매핑"""
        return {
            # 서울시 자치구
            'ydp.go.kr': '영등포구',
            'dongjak.go.kr': '동작구',
            'assembly.dongjak.go.kr': '동작구',
            'yscl.go.kr': '용산구',
            'gwangjin.go.kr': '광진구',
            'council.gwangjin.go.kr': '광진구',
            'seocho.go.kr': '서초구',
            'gangdong.go.kr': '강동구',
            'mapo.seoul.kr': '마포구',
            'council.mapo.seoul.kr': '마포구',
            'ddm.go.kr': '동대문구',
            'sb.go.kr': '성북구',
            'dobong.go.kr': '도봉구',
            'nowon.kr': '노원구',
            'gangseo.seoul.kr': '강서구',
            'ycc.go.kr': '양천구',
            'guro.go.kr': '구로구',
            'geumcheon.go.kr': '금천구',
            'songpa.go.kr': '송파구',
            'gangnam.go.kr': '강남구',
            'ep.go.kr': '은평구',
            'council.ep.go.kr': '은평구',
            'jongno.go.kr': '종로구',
            'sd.go.kr': '성동구',
            'jungnang.go.kr': '중랑구',
            'gangbuk.go.kr': '강북구',
            'council.gangbuk.go.kr': '강북구',
            'junggu.seoul.kr': '중구',
            
            # 경기도
            'suwon.go.kr': '수원시',
            'council.suwon.go.kr': '수원시',
            'goyang.go.kr': '고양시',
            'yongin.go.kr': '용인시',
            'seongnam.go.kr': '성남시',
            'bucheon.go.kr': '부천시',
            'ansan.go.kr': '안산시',
            'anyang.go.kr': '안양시',
            'namyangju.go.kr': '남양주시',
            'hwaseong.go.kr': '화성시',
            'pyeongtaek.go.kr': '평택시',
            'uijeongbu.go.kr': '의정부시',
            'siheung.go.kr': '시흥시',
            'gimpo.go.kr': '김포시',
            'gwangju.go.kr': '광주시',
            'gwangmyeong.go.kr': '광명시',
            'gunpo.go.kr': '군포시',
            'osan.go.kr': '오산시',
            'icheon.go.kr': '이천시',
            'yangju.go.kr': '양주시',
            'anseong.go.kr': '안성시',
            'guri.go.kr': '구리시',
            'pocheon.go.kr': '포천시',
            'uiwang.go.kr': '의왕시',
            'hanam.go.kr': '하남시',
            'paju.go.kr': '파주시',
            'yangpyeong.go.kr': '양평군',
            'yeoju.go.kr': '여주시',
            'dongducheon.go.kr': '동두천시',
            'gapyeong.go.kr': '가평군',
            'yeoncheon.go.kr': '연천군',
            
            # 특수 사이트
            'sscf2016.or.kr': '서초구재단',
        }
    
    def get_target_months(self) -> List[Tuple[int, int]]:
        """대상 월 계산"""
        months = []
        current = self.current_date
        
        for i in range(3):
            months.append((current.year, current.month))
            if current.month == 1:
                current = current.replace(year=current.year - 1, month=12)
            else:
                current = current.replace(month=current.month - 1)
        
        logger.info(f"대상 기간: {months}")
        return months
    
    def is_target_date(self, text: str) -> Tuple[bool, Optional[str]]:
        """날짜 확인 - 개선된 버전"""
        if not text:
            return False, None
        
        # 공백 정규화
        text = ' '.join(text.split())
        
        patterns = [
            (r'(\d{4})[\s\-\.년/](\d{1,2})[\s\-\.월]?', 'full'),
            (r'(\d{4})(\d{2})', 'compact'),
            (r'(\d{2})[\s\-\.년](\d{1,2})[\s\-\.월]', 'short'),
        ]
        
        for pattern, format_type in patterns:
            matches = re.findall(pattern, text)
            for match in matches:
                try:
                    if format_type == 'short':
                        year = 2000 + int(match[0])
                    else:
                        year = int(match[0])
                    month = int(match[1])
                    
                    if 1 <= month <= 12 and (year, month) in self.target_months:
                        return True, f"{year}년 {month}월"
                except:
                    continue
        
        # 한글 월 이름
        month_map = {
            '1월': 1, '2월': 2, '3월': 3, '4월': 4, '5월': 5, '6월': 6,
            '7월': 7, '8월': 8, '9월': 9, '10월': 10, '11월': 11, '12월': 12,
            '일월': 1, '이월': 2, '삼월': 3, '사월': 4, '오월': 5, '유월': 6,
            '칠월': 7, '팔월': 8, '구월': 9, '시월': 10, '십일월': 11, '십이월': 12
        }
        
        for month_name, month_num in month_map.items():
            if month_name in text:
                year_match = re.search(r'(\d{4})년?', text)
                year = int(year_match.group(1)) if year_match else self.current_date.year
                
                if (year, month_num) in self.target_months:
                    return True, f"{year}년 {month_num}월"
                if (year - 1, month_num) in self.target_months:
                    return True, f"{year - 1}년 {month_num}월"
        
        return False, None
    
    def get_site_name(self, url: str) -> str:
        """사이트 이름 추출 - 강화된 버전"""
        domain = urlparse(url).netloc.lower()
        path = urlparse(url).path.lower()
        
        # 직접 매핑 시도 (전체 도메인)
        if domain in self.site_name_mapping:
            return self.site_name_mapping[domain]
        
        # 도메인 부분 매칭
        for key, name in self.site_name_mapping.items():
            if key in domain:
                return name
        
        # council 특수 처리 (의회 사이트)
        if 'council' in domain:
            domain_parts = domain.split('.')
            if len(domain_parts) >= 3:
                council_site = '.'.join(domain_parts[1:])
                if council_site in self.site_name_mapping:
                    return f"{self.site_name_mapping[council_site]}의회"
                
                # council.XXX.go.kr 패턴
                middle_domain = domain_parts[1]
                for key, name in self.site_name_mapping.items():
                    if middle_domain in key:
                        return f"{name}의회"
                        
            return '의회'
        
        # www 특수 처리
        if domain.startswith('www.'):
            domain_without_www = domain[4:]
            if domain_without_www in self.site_name_mapping:
                return self.site_name_mapping[domain_without_www]
            
            for key, name in self.site_name_mapping.items():
                if key in domain_without_www:
                    return name
        
        # 패스에서 힌트 찾기
        if 'council' in path or 'assembly' in path:
            for key, name in self.site_name_mapping.items():
                if key in domain:
                    return f"{name}의회"
        
        # 최후의 수단: 도메인 첫 부분
        domain_parts = domain.split('.')
        if len(domain_parts) > 0 and domain_parts[0] != 'www' and len(domain_parts[0]) > 2:
            return domain_parts[0]
        
        # 두 번째 부분이 의미있는 경우 (www.xxx.go.kr)
        if len(domain_parts) > 1 and domain_parts[0] == 'www' and len(domain_parts[1]) > 2:
            return domain_parts[1]
        
        return domain
    
    def build_download_url_variants(self, base_url: str, file_id: str, file_name: str = '') -> List[str]:
        """다양한 다운로드 URL 패턴 생성"""
        parsed = urlparse(base_url)
        base = f"{parsed.scheme}://{parsed.netloc}"
        
        # 가능한 모든 다운로드 URL 패턴
        patterns = [
            f"{base}/common/download.do?fileId={file_id}",
            f"{base}/common/fileDown.do?fileId={file_id}",
            f"{base}/web/board/BD_fileDownload.do?fileNo={file_id}",
            f"{base}/file/download.do?atchFileId={file_id}",
            f"{base}/attach/download.do?fileSeq={file_id}",
            f"{base}/common/fileDown.do?file_id={file_id}",
            f"{base}/board/file_download.do?idx={file_id}",
            f"{base}/board/download.do?file_seq={file_id}",
            f"{base}/bbs/download.do?atchFileId={file_id}",
            f"{base}/cmm/fms/FileDown.do?atchFileId={file_id}",
            f"{base}/file.do?method=download&fileId={file_id}",
            f"{base}/common/downloadFile.do?fileId={file_id}",
            f"{base}/board/fileDownload.do?fileId={file_id}",
            f"{base}/cmm/fms/getFile.do?atchFileId={file_id}",
            f"{base}/cop/bbs/selectBoardArticleFile.do?atchFileId={file_id}",
            f"{base}/bbs/getBoardFile.do?fileId={file_id}",
            # 바로보기 관련 패턴
            f"{base}/common/viewer.do?fileId={file_id}",
            f"{base}/viewer.do?fileId={file_id}",
            f"{base}/fileViewer.do?fileId={file_id}",
            f"{base}/pdfjs/web/viewer.html?file={file_id}",
        ]
        
        # 파일명이 있으면 추가 패턴
        if file_name:
            encoded_name = quote(file_name)
            patterns.extend([
                f"{base}/download/{encoded_name}",
                f"{base}/files/{encoded_name}",
                f"{base}/attach/{encoded_name}",
                f"{base}/upload/{encoded_name}",
                f"{base}/data/download/{encoded_name}",
            ])
            
            # 바로보기 링크 처리
            if "바로보기" in file_name:
                file_name_cleaned = file_name.replace("바로보기", "").strip()
                if file_name_cleaned:
                    encoded_cleaned = quote(file_name_cleaned)
                    patterns.extend([
                        f"{base}/download/{encoded_cleaned}",
                        f"{base}/files/{encoded_cleaned}",
                        f"{base}/attach/{encoded_cleaned}",
                        f"{base}/upload/{encoded_cleaned}",
                    ])
        
        return patterns
    
    def clean_url(self, url: str) -> str:
        """URL 정리 - 개행문자 및 공백 제거"""
        if not url:
            return url
        
        # 개행문자, 탭, 공백 제거
        url = re.sub(r'[\r\n\t]', '', url)
        url = ' '.join(url.split())  # 중복 공백 제거
        url = url.strip()
        
        return url
    
    def extract_all_download_urls(self, soup: BeautifulSoup, base_url: str) -> List[Dict]:
        """모든 다운로드 URL 추출 - 강화 버전"""
        download_urls = []
        
        # 1. 바로보기/미리보기 링크 (최우선)
        for link in soup.find_all(['a', 'button', 'div']):
            text = link.get_text().strip()
            if '바로보기' in text or '미리보기' in text:
                href = link.get('href', '')
                onclick = link.get('onclick', '')
                data_link = link.get('data-link', '')
                
                potential_url = href or data_link
                if potential_url:
                    url = self.build_absolute_url(potential_url, base_url)
                    if url and not self.deduplicator.is_duplicate_url(url):
                        # 상위 요소에서 날짜 정보 추출 시도
                        parent = link.find_parent(['tr', 'li', 'div'])
                        parent_text = parent.get_text().strip() if parent else ""
                        is_target, date_str = self.is_target_date(parent_text)
                        
                        download_urls.append({
                            'url': url,
                            'type': 'preview',
                            'text': text,
                            'date': date_str if is_target else None,
                            'onclick': onclick
                        })
                
                # onclick 처리
                if onclick and ('window.open' in onclick or 'download' in onclick.lower()):
                    js_patterns = [
                        r"window\.open\(['\"]([^'\"]+)['\"]",
                        r"download\(['\"]?([^'\"]+)['\"]?",
                        r"fileDown\(['\"]?([^'\"]+)['\"]?",
                    ]
                    
                    for pattern in js_patterns:
                        matches = re.findall(pattern, onclick)
                        for match in matches:
                            preview_url = self.build_absolute_url(match, base_url)
                            if preview_url and not self.deduplicator.is_duplicate_url(preview_url):
                                download_urls.append({
                                    'url': preview_url,
                                    'type': 'preview-onclick',
                                    'text': text,
                                    'onclick': onclick
                                })
        
        # 2. href 기반 링크
        for link in soup.find_all('a', href=True):
            href = link.get('href', '')
            onclick = link.get('onclick', '')
            text = link.get_text().strip()
            title = link.get('title', '')
            
            # 파일 확장자 체크
            file_extensions = ['.pdf', '.xlsx', '.xls', '.hwp', '.doc', '.docx', '.zip', '.csv', '.hwpx']
            is_file_link = any(ext in href.lower() for ext in file_extensions)
            
            # 다운로드 키워드 체크
            download_keywords = ['download', 'fileDown', 'attachDown', 'file', 'attach',  
                                 'getFile', 'atchFile', 'boardFile', 'bbsFile', 'FileDown']
            is_download_link = any(kw in href.lower() or kw in onclick.lower()  
                                   for kw in download_keywords)
            
            if is_file_link or is_download_link:
                url = self.build_absolute_url(href, base_url)
                if url and not self.deduplicator.is_duplicate_url(url):
                    download_urls.append({
                        'url': url,
                        'type': 'direct',
                        'text': text,
                        'title': title,
                        'onclick': onclick
                    })
        
        # 3. onclick 기반 링크 - 확장된 패턴
        for link in soup.find_all(['a', 'button', 'span', 'div'], onclick=True):
            onclick = link.get('onclick', '')
            text = link.get_text().strip()
            
            # JavaScript 함수 패턴들
            js_patterns = [
                r"fn_download\(['\"]?([^'\"]+)['\"]?(?:,\s*['\"]?([^'\"]+)['\"]?)?\)",
                r"fileDownload\(['\"]?([^'\"]+)['\"]?(?:,\s*['\"]?([^'\"]+)['\"]?)?\)",
                r"download\(['\"]?([^'\"]+)['\"]?(?:,\s*['\"]?([^'\"]+)['\"]?)?\)",
                r"attachDown\(['\"]?([^'\"]+)['\"]?(?:,\s*['\"]?([^'\"]+)['\"]?)?\)",
                r"fn_fileDown\(['\"]?([^'\"]+)['\"]?(?:,\s*['\"]?([^'\"]+)['\"]?)?\)",
                r"jsFileDownload\(['\"]?([^'\"]+)['\"]?(?:,\s*['\"]?([^'\"]+)['\"]?)?\)",
                r"getFile\(['\"]?([^'\"]+)['\"]?(?:,\s*['\"]?([^'\"]+)['\"]?)?\)",
                r"file_down\(['\"]?([^'\"]+)['\"]?(?:,\s*['\"]?([^'\"]+)['\"]?)?\)",
                r"fnFileDown\(['\"]?([^'\"]+)['\"]?(?:,\s*['\"]?([^'\"]+)['\"]?)?\)",
                r"boardFileDown\(['\"]?([^'\"]+)['\"]?(?:,\s*['\"]?([^'\"]+)['\"]?)?\)",
                r"filePreview\(['\"]?([^'\"]+)['\"]?(?:,\s*['\"]?([^'\"]+)['\"]?)?\)",
                r"viewer\(['\"]?([^'\"]+)['\"]?(?:,\s*['\"]?([^'\"]+)['\"]?)?\)",
            ]
            
            for pattern in js_patterns:
                matches = re.findall(pattern, onclick)
                for match in matches:
                    file_id = match[0] if isinstance(match, tuple) else match
                    file_name = match[1] if isinstance(match, tuple) and len(match) > 1 else ''
                    
                    # 다중 URL 시도
                    url_variants = self.build_download_url_variants(base_url, file_id, file_name)
                    
                    for url in url_variants:
                        if not self.deduplicator.is_duplicate_url(url):
                            download_urls.append({
                                'url': url,
                                'type': 'onclick',
                                'text': text or file_name,
                                'file_id': file_id,
                                'file_name': file_name,
                                'variants': url_variants
                            })
                            break
        
        # 4. form 기반 다운로드
        for form in soup.find_all('form'):
            action = form.get('action', '')
            if 'download' in action.lower() or 'file' in action.lower():
                inputs = form.find_all('input')
                form_data = {}
                for inp in inputs:
                    name = inp.get('name')
                    value = inp.get('value')
                    if name and value:
                        form_data[name] = value
                
                if form_data:
                    url = self.build_absolute_url(action, base_url)
                    if url and not self.deduplicator.is_duplicate_url(url):
                        download_urls.append({
                            'url': url,
                            'type': 'form',
                            'method': form.get('method', 'get'),
                            'data': form_data
                        })
        
        # 5. data-* 속성 체크
        data_attrs = ['data-file', 'data-url', 'data-href', 'data-link', 'data-download', 'data-attach']
        for element in soup.find_all():
            for attr in data_attrs:
                if element.has_attr(attr):
                    file_url = element.get(attr)
                    if file_url:
                        url = self.build_absolute_url(file_url, base_url)
                        if url and not self.deduplicator.is_duplicate_url(url):
                            download_urls.append({
                                'url': url,
                                'type': 'data-attr',
                                'text': element.get_text().strip()
                            })
                        break  # 첫 번째 일치하는 data-* 속성만 처리
        
        # 6. iframe 내부 탐색
        for iframe in soup.find_all('iframe'):
            src = iframe.get('src')
            if src:
                iframe_url = self.build_absolute_url(src, base_url)
                if iframe_url:
                    try:
                        iframe_response = self.session.get(iframe_url, timeout=10, verify=False)
                        iframe_soup = BeautifulSoup(iframe_response.text, 'html.parser')
                        iframe_links = self.extract_all_download_urls(iframe_soup, iframe_url)
                        download_urls.extend(iframe_links)
                    except:
                        pass
        
        return download_urls
    
    def build_absolute_url(self, url: str, base_url: str) -> Optional[str]:
        """절대 URL 생성 - 개선된 버전"""
        if not url or url.startswith('#') or url.startswith('javascript'):
            return None
        
        # URL 정리
        url = self.clean_url(url)
        
        # 로컬 파일 경로 체크 (D:/, C:/ 등)
        if re.match(r'^[A-Za-z]:[/\\]', url):
            logger.debug(f"로컬 파일 경로 무시: {url}")
            return None
        
        if url.startswith('http'):
            return url
        
        parsed_base = urlparse(base_url)
        
        if url.startswith('//'):
            return f"{parsed_base.scheme}:{url}"
        
        if url.startswith('/'):
            return f"{parsed_base.scheme}://{parsed_base.netloc}{url}"
        
        # URL 결합 후 정규화
        joined_url = urljoin(base_url, url)
        parsed = urlparse(joined_url)
        normalized = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
        if parsed.query:
            normalized += f"?{parsed.query}"
        if parsed.fragment:
            normalized += f"#{parsed.fragment}"
            
        return normalized
    
    def explore_detail_page(self, detail_url: str, base_url: str) -> List[Dict]:
        """상세 페이지 탐색 - 재시도 포함"""
        download_urls = []
        
        for attempt in range(3):  # 재시도 횟수 증가
            try:
                headers = {'User-Agent': random.choice(self.user_agents)}
                response = self.session.get(detail_url, timeout=15, verify=False, headers=headers)
                response.encoding = self._detect_encoding(response)
                soup = BeautifulSoup(response.text, 'html.parser')
                
                detail_downloads = self.extract_all_download_urls(soup, detail_url)
                download_urls.extend(detail_downloads)
                break
                
            except Exception as e:
                if attempt < 2:  # 마지막 시도 전까지
                    logger.debug(f"상세 페이지 시도 {attempt+1} 실패: {detail_url}")
                    time.sleep(1)
                else:
                    logger.debug(f"상세 페이지 최종 실패: {detail_url} - {e}")
        
        return download_urls
    
    def _detect_encoding(self, response: requests.Response) -> str:
        """응답 인코딩 감지"""
        # 1. Content-Type 헤더 확인
        content_type = response.headers.get('Content-Type', '').lower()
        charset_match = re.search(r'charset=([^\s;]+)', content_type)
        if charset_match:
            return charset_match.group(1)
            
        # 2. HTML 메타 태그 확인
        charset_pattern = re.compile(rb'<meta.*?charset=["\']*([^\s"\'/>]+)', re.I)
        match = charset_pattern.search(response.content)
        if match:
            return match.group(1).decode()
            
        # 3. BOM 확인
        if response.content.startswith(b'\xef\xbb\xbf'):
            return 'utf-8-sig'
            
        # 4. 자동 감지
        return response.apparent_encoding or 'utf-8'
    
    def find_detail_page_links(self, soup: BeautifulSoup, base_url: str) -> List[Tuple[str, str]]:
        """상세 페이지 링크 찾기"""
        detail_links: List[Tuple[str, str]] = []

        detail_keywords = ['view', 'detail', 'read', 'content', 'article', 'show', 'View', 'Detail', '상세']

        for link in soup.find_all('a', href=True):
            href = link.get('href', '')
            if not href:
                continue

            # href 또는 링크 텍스트 체크
            is_detail = any(kw in href for kw in detail_keywords)
            if not is_detail:
                link_text = link.get_text().strip()
                is_detail = any(kw in link_text for kw in ['상세', '보기', '조회', '내용'])
            
            if is_detail:
                parent = link.find_parent(['tr', 'li', 'div', 'article'])
                context_text = ''
                if parent:
                    context_text = parent.get_text(separator=' ', strip=True)
                else:
                    context_text = link.get_text(separator=' ', strip=True)

                is_target, date_str = self.is_target_date(context_text)
                if is_target:
                    url = self.build_absolute_url(href, base_url)
                    if url:
                        item = (url, date_str or '')
                        if item not in detail_links:
                            detail_links.append(item)

        return detail_links

    def download_file_with_retry(self, file_info: Dict, save_dir: str, max_retries: int = 3) -> bool:
        """재시도 로직이 포함된 파일 다운로드 (v5.0: 바로보기 링크 처리 개선)"""
        backoffs = [0, 2, 4, 8]  # 첫 시도는 0초
        variants = file_info.get('variants', [])
        tried_urls: Set[str] = set()

        # 첫 번째는 원 URL, 이후 variants 섞어서 시도
        candidate_rounds: List[List[str]] = []
        primary = file_info.get('url')
        if primary:
            candidate_rounds.append([primary])

        if variants:
            # 중복 제거 및 정리
            uniq_variants = [u for u in variants if u and u not in tried_urls]
            candidate_rounds.extend([[u] for u in uniq_variants[:4]])  # 과도 시도 방지

        # "바로보기" 또는 "미리보기" 관련 특별 처리
        original_text = file_info.get('text', '')
        if '바로보기' in original_text or '미리보기' in original_text:
            # 파일명에서 날짜 추출 시도
            date_part = re.search(r'(\d{4})년\s*(\d{1,2})월', original_text)
            if date_part:
                year, month = date_part.groups()
                # 파일명 구성
                file_name_guess = f"{year}년 {month}월_업무추진비.pdf"
                file_info['file_name'] = file_name_guess

        tries = 0
        for attempt in range(min(max_retries, len(backoffs))):
            wait = backoffs[attempt] if attempt < len(backoffs) else backoffs[-1]
            if wait:
                time.sleep(wait)

            url_batch = candidate_rounds[attempt] if attempt < len(candidate_rounds) else []
            if not url_batch and primary and primary not in tried_urls:
                url_batch = [primary]

            for url in url_batch:
                if not url or url in tried_urls:
                    continue
                tried_urls.add(url)

                try_info = dict(file_info)
                try_info['url'] = url
                try:
                    if self.download_file(try_info, save_dir):
                        return True
                except requests.exceptions.HTTPError as e:
                    code = getattr(e.response, 'status_code', None)
                    if code and 400 <= code < 500 and code != 429:
                        logger.debug(f"HTTP {code}로 중단: {url}")
                        # 4xx (429 제외)는 즉시 다음 변형 시도
                        continue
                    # 5xx 또는 429는 다음 attempt로 백오프
                except (requests.exceptions.Timeout,
                        requests.exceptions.SSLError,
                        requests.exceptions.ConnectionError) as e:
                    logger.debug(f"네트워크 오류 재시도 예정: {e}")
                    # 다음 attempt
                except Exception as e:
                    logger.debug(f"예상치 못한 오류(계속 진행): {e}")

                tries += 1

        return False

    def decode_filename(self, text: str) -> str:
        """파일명 디코딩 - 다양한 인코딩 시도 (v5.0: 개선)"""
        if not text:
            return text

        # 이미 한글/유니코드로 정상일 수 있음
        try:
            if re.search(r'[\uAC00-\uD7A3]', text):  # 한글 포함 확인
                return text  # 이미 한글이 정상적으로 포함된 경우
            
            text.encode('ascii')  # ASCII로 인코딩 시도
            # ASCII로 인코딩 가능하면 이것은 인코딩 문제가 있을 수 있음
        except UnicodeEncodeError:
            # 이미 유니코드로 정상적인 경우
            return text

        # URL 인코딩 확인
        if '%' in text:
            try:
                decoded = unquote(text)
                if decoded != text:
                    return decoded
            except Exception:
                pass

        # 다양한 인코딩 시도
        encodings = ['utf-8', 'euc-kr', 'cp949', 'iso-8859-1', 'latin-1']
        for encoding in encodings:
            try:
                # latin-1로 바이트로 변환 후 다시 목표 인코딩으로 디코딩
                decoded = text.encode('latin-1').decode(encoding)
                
                # 성공적인 디코딩 확인 (한글 포함 여부)
                if re.search(r'[\uAC00-\uD7A3]', decoded):
                    return decoded
            except Exception:
                continue

        # 마지막 시도: 단순 URL 디코딩
        try:
            return unquote(text)
        except:
            pass

        return text

    def download_file(self, file_info: Dict, save_dir: str) -> bool:
        """파일 다운로드"""
        url = file_info['url']
        method = file_info.get('method', 'get').lower()
        data = file_info.get('data')

        headers = {
            'Referer': url,
            'Accept': '*/*',
            'User-Agent': random.choice(self.user_agents)
        }

        # 실제 요청
        try:
            if method == 'post' and data:
                resp = self.session.post(url, data=data, headers=headers,
                                         timeout=30, stream=True, verify=False, allow_redirects=True)
            else:
                resp = self.session.get(url, headers=headers,
                                        timeout=30, stream=True, verify=False, allow_redirects=True)

            resp.raise_for_status()
        except Exception as e:
            logger.debug(f"다운로드 요청 실패: {url} - {e}")
            return False

        # 콘텐츠 타입 검사
        content_type = resp.headers.get('Content-Type', '').lower()
        if 'text/html' in content_type and 'attachment' not in resp.headers.get('Content-Disposition', ''):
            # HTML 응답이지만 바로보기 링크인 경우 PDF 변환 시도
            if '바로보기' in file_info.get('text', '') or '미리보기' in file_info.get('text', ''):
                if self.use_selenium and SELENIUM_AVAILABLE and self.driver:
                    return self._download_preview_with_selenium(url, file_info, save_dir)
            logger.debug(f"HTML 응답(파일 아님) 건너뜀: {url}")
            return False

        filename = self.extract_filename(resp, file_info)

        # 파일명 중복 체크
        if self.deduplicator.is_duplicate_filename(filename, url):
            logger.debug(f"중복 파일명 건너뜀: {filename}")
            return False

        save_path = os.path.join(save_dir, filename)
        save_path = self.get_unique_filepath(save_path)

        # 저장
        with open(save_path, 'wb') as f:
            for chunk in resp.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)

        # 내용 중복 체크(해시)
        if self.deduplicator.is_duplicate_content(save_path):
            logger.info(f"중복 내용 삭제: {filename}")
            os.remove(save_path)
            return False

        size = os.path.getsize(save_path)
        if size < 100:
            logger.warning(f"파일이 너무 작음 ({size} bytes): {filename}")
            os.remove(save_path)
            return False

        logger.info(f"✓ 다운로드 성공: {filename} ({size:,} bytes)")
        return True
    
    def _download_preview_with_selenium(self, url: str, file_info: Dict, save_dir: str) -> bool:
        """Selenium으로 바로보기/미리보기 다운로드"""
        if not self.driver:
            return False
            
        try:
            self.driver.get(url)
            time.sleep(3)  # 페이지 로딩 대기
            
            # PDF 내용 확인 (iframe 또는 embed 요소)
            pdf_elements = self.driver.find_elements(By.TAG_NAME, "iframe") + \
                           self.driver.find_elements(By.TAG_NAME, "embed") + \
                           self.driver.find_elements(By.TAG_NAME, "object")
                           
            if not pdf_elements:
                return False
                
            pdf_src = None
            for elem in pdf_elements:
                src = elem.get_attribute("src") or elem.get_attribute("data")
                if src and ('.pdf' in src or 'viewer' in src):
                    pdf_src = src
                    break
                    
            if not pdf_src:
                return False
                
            # PDF URL 추출
            pdf_url = pdf_src
            if '?file=' in pdf_src:
                pdf_url = re.search(r'\?file=([^&]+)', pdf_src).group(1)
                
            if not pdf_url:
                return False
                
            # 파일명 생성
            date_str = file_info.get('date', '')
            if not date_str:
                text = file_info.get('text', '')
                date_match = re.search(r'(\d{4})년\s*(\d{1,2})월', text)
                if date_match:
                    date_str = f"{date_match.group(1)}년 {date_match.group(2)}월"
                    
            filename = date_str + "_업무추진비.pdf" if date_str else "업무추진비.pdf"
            
            # 원본 PDF 다운로드
            headers = {
                'Referer': url,
                'User-Agent': self.driver.execute_script("return navigator.userAgent"),
            }
            
            resp = self.session.get(pdf_url, headers=headers, timeout=30, 
                                  stream=True, verify=False, allow_redirects=True)
                                  
            save_path = os.path.join(save_dir, filename)
            save_path = self.get_unique_filepath(save_path)
            
            with open(save_path, 'wb') as f:
                for chunk in resp.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
                        
            size = os.path.getsize(save_path)
            if size < 100:
                logger.warning(f"바로보기 PDF가 너무 작음 ({size} bytes): {filename}")
                os.remove(save_path)
                return False
                
            logger.info(f"✓ 바로보기 다운로드 성공: {filename} ({size:,} bytes)")
            return True
            
        except Exception as e:
            logger.debug(f"Selenium 바로보기 다운로드 실패: {e}")
            return False

    def extract_filename(self, response: requests.Response, file_info: Dict) -> str:
        """파일명 추출 - 인코딩/확장자 처리 개선(v5.0)"""
        filename: Optional[str] = None

        # 1) Content-Disposition
        cd = response.headers.get('Content-Disposition', '')
        if cd:
            # RFC 5987
            m = re.findall(r"filename\*=UTF-8''([^;]+)", cd)
            if m:
                filename = unquote(m[0])

            if not filename:
                m = re.findall(r"filename\*=([^']+)''([^;]+)", cd)
                if m:
                    _, enc_name = m[0]
                    try:
                        filename = unquote(enc_name)
                    except Exception:
                        filename = enc_name

            if not filename:
                m = re.findall(r'filename[^;=\n]*=(([\'"]).*?\2|[^;\n]*)', cd)
                if m:
                    raw = m[0][0].strip('"\'')

                    # URL 디코딩 + 인코딩 교정
                    try:
                        decoded = unquote(raw)
                        filename = decoded if decoded else raw
                    except Exception:
                        filename = raw

                    filename = self.decode_filename(filename)

        # 2) file_info 힌트 - 날짜 정보 활용
        if not filename or len(filename) < 2:
            date_info = file_info.get('date', '')
            text_info = file_info.get('text', '').strip()
            
            if date_info and date_info not in text_info:
                text_info = f"{date_info}_{text_info}"
                
            if text_info:
                filename = self.decode_filename(text_info)
            else:
                for k in ('file_name', 'title'):
                    v = file_info.get(k)
                    if v:
                        filename = self.decode_filename(v.strip())
                        if date_info and date_info not in filename:
                            filename = f"{date_info}_{filename}"
                        break

        # 3) URL에서 추출
        if not filename or len(filename) < 2:
            url_path = urlparse(file_info['url']).path
            base = os.path.basename(url_path)
            if base and len(base) > 2:
                filename = self.decode_filename(unquote(base))

        # 4) 기본값
        if not filename or len(filename) < 2:
            site_name = file_info.get('site_name', '')
            date_info = file_info.get('date', '')
            if site_name and date_info:
                filename = f"{site_name}_{date_info}_업무추진비"
            else:
                filename = f"file_{int(time.time())}_{random.randint(1000, 9999)}"

        # 파일명 정리
        filename = re.sub(r'[<>:"/\\|?*\x00-\x1f]', '_', filename).strip()
        
        # '바로보기' 또는 '미리보기' 텍스트 제거
        filename = re.sub(r'바로보기|미리보기', '', filename).strip()
        filename = re.sub(r'__+', '_', filename)  # 중복 언더스코어 제거
        
        # 너무 긴 파일명 처리 (확장자 보존 시도)
        if len(filename) > 200:
            name_part = filename[:190]
            ext_match = re.search(r'\.[A-Za-z0-9]+$', filename)
            if ext_match:
                filename = name_part + ext_match.group()
            else:
                filename = name_part

        # 확장자 확인/추가
        valid_exts = ['.pdf', '.xlsx', '.xls', '.hwp', '.hwpx', '.doc', '.docx', '.zip', '.csv', '.ppt', '.pptx']
        if not any(filename.lower().endswith(ext) for ext in valid_exts):
            ctype = response.headers.get('Content-Type', '').lower()
            ext_map = {
                'pdf': '.pdf',
                'excel': '.xlsx',
                'spreadsheet': '.xlsx',
                'sheet': '.xlsx',
                'hwp': '.hwp',
                'msword': '.doc',
                'wordprocessing': '.docx',
                'zip': '.zip',
                'csv': '.csv',
                'presentation': '.pptx'
            }
            appended = False
            for key, ext in ext_map.items():
                if key in ctype:
                    filename += ext
                    appended = True
                    break

            if not appended:
                # URL 힌트
                url_lower = file_info['url'].lower()
                for ext in valid_exts:
                    if ext in url_lower:
                        filename += ext
                        break
                        
                # 최후의 수단으로 PDF 확장자 추가
                if not any(filename.lower().endswith(ext) for ext in valid_exts):
                    filename += '.pdf'

        return filename

    def get_unique_filepath(self, filepath: str) -> str:
        """고유한 파일 경로 생성"""
        if not os.path.exists(filepath):
            return filepath
        base, ext = os.path.splitext(filepath)
        i = 1
        candidate = filepath
        while os.path.exists(candidate):
            candidate = f"{base}_{i}{ext}"
            i += 1
        return candidate

    def process_site_with_selenium(self, url: str) -> List[Dict]:
        """Selenium으로 동적 페이지에서 다운로드 링크 수집"""
        results: List[Dict] = []
        if not self.driver:
            return results

        try:
            self.driver.get(url)
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )

            # 스크롤 다운으로 동적 로딩 유도
            last_height = self.driver.execute_script("return document.body.scrollHeight")
            for _ in range(3):
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(1.5)
                new_height = self.driver.execute_script("return document.body.scrollHeight")
                if new_height == last_height:
                    break
                last_height = new_height
                
            # 더보기/추가 버튼 찾기 시도
            more_button_candidates = [
                "//button[contains(text(), '더보기')]",
                "//a[contains(text(), '더보기')]",
                "//button[contains(@class, 'more')]",
                "//a[contains(@class, 'more')]",
                "//button[contains(@class, 'load-more')]"
            ]
            
            for xpath in more_button_candidates:
                try:
                    buttons = self.driver.find_elements(By.XPATH, xpath)
                    for button in buttons:
                        if button.is_displayed():
                            button.click()
                            time.sleep(1.5)
                except:
                    continue

            soup = BeautifulSoup(self.driver.page_source, 'html.parser')
            results = self.extract_all_download_urls(soup, url)
        except Exception as e:
            logger.debug(f"Selenium 처리 오류: {e}")

        return results

    def process_site(self, url: str) -> Dict:
        """사이트 처리"""
        site_name = self.get_site_name(url)
        logger.info(f"\n{'='*60}")
        logger.info(f"📍 처리 중: {site_name}")
        logger.info(f"🔗 URL: {url}")
        logger.info(f"{'='*60}")

        stats = {
            'site_name': site_name,
            'url': url,
            'total_links': 0,
            'download_candidates': 0,
            'detail_pages': 0,
            'target_files': 0,
            'downloaded': 0,
            'failed': 0,
            'duplicates_removed': 0,
            'errors': []
        }

        site_dir = os.path.join(self.base_download_dir, site_name)
        os.makedirs(site_dir, exist_ok=True)

        try:
            headers = {'User-Agent': random.choice(self.user_agents)}
            resp = self.session.get(url, timeout=30, verify=False, headers=headers)
            resp.encoding = self._detect_encoding(resp)
            soup = BeautifulSoup(resp.text, 'html.parser')

            all_links = soup.find_all('a')
            stats['total_links'] = len(all_links)
            logger.info(f"📊 전체 링크 수: {stats['total_links']:,}개")

            download_urls = self.extract_all_download_urls(soup, url)
            
            # Selenium 보조
            if self.use_selenium and SELENIUM_AVAILABLE and self.driver:
                logger.info("🤖 Selenium으로 동적 콘텐츠 확인 중...")
                selenium_urls = self.process_site_with_selenium(url)
                # URL 중복 제거
                for su in selenium_urls:
                    if not any(du['url'] == su['url'] for du in download_urls):
                        download_urls.append(su)
            
            stats['download_candidates'] = len(download_urls)
            logger.info(f"📥 다운로드 후보: {stats['download_candidates']}개")

            detail_links = self.find_detail_page_links(soup, url)
            stats['detail_pages'] = len(detail_links)
            logger.info(f"🔍 상세 페이지: {stats['detail_pages']}개 탐색 중...")

            # 상세 페이지 탐색 (최대 100개)
            for durl, date_str in detail_links[:100]:
                for dl in self.explore_detail_page(durl, url):
                    dl['date'] = date_str
                    dl['site_name'] = site_name  # 사이트명 추가
                    download_urls.append(dl)
                time.sleep(random.uniform(0.2, 0.5))

            # 기간 필터링
            filtered: List[Dict] = []
            for info in download_urls:
                if info.get('date'):
                    info['site_name'] = site_name  # 사이트명 추가
                    filtered.append(info)
                    continue

                text_to_check = f"{info.get('text', '')} {info.get('title', '')}"
                is_target, date_str = self.is_target_date(text_to_check)
                if is_target:
                    info['date'] = date_str
                    info['site_name'] = site_name  # 사이트명 추가
                    filtered.append(info)

            stats['target_files'] = len(filtered)
            logger.info(f"🎯 대상 파일: {stats['target_files']}개")

            if stats['target_files'] == 0:
                logger.warning(f"⚠️  {site_name}: 대상 기간의 파일을 찾을 수 없습니다.")

            # 진행 표시
            if TQDM_AVAILABLE:
                pbar = tqdm(total=stats['target_files'], desc=f"{site_name} 다운로드", 
                            unit="file", leave=False)
            
            for idx, info in enumerate(filtered, 1):
                try:
                    # 파일명 프리픽스에 날짜 표시
                    if info.get('date'):
                        original_text = info.get('text', '')
                        if original_text and info['date'] not in original_text:
                            info['text'] = f"{info['date']}_{original_text}"

                    logger.info(f"⏬ [{idx}/{stats['target_files']}] 다운로드 시도: {info.get('text', 'unknown')[:60]}")
                    ok = self.download_file_with_retry(info, site_dir, max_retries=4)
                    if ok:
                        stats['downloaded'] += 1
                    else:
                        stats['failed'] += 1
                        stats['errors'].append(f"다운로드 실패: {info.get('text', 'unknown')[:60]}")

                    # 진행 표시 업데이트
                    if TQDM_AVAILABLE:
                        pbar.update(1)
                        
                    time.sleep(random.uniform(0.25, 0.6))
                except Exception as e:
                    stats['failed'] += 1
                    em = f"파일 처리 오류: {str(e)[:120]}"
                    stats['errors'].append(em)
                    logger.error(f"❌ {em}")
                    
                    # 진행 표시 업데이트
                    if TQDM_AVAILABLE:
                        pbar.update(1)
            
            # 진행 표시 종료
            if TQDM_AVAILABLE:
                pbar.close()

            initial_candidates = stats['download_candidates'] + stats['detail_pages']
            stats['duplicates_removed'] = max(0, initial_candidates - stats['target_files'])

        except requests.exceptions.Timeout:
            em = "페이지 로딩 타임아웃"
            stats['errors'].append(em)
            logger.error(f"❌ {site_name}: {em}")
        except requests.exceptions.ConnectionError:
            em = "네트워크 연결 오류"
            stats['errors'].append(em)
            logger.error(f"❌ {site_name}: {em}")
        except Exception as e:
            em = f"처리 오류: {str(e)[:200]}"
            stats['errors'].append(em)
            logger.error(f"❌ {site_name}: {em}")

        if stats['downloaded'] > 0:
            logger.info(f"✅ {site_name} 완료: {stats['downloaded']}/{stats['target_files']}개 다운로드 성공")
        elif stats['target_files'] > 0:
            logger.warning(f"⚠️  {site_name}: {stats['target_files']}개 파일 발견했으나 다운로드 실패")
        else:
            logger.info(f"ℹ️  {site_name}: 대상 파일 없음")

        return stats
        
    def process_sites_parallel(self, urls: List[str]) -> List[Dict]:
        """병렬 처리로 여러 사이트 동시 처리"""
        all_stats = []
        
        if self.max_workers <= 1:
            # 순차 처리
            for i, url in enumerate(urls, 1):
                logger.info(f"\n{'▶'*3} 진행: {i}/{len(urls)} ({i/len(urls)*100:.1f}%)")
                stats = self.process_site(url)
                all_stats.append(stats)
                self.stats[stats['site_name']] = stats
                
                if i < len(urls):
                    time.sleep(random.uniform(1.0, 2.0))
            return all_stats
        
        # 병렬 처리
        with concurrent.futures.ProcessPoolExecutor(max_workers=self.max_workers) as executor:
            # Selenium을 사용하는 경우 프로세스 풀의 각 워커에게 알림
            if self.use_selenium:
                # 각 프로세스마다 독립적인 Selenium 세션을 생성해야 함
                # 이를 위한 설정 전달 (직접 실행 시에는 처리 필요)
                pass
                
            # 진행 표시
            if TQDM_AVAILABLE:
                results = list(tqdm(
                    executor.map(self.process_site, urls),
                    total=len(urls),
                    desc="사이트 처리 중",
                    unit="site"
                ))
            else:
                # 병렬 처리 실행
                futures = [executor.submit(self.process_site, url) for url in urls]
                
                # 결과 수집
                results = []
                for i, future in enumerate(concurrent.futures.as_completed(futures)):
                    try:
                        stats = future.result()
                        results.append(stats)
                        logger.info(f"완료: {i+1}/{len(urls)} - {stats['site_name']}")
                    except Exception as e:
                        logger.error(f"처리 오류: {e}")
                
            all_stats = results
            
            # 통계 저장
            for stats in all_stats:
                self.stats[stats['site_name']] = stats
                
        return all_stats

    def run(self):
        """메인 실행"""
        logger.info(f"\n{'='*80}")
        logger.info(f"🚀 지자체 업무추진비 파일 다운로더 v5.0 시작")
        logger.info(f"{'='*80}")
        logger.info(f"⏰ 실행 시각: {self.current_date.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"📅 대상 월: {', '.join([f'{y}년 {m}월' for y, m in self.target_months])}")
        logger.info(f"🧵 병렬 처리: {self.max_workers}개 프로세스 사용")

        url_file = 'urls.txt'
        if os.path.exists('urls_test.txt'):
            url_file = 'urls_test.txt'
            logger.info("🧪 테스트 모드: urls_test.txt 사용")

        if not os.path.exists(url_file):
            logger.error(f"❌ {url_file} 파일이 없습니다.")
            return

        with open(url_file, 'r', encoding='utf-8') as f:
            urls = [line.strip() for line in f if line.strip() and not line.startswith('#')]

        urls = list(dict.fromkeys(urls))  # dedup
        logger.info(f"📋 처리할 사이트 수: {len(urls)}개")
        logger.info(f"{'='*80}\n")

        start = time.time()

        # 병렬 처리
        all_stats = self.process_sites_parallel(urls)

        if self.driver:
            self.driver.quit()

        elapsed = time.time() - start
        self.generate_report(all_stats, elapsed)

    def generate_report(self, all_stats: List[Dict], elapsed_time: float):
        """최종 보고서 생성"""
        report: List[str] = []
        report.append("=" * 80)
        report.append("📊 지자체 업무추진비 파일 다운로드 결과 보고서 v5.0")
        report.append("=" * 80)
        report.append(f"⏰ 실행 시간: {self.current_date.strftime('%Y-%m-%d %H:%M:%S')}")
        report.append(f"⏱️  소요 시간: {int(elapsed_time // 60)}분 {int(elapsed_time % 60)}초")
        report.append(f"📅 대상 기간: {', '.join([f'{y}년 {m}월' for y, m in self.target_months])}")
        report.append(f"🏢 처리 사이트: {len(all_stats)}개")
        report.append(f"🧵 병렬 처리: {self.max_workers}개 프로세스 사용")
        report.append("")

        total_links = sum(s['total_links'] for s in all_stats)
        total_candidates = sum(s['download_candidates'] for s in all_stats)
        total_detail = sum(s['detail_pages'] for s in all_stats)
        total_target = sum(s['target_files'] for s in all_stats)
        total_downloaded = sum(s['downloaded'] for s in all_stats)
        total_failed = sum(s['failed'] for s in all_stats)
        total_duplicates = sum(s['duplicates_removed'] for s in all_stats)

        report.append("=" * 80)
        report.append("📈 전체 결과 요약")
        report.append("=" * 80)
        report.append(f"  🔗 전체 링크 수: {total_links:,}개")
        report.append(f"  📥 다운로드 후보: {total_candidates:,}개")
        report.append(f"  🔍 탐색한 상세페이지: {total_detail:,}개")
        report.append(f"  🗑️  중복 제거: {total_duplicates:,}개")
        report.append(f"  🎯 대상 파일: {total_target:,}개")
        report.append(f"  ✅ 다운로드 성공: {total_downloaded:,}개")
        report.append(f"  ❌ 다운로드 실패: {total_failed:,}개")
        if total_target > 0:
            success_rate = total_downloaded / total_target * 100
            report.append(f"  📊 성공률: {success_rate:.1f}%")
        report.append("")

        dedup_stats = self.deduplicator.get_stats()
        report.append("=" * 80)
        report.append("🗂️  중복 제거 상세 통계")
        report.append("=" * 80)
        report.append(f"  • 고유 URL: {dedup_stats['unique_urls']:,}개")
        report.append(f"  • 고유 파일명: {dedup_stats['unique_filenames']:,}개")
        report.append(f"  • 고유 파일 내용: {dedup_stats['unique_contents']:,}개")
        report.append("")

        report.append("=" * 80)
        report.append("🏢 사이트별 상세 결과")
        report.append("=" * 80)

        successful = [s for s in all_stats if s['downloaded'] > 0]
        if successful:
            report.append(f"\n✅ 다운로드 성공 사이트 ({len(successful)}개)")
            report.append("-" * 80)
            for st in sorted(successful, key=lambda x: x['downloaded'], reverse=True):
                rate = (st['downloaded'] / st['target_files'] * 100) if st['target_files'] > 0 else 0
                report.append(f"\n  📍 {st['site_name']}: {st['downloaded']}/{st['target_files']}개 ({rate:.1f}%)")
                report.append(f"    URL: {st['url']}")
                report.append(f"    전체링크: {st['total_links']:,} | 후보: {st['download_candidates']:,} | 상세: {st['detail_pages']:,}")
                if st['failed'] > 0:
                    report.append(f"    ⚠️  실패: {st['failed']}개")

        partial_failed = [s for s in all_stats if 0 < s['downloaded'] < s['target_files']]
        if partial_failed:
            report.append(f"\n⚠️  부분 실패 사이트 ({len(partial_failed)}개)")
            report.append("-" * 80)
            for st in partial_failed:
                report.append(f"\n  📍 {st['site_name']}: {st['downloaded']}/{st['target_files']}개")
                report.append(f"    URL: {st['url']}")
                if st['errors']:
                    report.append(f"    오류: {st['errors'][0][:120]}")

        failed = [s for s in all_stats if s['downloaded'] == 0 and s['target_files'] > 0]
        if failed:
            report.append(f"\n❌ 다운로드 실패 사이트 ({len(failed)}개)")
            report.append("-" * 80)
            for st in failed:
                report.append(f"\n  📍 {st['site_name']}: {st['target_files']}개 파일 모두 실패")
                report.append(f"    URL: {st['url']}")
                if st['errors']:
                    report.append(f"    오류: {st['errors'][0][:150]}")

        no_files = [s for s in all_stats if s['target_files'] == 0]
        if no_files:
            report.append(f"\nℹ️  대상 파일 없음 ({len(no_files)}개)")
            report.append("-" * 80)
            for st in no_files:
                report.append(f"  • {st['site_name']} (링크: {st['total_links']:,}개)")

        report.append("")
        report.append("=" * 80)
        report.append("🎉 다운로드 완료!")
        report.append(f"📁 결과 폴더: {self.base_download_dir}")
        report.append("=" * 80)

        report_text = '\n'.join(report)
        print("\n" + report_text)

        # 산출물 저장
        report_file = os.path.join(self.base_download_dir, 'download_report.txt')
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write(report_text)

        json_file = os.path.join(self.base_download_dir, 'download_stats.json')
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump({
                'execution_time': self.current_date.strftime('%Y-%m-%d %H:%M:%S'),
                'elapsed_seconds': int(elapsed_time),
                'summary': {
                    'total_sites': len(all_stats),
                    'total_links': total_links,
                    'download_candidates': total_candidates,
                    'detail_pages': total_detail,
                    'duplicates_removed': total_duplicates,
                    'target_files': total_target,
                    'downloaded': total_downloaded,
                    'failed': total_failed,
                    'success_rate': round(total_downloaded / total_target * 100, 1) if total_target > 0 else 0
                },
                'deduplication': dedup_stats,
                'sites': all_stats
            }, f, ensure_ascii=False, indent=2)

        csv_file = os.path.join(self.base_download_dir, 'download_summary.csv')
        with open(csv_file, 'w', encoding='utf-8-sig') as f:
            f.write("사이트명,URL,전체링크,다운로드후보,상세페이지,중복제거,대상파일,다운로드성공,다운로드실패,성공률\n")
            for st in all_stats:
                rate = (st['downloaded'] / st['target_files'] * 100) if st['target_files'] > 0 else 0
                f.write(
                    f"{st['site_name']},{st['url']},{st['total_links']},"
                    f"{st['download_candidates']},{st['detail_pages']},{st['duplicates_removed']},"
                    f"{st['target_files']},{st['downloaded']},{st['failed']},{rate:.1f}%\n"
                )

        logger.info(f"\n📄 보고서 저장 완료:")
        logger.info(f"  • 텍스트: {report_file}")
        logger.info(f"  • JSON: {json_file}")
        logger.info(f"  • CSV: {csv_file}")


def main():
    """메인 함수"""
    import argparse

    parser = argparse.ArgumentParser(
        description='지자체 업무추진비 파일 다운로드 v5.0',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
💡 사용 예시:
  python %(prog)s                 # 기본 실행 (병렬 처리)
  python %(prog)s --workers 8       # 8개 프로세스로 병렬 처리
  python %(prog)s --selenium        # Selenium 사용 (동적 페이지)
  python %(prog)s --test            # 테스트 모드 
  python %(prog)s --selenium --test # Selenium + 테스트 모드
  python %(prog)s --debug           # 디버그
        """
    )
    parser.add_argument('--selenium', action='store_true', help='Selenium 사용 (동적 페이지 처리)')
    parser.add_argument('--test', action='store_true', help='테스트 모드: urls_test.txt가 있을 때 우선 사용')
    parser.add_argument('--debug', action='store_true', help='디버그 로그 출력 (DEBUG 레벨)')
    parser.add_argument('--workers', type=int, default=4, help='병렬 처리 프로세스 수 (기본: 4)')
    parser.add_argument('--outdir', type=str, default='pdf_data', help='결과 저장 루트 디렉토리 (기본: pdf_data)')

    args = parser.parse_args()

    # 로그 레벨
    if args.debug:
        logger.setLevel(logging.DEBUG)
        for h in logger.handlers:
            h.setLevel(logging.DEBUG)
        logging.getLogger('urllib3').setLevel(logging.WARNING)

    # 테스트 파일 빠른 검증
    if args.test and not os.path.exists('urls_test.txt'):
        logger.error("❌ 테스트 모드이지만 urls_test.txt 파일이 없습니다.")
        return

    # 병렬 처리 설정
    max_workers = args.workers
    if max_workers < 1:
        max_workers = 1
    elif max_workers > 16:  # 최대 제한
        max_workers = 16
        
    # 다운로더 초기화
    downloader = CouncilFileDownloader(use_selenium=args.selenium, max_workers=max_workers)

    # 출력 폴더를 pdf_data/<타임스탬프> 형태로 강제
    ts = downloader.current_date.strftime('%Y%m%d_%H%M')
    out_root = os.path.abspath(args.outdir)
    downloader.base_download_dir = os.path.join(out_root, ts)
    os.makedirs(downloader.base_download_dir, exist_ok=True)

    try:
        downloader.run()
    except KeyboardInterrupt:
        print("\n\n⏹️  사용자 중단")
    except Exception as e:
        logger.error(f"예상치 못한 오류: {e}", exc_info=True)
