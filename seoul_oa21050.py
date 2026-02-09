import hashlib
import logging
import os
import re
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import requests

BASE_URL = "http://openapi.seoul.go.kr:8088"
DATASET_ID = "OA-21050"
DEFAULT_TIMEOUT = 10
RETRIES = 3
BACKOFF_BASE = 0.8

logger = logging.getLogger(__name__)
if not logger.handlers:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")


class OA21050ServiceNameError(RuntimeError):
    pass


def is_language_code(s: str) -> bool:
    if not s:
        return False
    s = s.strip()
    if re.fullmatch(r"[a-z]{2,3}$", s):
        return True
    if re.fullmatch(r"[a-z]{2,3}(-[A-Za-z0-9]{2,8})+$", s):
        return True
    if len(s) <= 3 and re.fullmatch(r"[A-Za-z]+", s):
        return True
    return False


def is_numeric_code(s: str) -> bool:
    s = s.strip()
    if re.fullmatch(r"\\d+$", s):
        return True
    if len(s) <= 6 and re.fullmatch(r"[A-Za-z0-9]+", s):
        return True
    return False


def _looks_like_address(s: str) -> bool:
    return bool(re.search(r"(로|길|동|구|구청|번지|\\d{2,}|-dong|-gu|seoul|서울)", s, re.IGNORECASE))


def pick_best_name(raw: Dict[str, Any], sample_log: Optional[List[str]] = None) -> str:
    key_candidates = [
        "NAME",
        "name",
        "TITLE",
        "title",
        "PLACE_NM",
        "PLACE_NAME",
        "POI_NM",
        "TOUR_NM",
        "SIGHT_NM",
        "NM",
        "SUBJECT",
        "TRRSRT_NM",
        "TRRSRT_NAME",
        "FACI_NM",
    ]
    # 1) 후보 키 우선
    for k in key_candidates:
        if k in raw and isinstance(raw[k], str):
            v = raw[k].strip()
            if not v:
                continue
            if is_language_code(v) or is_numeric_code(v):
                if sample_log is not None and len(sample_log) < 5:
                    sample_log.append(v)
                continue
            if re.search(r"[가-힣]", v):
                return v
            # 한글 없는 값은 name으로 사용하지 않음
            continue

    # 2) 전체 문자열 중 탐색
    strings = [v.strip() for v in raw.values() if isinstance(v, str) and v.strip()]
    # 한글 포함 우선
    for v in strings:
        if is_language_code(v) or is_numeric_code(v):
            if sample_log is not None and len(sample_log) < 5:
                sample_log.append(v)
            continue
        if _looks_like_address(v):
            continue
        if re.search(r"[가-힣]", v):
            return v

    # 최후
    base = "unknown"
    return f"이름미상-{hashlib.sha1(base.encode('utf-8')).hexdigest()[:6]}"

def _request_with_retry(
    url: str,
    timeout: int = DEFAULT_TIMEOUT,
    debug: bool = False,
) -> Optional[requests.Response]:
    for attempt in range(1, RETRIES + 1):
        try:
            resp = requests.get(url, timeout=timeout)
            if debug:
                logger.debug("HTTP %s %s", resp.status_code, url)
                logger.debug("Content-Type: %s", resp.headers.get("Content-Type"))
                logger.debug("Body head(500): %s", resp.text[:500])
            resp.raise_for_status()
            return resp
        except Exception as exc:
            if attempt == RETRIES:
                logger.warning("OA-21050 request failed: %s", exc)
                return None
            time.sleep((2 ** (attempt - 1)) * BACKOFF_BASE)
    return None


def _get_first_value(d: Dict[str, Any], keys: List[str]) -> Optional[str]:
    for k in keys:
        if k in d and d[k]:
            return str(d[k]).strip()
        lk = k.lower()
        for key in d.keys():
            if key.lower() == lk and d[key]:
                return str(d[key]).strip()
    return None


def _find_rows(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    if not isinstance(payload, dict):
        return []

    def walk(obj: Any) -> Optional[List[Dict[str, Any]]]:
        if isinstance(obj, dict):
            if "row" in obj and isinstance(obj["row"], list):
                return obj["row"]
            for v in obj.values():
                found = walk(v)
                if found is not None:
                    return found
        elif isinstance(obj, list):
            for v in obj:
                found = walk(v)
                if found is not None:
                    return found
        return None

    return walk(payload) or []


def _extract_service_name(payload: Dict[str, Any]) -> Optional[str]:
    rows = _find_rows(payload)
    if not rows:
        return None
    key_candidates = [
        "OPENAPISERVICE",
        "openApiService",
        "openapiService",
        "serviceName",
        "SERVICE_NAME",
        "OpenAPIServiceName",
        "openapi서비스명",
        "SERVICE",
        "service",
        "svc",
        "SVC",
    ]
    for row in rows:
        if not isinstance(row, dict):
            continue
        val = _get_first_value(row, key_candidates)
        if val:
            return val

        # heuristic: identifier-like string
        values = [str(v) for v in row.values() if isinstance(v, (str, int))]
        for v in values:
            if re.fullmatch(r"[A-Za-z0-9_]{3,80}", v):
                return v
    return None


def _parse_float(v: Any) -> Optional[float]:
    try:
        return float(str(v).strip())
    except Exception:
        return None


def _extract_gu(address: Optional[str]) -> Optional[str]:
    if not address:
        return None
    # 한글 구
    m = re.search(r"([가-힣]{2,4}구)", address)
    if m:
        return m.group(1)
    # 영문 -gu
    m = re.search(r"([A-Za-z\-]+)-gu", address)
    if m:
        eng = m.group(1)
        eng_map = {
            "Jongno": "종로구",
            "Jung": "중구",
            "Yongsan": "용산구",
            "Seongdong": "성동구",
            "Gwangjin": "광진구",
            "Dongdaemun": "동대문구",
            "Jungnang": "중랑구",
            "Seongbuk": "성북구",
            "Gangbuk": "강북구",
            "Dobong": "도봉구",
            "Nowon": "노원구",
            "Eunpyeong": "은평구",
            "Seodaemun": "서대문구",
            "Mapo": "마포구",
            "Yangcheon": "양천구",
            "Gangseo": "강서구",
            "Guro": "구로구",
            "Geumcheon": "금천구",
            "Yeongdeungpo": "영등포구",
            "Dongjak": "동작구",
            "Gwanak": "관악구",
            "Seocho": "서초구",
            "Gangnam": "강남구",
            "Songpa": "송파구",
            "Gangdong": "강동구",
        }
        return eng_map.get(eng, f"{eng}-gu")
    # 중문/한자 구
    cn_map = {
        "江南區": "강남구",
        "瑞草區": "서초구",
        "鐘路區": "종로구",
        "中區": "중구",
        "麻浦區": "마포구",
        "龍山區": "용산구",
        "松坡區": "송파구",
        "永登浦區": "영등포구",
    }
    for k, v in cn_map.items():
        if k in address:
            return v
    return None


def normalize_row(raw: Dict[str, Any]) -> Dict[str, Any]:
    excluded_lang_samples: List[str] = []
    name = pick_best_name(raw, excluded_lang_samples)
    address = _get_first_value(raw, ["ADDR", "ADDR_1", "ADDR1", "ADDRESS", "ROAD_ADDR", "ADDR_NM"])
    desc = _get_first_value(raw, ["DESC", "DESCRIPTION", "CONTENT", "DTL_CN"])
    url = _get_first_value(raw, ["HOMEPAGE", "URL", "HOMEPAGE_URL", "HMPG_URL"])
    gu = _get_first_value(raw, ["GU_NM", "GU", "SGG_NM", "SIGUNGU", "SIGUNGU_NM"]) or _extract_gu(address)
    lat = _parse_float(_get_first_value(raw, ["LAT", "LATITUDE", "Y", "MAPY"]))
    lng = _parse_float(_get_first_value(raw, ["LNG", "LON", "LONGITUDE", "X", "MAPX"]))

    tags: List[str] = []
    for key in ["THEMA", "THEME", "TAG", "TAGS", "CATEGORY", "CLASS"]:
        val = _get_first_value(raw, [key])
        if val:
            tags.extend([t.strip() for t in val.replace("/", ",").split(",") if t.strip()])

    place_id = _get_first_value(raw, ["ID", "PLACE_ID", "POI_ID", "SEQ", "MNG_NO", "관리번호"])
    if not place_id:
        base = f"{name or ''}|{address or ''}"
        place_id = hashlib.sha1(base.encode("utf-8")).hexdigest()
    if not name:
        name = f"이름미상-{place_id[:6]}"
    if excluded_lang_samples:
        logger.info("name excluded codes (lang/numeric): %s", excluded_lang_samples)

    area = (
        _get_first_value(raw, ["area", "AREA"])
        or gu
        or _extract_gu(address)
        or "미분류"
    )

    return {
        "place_id": place_id,
        "name": name,
        "area": area,
        "address": address,
        "gu": gu,
        "tags": tags,
        "description": desc,
        "homepage_url": url,
        "lat": lat,
        "lng": lng,
    }


@dataclass
class _Cache:
    value: Any
    expires_at: float
    key: str


_service_cache: Optional[_Cache] = None
_places_cache: Optional[_Cache] = None


def get_service_name_for_oa21050(api_key: Optional[str] = None, debug: bool = False) -> Optional[str]:
    global _service_cache
    now = time.time()
    if _service_cache and _service_cache.expires_at > now and _service_cache.key == (api_key or ""):
        return _service_cache.value

    key = (api_key or "").strip() or os.getenv("SEOUL_OPENAPI_KEY", "").strip()
    if not key:
        logger.warning("SEOUL_OPENAPI_KEY is missing")
        return None

    url = f"{BASE_URL}/{key}/json/SearchOpenAPIIOValueService/1/5/{DATASET_ID}/"
    logger.info("OA-21050 serviceName lookup: %s", url)
    resp = _request_with_retry(url, debug=debug)
    if not resp:
        logger.warning("OA-21050 serviceName lookup failed")
        return None

    try:
        payload = resp.json()
    except Exception as exc:
        msg = f"serviceName lookup JSON parse failed: {exc}"
        logger.warning(msg)
        raise OA21050ServiceNameError(msg)

    if debug:
        logger.debug("Top-level keys: %s", list(payload.keys()))

    rows = _find_rows(payload)
    if not rows:
        result = None
        for k in payload.keys():
            if isinstance(payload[k], dict) and "RESULT" in payload[k]:
                result = payload[k].get("RESULT")
                break
        logger.warning("OA-21050 serviceName row empty; RESULT=%s", result)

    service_name = _extract_service_name(payload)
    if not service_name:
        if rows and isinstance(rows[0], dict):
            logger.debug("row[0] keys: %s", list(rows[0].keys()))
        logger.warning("OA-21050 serviceName not found in response")
        fallback = os.getenv("OA21050_SERVICE_NAME", "").strip()
        if fallback:
            logger.info("Using OA21050_SERVICE_NAME fallback: %s", fallback)
            _service_cache = _Cache(fallback, now + 24 * 3600, key)
            return fallback
        return None

    _service_cache = _Cache(service_name, now + 24 * 3600, key)
    return service_name


def get_tour_places(api_key: Optional[str] = None) -> List[Dict[str, Any]]:
    global _places_cache
    now = time.time()
    if _places_cache and _places_cache.expires_at > now and _places_cache.key == (api_key or ""):
        return _places_cache.value

    service_name = get_service_name_for_oa21050(api_key=api_key, debug=False)
    if not service_name:
        return []

    key = (api_key or "").strip() or os.getenv("SEOUL_OPENAPI_KEY", "").strip()
    if not key:
        logger.warning("SEOUL_OPENAPI_KEY is missing")
        return []

    url = f"{BASE_URL}/{key}/json/{service_name}/1/1000/"
    logger.info("OA-21050 data fetch: %s", url)
    resp = _request_with_retry(url, debug=False)
    if not resp:
        logger.warning("OA-21050 data fetch failed")
        return []

    try:
        payload = resp.json()
    except Exception as exc:
        logger.warning("OA-21050 data JSON parse failed: %s", exc)
        return []

    rows = _find_rows(payload)
    if not rows:
        logger.warning("OA-21050 data rows empty")
        return []

    places = [normalize_row(r) for r in rows if isinstance(r, dict)]
    area_empty = len([p for p in places if not p.get("area")])
    gu_empty = len([p for p in places if not p.get("gu")])
    name_nonempty = len([p for p in places if p.get("name")])
    numeric_name_count = len([p for p in places if re.fullmatch(r"\\d+$", str(p.get("name", "")))])
    sample_keys = list(rows[0].keys())[:3] if rows and isinstance(rows[0], dict) else []
    logger.info("OA-21050 normalized places: %d", len(places))
    logger.info(
        "normalize debug: area_empty=%d gu_empty=%d name_nonempty=%d numeric_name_count=%d sample_keys=%s",
        area_empty,
        gu_empty,
        name_nonempty,
        numeric_name_count,
        sample_keys,
    )

    _places_cache = _Cache(places, now + 6 * 3600, key)
    return places


__all__ = ["get_tour_places", "normalize_row", "get_service_name_for_oa21050"]
