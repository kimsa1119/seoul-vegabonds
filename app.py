# app.py
import os
import re
import json
import math
import time
import random
import hashlib
import requests
import pandas as pd
import streamlit as st
import pydeck as pdk
from typing import Any, Optional
import logging
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import List, Dict, Any, Tuple
from seoul_oa21050 import get_tour_places

# -----------------------------
# Page / Theme
# -----------------------------
st.set_page_config(
    page_title="서울 방랑자 | Seoul Nomads",
    layout="wide",
)

# -----------------------------
# Constants / Defaults
# -----------------------------
APP_NAME_KR = "서울 방랑자"
APP_NAME_EN = "Seoul Nomads"

MAX_EXTRA_PEOPLE = 2  # user + up to 2
RECOMMEND_COUNT = 4
MAX_CANDIDATES = 600
MAX_CANDIDATES_RERANK = 1500

logger = logging.getLogger("recommender")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

REGIONS_JSON_PATH = Path("regions.json")
REGION_ASSETS_DIR = Path("assets/regions")
REGION_THUMBS_DIR = Path("assets/regions/thumbs")
REGIONS_META_PATH = Path("assets/regions/regions_meta.json")
PLACEHOLDER_IMAGE_PATH = Path("assets/placeholder.webp")

# 대표 "지역(동네) 후보" 기본 풀 (API 연동이 불완전할 때 fallback)
# 실제 운영에서는 관광명소 DB(OA-21050)에서 동네/권역 단위로 집계하거나 별도 지역 마스터 테이블을 권장
DEFAULT_AREAS = [
    {"area": "인사동", "gu": "종로구", "center": (37.5740, 126.9856), "addr": "서울특별시 종로구 인사동 일대"},
    {"area": "성수", "gu": "성동구", "center": (37.5445, 127.0566), "addr": "서울특별시 성동구 성수동 일대"},
    {"area": "연남", "gu": "마포구", "center": (37.5637, 126.9216), "addr": "서울특별시 마포구 연남동 일대"},
    {"area": "한남", "gu": "용산구", "center": (37.5343, 127.0067), "addr": "서울특별시 용산구 한남동 일대"},
    {"area": "삼청동", "gu": "종로구", "center": (37.5826, 126.9816), "addr": "서울특별시 종로구 삼청동 일대"},
    {"area": "이태원", "gu": "용산구", "center": (37.5346, 126.9946), "addr": "서울특별시 용산구 이태원동 일대"},
    {"area": "여의도", "gu": "영등포구", "center": (37.5219, 126.9246), "addr": "서울특별시 영등포구 여의도 일대"},
    {"area": "잠실", "gu": "송파구", "center": (37.5133, 127.1028), "addr": "서울특별시 송파구 잠실동 일대"},
    {"area": "서촌", "gu": "종로구", "center": (37.5793, 126.9689), "addr": "서울특별시 종로구 서촌(통인/효자동) 일대"},
    {"area": "익선동", "gu": "종로구", "center": (37.5759, 126.9897), "addr": "서울특별시 종로구 익선동 일대"},
]

# 구 목록 (관광명소 인덱싱 최적화용)
GU_LIST = tuple(sorted({a["gu"] for a in DEFAULT_AREAS}))

# 간이 "인근 추천" (실서비스에서는 그래프/거리 기반 추천 권장)
NEARBY_BEST = {
    "인사동": ["북촌 한옥마을", "삼청동"],
    "삼청동": ["북촌 한옥마을", "인사동"],
    "서촌": ["경복궁", "광화문"],
    "익선동": ["인사동", "종로1가"],
    "성수": ["서울숲", "뚝섬"],
    "연남": ["홍대", "합정"],
    "한남": ["이태원", "용산"],
    "이태원": ["한남", "용산"],
    "여의도": ["더현대 서울", "한강공원(여의도)"],
    "잠실": ["석촌호수", "송리단길"],
}

# 간이 지하철역(500m) 예시 (실서비스에서는 역 좌표 데이터 + 거리 계산 필요)
NEARBY_STATIONS = {
    "인사동": ["안국역 (3호선)", "종각역 (1호선)"],
    "삼청동": ["안국역 (3호선)"],
    "서촌": ["경복궁역 (3호선)", "광화문역 (5호선)"],
    "익선동": ["종로3가역 (1/3/5호선)", "안국역 (3호선)"],
    "성수": ["성수역 (2호선)", "뚝섬역 (2호선)"],
    "연남": ["홍대입구역 (2/경의중앙/공항철도)"],
    "한남": ["한강진역 (6호선)"],
    "이태원": ["이태원역 (6호선)", "한강진역 (6호선)"],
    "여의도": ["여의도역 (5/9호선)", "여의나루역 (5호선)"],
    "잠실": ["잠실역 (2/8호선)", "잠실나루역 (2호선)"],
}

CROWD_LEVELS = ["여유", "약간 붐빔", "붐빔"]
CROWD_COLOR = {
    "여유": "green",
    "약간 붐빔": "orange",
    "붐빔": "red",
}

# -----------------------------
# Helpers: Session State
# -----------------------------
def init_state():
    if "people" not in st.session_state:
        st.session_state.people = [default_person(is_me=True)]
    if "disliked" not in st.session_state:
        # key: signature(str) -> set(area_names)
        st.session_state.disliked = {}
    if "last_reco" not in st.session_state:
        st.session_state.last_reco = []
    if "last_signature" not in st.session_state:
        st.session_state.last_signature = ""
    if "reco_signature" not in st.session_state:
        st.session_state.reco_signature = ""
    if "seen_place_ids" not in st.session_state:
        st.session_state.seen_place_ids = set()
    if "feed_buffer" not in st.session_state:
        st.session_state.feed_buffer = []
    if "master_pool" not in st.session_state:
        st.session_state.master_pool = []
    if "cursor" not in st.session_state:
        st.session_state.cursor = 0
    if "pool_limit" not in st.session_state:
        st.session_state.pool_limit = 0


# -----------------------------
# Data structures
# -----------------------------
@dataclass
class StartLocation:
    scope: str  # "서울 내" / "서울 외부"
    si: str = ""       # 서울 외부 선택 시
    gu: str = ""       # 서울 내 선택 시
    dong: str = ""     # 공통

@dataclass
class PersonInput:
    is_me: bool
    relationship: str
    taste: str
    purpose: str
    start_location: StartLocation

def default_person(is_me: bool = False) -> PersonInput:
    return PersonInput(
        is_me=is_me,
        relationship="본인" if is_me else "",
        taste="",
        purpose="",
        start_location=StartLocation(scope="서울 내", gu="", dong="", si=""),
    )


# -----------------------------
# Signature (조건 변경 감지 / 비선호 재등장 방지)
# -----------------------------
def make_signature(main_taste: str, main_purpose: str, crowd_pref: str, people: List[PersonInput]) -> str:
    # 조건이 동일하면 같은 signature가 되도록 정규화
    def norm(s: str) -> str:
        s = (s or "").strip().lower()
        s = re.sub(r"\s+", " ", s)
        return s

    core = {
        "main_taste": norm(main_taste),
        "main_purpose": norm(main_purpose),
        "crowd_pref": crowd_pref,
        "people": [
            {
                "rel": norm(p.relationship),
                "taste": norm(p.taste),
                "purpose": norm(p.purpose),
                "loc": {
                    "scope": p.start_location.scope,
                    "si": norm(p.start_location.si),
                    "gu": norm(p.start_location.gu),
                    "dong": norm(p.start_location.dong),
                },
            }
            for p in people
            if not p.is_me  # 동행자만 반영(본인은 main으로 이미 있음)
        ],
    }
    return json.dumps(core, ensure_ascii=False, sort_keys=True)


# -----------------------------
# OpenAI (추천 이유/코스 문구 생성)
# -----------------------------
def generate_reason_with_openai(
    openai_api_key: str,
    area_name: str,
    crowd_label: str,
    main_taste: str,
    main_purpose: str,
    extra_context: Dict[str, Any],
) -> Dict[str, Any]:
    """
    반환 형식:
    {
      "one_liner": "3문장",
      "bullets": [],
      "course": {}
    }
    """
    # 키가 없으면 템플릿 텍스트로 fallback
    if not openai_api_key:
        taste = main_taste or "다양한 취향"
        purpose = main_purpose or "여러 목적"
        fallback = (
            f"{area_name}은(는) {taste}과(와) {purpose}에 맞춘 동선이 잘 맞습니다. "
            "주변에 선택지가 모여 있어 일정 구성 부담이 낮습니다. "
            "취향 키워드와 연결된 포인트를 중심으로 코스를 잡기 좋습니다."
        )
        return {
            "one_liner": fallback,
            "bullets": split_sentences_for_bullets(fallback),
            "course": {
                "culture": [],
                "cafe": [],
                "food": [],
                "activity": [],
            },
        }

    # OpenAI 최신 SDK를 쓰지 않고도 동작 가능하게 "HTTP 호출" 형태로 구성 (환경별 유연성)
    # 사용자가 설치한 환경에 따라 SDK를 붙일 수도 있음.
    # 모델명은 환경에 맞게 조정 가능.
    url = "https://api.openai.com/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {openai_api_key}",
        "Content-Type": "application/json",
    }

    sys = (
        "너는 서울 내 약속/데이트 장소 추천 서비스의 카피라이터다. "
        "과장 없이, 3문장으로만 작성한다. "
        "사용자가 입력한 취향/목적 키워드가 있으면 반드시 문장에 포함한다. "
        "이모지는 절대 사용하지 않는다. "
        "추천 이유는 bullets(3문장 리스트)로도 반환하고, "
        "상세 코스는 culture/cafe/food/activity 각 2~3개씩 간단 키워드로 제시한다. "
        "출력은 반드시 JSON만 반환한다."
    )

    user = {
        "area": area_name,
        "crowd": crowd_label,
        "taste": main_taste,
        "purpose": main_purpose,
        "context": extra_context,
        "format": {
            "one_liner": "3 sentences string",
            "bullets": ["sentence1", "sentence2", "sentence3"],
            "course": {
                "culture": ["string", "string"],
                "cafe": ["string", "string"],
                "food": ["string", "string"],
                "activity": ["string", "string"]
            }
        }
    }

    payload = {
        "model": "gpt-4o-mini",  # 필요 시 변경
        "messages": [
            {"role": "system", "content": sys},
            {"role": "user", "content": json.dumps(user, ensure_ascii=False)}
        ],
        "temperature": 0.6,
        "response_format": {"type": "json_object"},
    }

    try:
        resp = requests.post(url, headers=headers, json=payload, timeout=20)
        resp.raise_for_status()
        content = resp.json()["choices"][0]["message"]["content"]
        data = json.loads(content)
        # 기본 키 보정
        data.setdefault("one_liner", "")
        bullets = data.get("bullets") or []
        if not isinstance(bullets, list):
            bullets = []
        if not bullets and data.get("one_liner"):
            bullets = split_sentences_for_bullets(str(data.get("one_liner")))
        data["bullets"] = bullets
        course = data.get("course") or {}
        if not isinstance(course, dict):
            course = {}
        data["course"] = course
        return data
    except Exception:
        # 장애 시 fallback
        taste = main_taste or "다양한 취향"
        purpose = main_purpose or "여러 목적"
        fallback = (
            f"{area_name}은(는) {taste}과(와) {purpose}에 맞춘 동선이 잘 맞습니다. "
            "주변에 선택지가 모여 있어 일정 구성 부담이 낮습니다. "
            "취향 키워드와 연결된 포인트를 중심으로 코스를 잡기 좋습니다."
        )
        return {
            "one_liner": fallback,
            "bullets": split_sentences_for_bullets(fallback),
            "course": {
                "culture": [],
                "cafe": [],
                "food": [],
                "activity": [],
            },
        }


# -----------------------------
# OpenAI (키워드 확장 / 재랭킹)
# -----------------------------
def expand_keywords_with_openai(
    openai_api_key: str,
    main_taste: str,
    main_purpose: str,
    extra_people: List[PersonInput],
) -> List[str]:
    if not openai_api_key:
        return []

    url = "https://api.openai.com/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {openai_api_key}",
        "Content-Type": "application/json",
    }

    user = {
        "taste": main_taste,
        "purpose": main_purpose,
        "companions": [
            {"relationship": p.relationship, "taste": p.taste, "purpose": p.purpose}
            for p in extra_people
        ],
        "format": {"keywords": ["string", "string", "string"]},
    }

    payload = {
        "model": "gpt-4o-mini",
        "messages": [
            {"role": "system", "content": "키워드 확장기. JSON만 반환."},
            {"role": "user", "content": json.dumps(user, ensure_ascii=False)},
        ],
        "temperature": 0.4,
        "response_format": {"type": "json_object"},
    }

    try:
        resp = requests.post(url, headers=headers, json=payload, timeout=20)
        resp.raise_for_status()
        content = resp.json()["choices"][0]["message"]["content"]
        data = json.loads(content)
        kws = data.get("keywords", [])
        if isinstance(kws, list):
            return [str(k).strip() for k in kws if str(k).strip()]
        return []
    except Exception:
        return []


def rerank_areas_with_openai(
    openai_api_key: str,
    main_taste: str,
    main_purpose: str,
    extra_people: List[PersonInput],
    candidates: List[Dict[str, Any]],
) -> List[str]:
    if not openai_api_key:
        return []

    url = "https://api.openai.com/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {openai_api_key}",
        "Content-Type": "application/json",
    }

    user = {
        "taste": main_taste,
        "purpose": main_purpose,
        "companions": [
            {"relationship": p.relationship, "taste": p.taste, "purpose": p.purpose}
            for p in extra_people
        ],
        "candidates": [
            {
                "area": c.get("area"),
                "gu": c.get("gu"),
                "crowd": c.get("crowd_now"),
                "score_hint": round(float(c.get("score", 0.0)), 2),
                "keyword_hits": c.get("keyword_hits", []),
            }
            for c in candidates
        ],
        "format": {"ranked": ["area_name"]},
    }

    payload = {
        "model": "gpt-4o-mini",
        "messages": [
            {"role": "system", "content": "추천 지역 재랭킹. JSON만 반환."},
            {"role": "user", "content": json.dumps(user, ensure_ascii=False)},
        ],
        "temperature": 0.3,
        "response_format": {"type": "json_object"},
    }

    try:
        resp = requests.post(url, headers=headers, json=payload, timeout=20)
        resp.raise_for_status()
        content = resp.json()["choices"][0]["message"]["content"]
        data = json.loads(content)
        ranked = data.get("ranked", [])
        if isinstance(ranked, list):
            return [str(r).strip() for r in ranked if str(r).strip()]
        return []
    except Exception:
        return []


# 빠른 템플릿 문구 (리스트 화면용)
def quick_reason_template(area_name: str, crowd_label: str, main_taste: str, main_purpose: str) -> str:
    taste = main_taste or "다양한 취향"
    purpose = main_purpose or "여러 목적"
    return f"{area_name}은(는) {taste}과(와) {purpose}에 맞춰 동선이 깔끔합니다."


@st.cache_data(ttl=3600)
def generate_reason_cached(
    openai_api_key: str,
    area_name: str,
    crowd_label: str,
    main_taste: str,
    main_purpose: str,
    extra_context: Dict[str, Any],
) -> Dict[str, Any]:
    return generate_reason_with_openai(
        openai_api_key=openai_api_key,
        area_name=area_name,
        crowd_label=crowd_label,
        main_taste=main_taste,
        main_purpose=main_purpose,
        extra_context=extra_context,
    )


# -----------------------------
# Seoul Open Data: Real-time population (OA-21778) - 자리/파싱 TODO
# -----------------------------
@st.cache_data(ttl=60)
def fetch_seoul_realtime_population(seoul_api_key: str) -> Dict[str, Any]:
    """
    서울 열린데이터광장 '서울시 실시간 인구 데이터' API 호출 결과 raw를 반환.
    실제 URL/파라미터는 발급키 유형과 문서에 맞게 조정 필요.
    """
    if not seoul_api_key:
        return {}

    # TODO: 아래 URL은 예시 형태입니다. 실제 엔드포인트/서비스명은 OA-21778 문서에 맞춰 수정하세요.
    # 예) http://openapi.seoul.go.kr:8088/{KEY}/json/citydata/1/5/
    url = f"http://openapi.seoul.go.kr:8088/{seoul_api_key}/json/citydata/1/200/"
    try:
        r = requests.get(url, timeout=15)
        r.raise_for_status()
        return r.json()
    except Exception:
        return {}

def crowd_label_from_population(area_name: str, crowd_pref: str, pop_raw: Dict[str, Any]) -> str:
    """
    area_name별 혼잡도를 산출.
    실제로는 API 응답에서 해당 area_name(또는 장소코드)에 매칭되는 항목의 혼잡도를 읽어야 함.
    여기서는 (1) API 응답 매칭 실패 시 (2) crowd_pref를 그대로 사용.
    """
    # TODO: pop_raw 파싱/매칭 로직을 OA-21778 응답 구조에 맞게 구현
    # 응답에서 'AREA_NM', 'AREA_CONGEST_LVL' 같은 필드가 있을 가능성이 큼.
    # 아래는 예시:
    try:
        # 예시: pop_raw["CITYDATA"]["row"] 구조 가정 (실제와 다를 수 있음)
        rows = pop_raw.get("CITYDATA", {}).get("row", [])
        for row in rows:
            if str(row.get("AREA_NM", "")).strip() == area_name:
                lvl = str(row.get("AREA_CONGEST_LVL", "")).strip()
                if lvl in CROWD_LEVELS:
                    return lvl
    except Exception:
        pass

    # fallback: 사용자가 선택한 희망 혼잡도
    return crowd_pref


# -----------------------------
# Seoul Open Data: Tourist attractions (OA-21050) - 자리/파싱 TODO
# -----------------------------
@st.cache_data(ttl=3600)
def fetch_seoul_tour_spots(seoul_api_key: str) -> List[Dict[str, Any]]:
    """
    '서울 관광명소 데이터 DB' raw 목록 반환.
    실제 URL/서비스명/필드는 OA-21050 문서에 맞게 수정 필요.
    """
    if not seoul_api_key:
        return []

    # TODO: 아래 URL은 예시 형태입니다. 실제 서비스명/엔드포인트는 OA-21050에 맞게 수정하세요.
    # 예) http://openapi.seoul.go.kr:8088/{KEY}/json/<SERVICE>/1/1000/
    url = f"http://openapi.seoul.go.kr:8088/{seoul_api_key}/json/seoultour/1/1000/"
    try:
        r = requests.get(url, timeout=20)
        r.raise_for_status()
        data = r.json()

        # TODO: data 구조에 맞게 row 추출
        # 예시: data["seoultour"]["row"]
        rows = []
        for k in data.keys():
            if isinstance(data[k], dict) and "row" in data[k]:
                rows = data[k]["row"]
                break
        if not isinstance(rows, list):
            return []
        return rows
    except Exception:
        return []


# -----------------------------
# Tourist spots index (precompute search text per gu)
# -----------------------------
@st.cache_data(ttl=21600)
def build_tour_spot_index(tour_spots: List[Dict[str, Any]], gu_list: Tuple[str, ...]) -> Dict[str, List[str]]:
    """
    관광명소 row 텍스트를 미리 합쳐두고 구(gu)별로 인덱싱.
    추천 시 반복 문자열 합치기/필터링 비용을 줄인다.
    """
    index: Dict[str, List[str]] = {gu: [] for gu in gu_list}
    if not tour_spots:
        return index

    for row in tour_spots:
        if not isinstance(row, dict):
            continue
        text = " ".join([str(v) for v in row.values() if isinstance(v, (str, int, float))]).lower()
        if not text:
            continue
        for gu in gu_list:
            if gu in text:
                index[gu].append(text)
    return index


def _get_first_value(row: Dict[str, Any], keys: List[str]) -> str:
    for k in keys:
        if k in row and row[k]:
            return str(row[k]).strip()
        lk = k.lower()
        for key in row.keys():
            if key.lower() == lk and row[key]:
                return str(row[key]).strip()
    return ""


def _parse_float(v: Any) -> Optional[float]:
    try:
        return float(str(v).strip())
    except Exception:
        return None


def build_region_candidates_from_places(
    places: List[Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], bool, str]:
    """
    OA-21050 places를 gu 기준 지역 후보로 묶는다.
    gu가 없는 place는 '기타' 그룹으로 포함한다.
    반환: (candidates, fallback_used, fallback_reason)
    """
    if not places:
        return [], True, "places empty"

    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for p in places:
        gu = (p.get("gu") or "").strip() or "기타"
        grouped.setdefault(gu, []).append(p)

    candidates: List[Dict[str, Any]] = []
    for gu, ps in grouped.items():
        lats = [p.get("lat") for p in ps if isinstance(p.get("lat"), (int, float))]
        lngs = [p.get("lng") for p in ps if isinstance(p.get("lng"), (int, float))]
        center = None
        if lats and lngs:
            center = (sum(lats) / len(lats), sum(lngs) / len(lngs))
        candidates.append(
            {
                "area": gu,
                "gu": gu if gu != "기타" else "",
                "center": center or (37.5665, 126.9780),
                "addr": f"서울특별시 {gu}" if gu != "기타" else "서울특별시",
                "places": ps,
                "place_count": len(ps),
                "has_center": bool(center),
            }
        )

    if not candidates:
        return [], True, "no groups"

    candidates.sort(key=lambda x: x.get("place_count", 0), reverse=True)
    return candidates[:MAX_CANDIDATES], False, ""


def build_place_candidates_from_places(
    places: List[Dict[str, Any]],
    limit: int = MAX_CANDIDATES,
) -> List[Dict[str, Any]]:
    """
    places에서 개별 POI 후보 리스트 생성 (top-up용).
    """
    candidates: List[Dict[str, Any]] = []
    seen = set()
    for p in places:
        lat = p.get("lat")
        lng = p.get("lng")
        if isinstance(lat, (int, float)) and isinstance(lng, (int, float)):
            center = (lat, lng)
            key = f"{p.get('name','')}|{lat:.5f}|{lng:.5f}"
        else:
            center = (37.5665, 126.9780)
            key = f"{p.get('name','')}|no-coords"
        if key in seen:
            continue
        seen.add(key)
        name = (p.get("name") or "").strip()
        address = (p.get("address") or "").strip()
        if is_excluded_place(name, address):
            continue
        dong = extract_dong_from_place(p)
        if not dong:
            continue
        if not is_korean_text(dong):
            continue
        candidates.append(
            {
                "area": dong,
                "gu": p.get("gu") or "",
                "center": center,
                "addr": p.get("address") or "",
                "place_id": p.get("place_id"),
                "tags": p.get("tags") or [],
                "description": p.get("description"),
                "homepage_url": p.get("homepage_url"),
            }
        )
    return candidates[:limit]


# -----------------------------
# Utility: text scoring
# -----------------------------
def tokenize_korean_keywords(text: str) -> List[str]:
    if not text:
        return []
    # 아주 단순 토큰화 (운영에서는 형태소 분석/임베딩 추천)
    text = re.sub(r"[^\w\s가-힣]", " ", text)
    toks = [t.strip().lower() for t in text.split() if t.strip()]
    # 너무 짧은 토큰 제거
    return [t for t in toks if len(t) >= 2]

def split_sentences_for_bullets(text: str) -> List[str]:
    if not text:
        return []
    parts = re.split(r"(?<=[.!?。])\s+", text.strip())
    bullets = []
    for p in parts:
        p = p.strip()
        if not p:
            continue
        bullets.append(p)
    # 한글 문장 마침표가 없을 수 있으므로 길게 남은 문장을 3개로 분할
    if len(bullets) <= 1 and len(text) > 30:
        chunks = re.split(r"[;•]", text)
        for c in chunks:
            c = c.strip()
            if c and c not in bullets:
                bullets.append(c)
    return bullets[:3]


def to_korean_display(text: str) -> str:
    if not text:
        return ""
    # 한글/숫자/공백/하이픈만 남김
    cleaned = re.sub(r"[^0-9가-힣\s\-]", "", text)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned


def is_korean_text(text: str) -> bool:
    if not text:
        return False
    return bool(re.search(r"[가-힣]", text))


def is_excluded_place(name: str, address: str = "") -> bool:
    target = f"{name} {address}".strip()
    return "안녕인사동" in target


def to_road_address(text: str) -> str:
    if not text:
        return ""
    cleaned = to_korean_display(text)
    # 숫자 제거
    cleaned = re.sub(r"\d+", "", cleaned).strip()
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    # 서울-구-동까지만 표기
    m = re.search(r"(서울\s*[가-힣]{2,4}구\s*[가-힣]{2,4}동)", cleaned)
    if m:
        return m.group(1).replace("  ", " ").strip()
    # 동까지만 있는 경우
    m = re.search(r"([가-힣]{2,4}구\s*[가-힣]{2,4}동)", cleaned)
    if m:
        return f"서울 {m.group(1)}".strip()
    return cleaned


def render_bullet_list(items: List[str]) -> None:
    if not items:
        return
    safe_items = [to_korean_display(i) if isinstance(i, str) else str(i) for i in items if str(i).strip()]
    if not safe_items:
        return
    bullets_html = "".join([f"<li style='margin-bottom:6px;'>{i}</li>" for i in safe_items])
    st.markdown(
        f"<ul style='margin:0 0 0 18px; padding:0; color:#333;'>{bullets_html}</ul>",
        unsafe_allow_html=True,
    )


def pick_korean_name(*candidates: str) -> str:
    # 후보 문자열에서 한글 토큰을 추출해 가장 긴 것을 선택
    best = ""
    for text in candidates:
        if not text:
            continue
        # 한글 연속 문자열들 추출
        parts = re.findall(r"[가-힣]{2,}", str(text))
        if parts:
            parts.sort(key=len, reverse=True)
            if len(parts[0]) > len(best):
                best = parts[0]
    return best


def extract_dong(text: str) -> str:
    if not text:
        return ""
    m = re.search(r"([가-힣]{2,4}동)", text)
    if m:
        return m.group(1)
    m = re.search(r"([A-Za-z\\-]+)-dong", text)
    if m:
        return f"{m.group(1)}-dong"
    return ""


def is_dong_name(name: str) -> bool:
    if not name:
        return False
    return bool(re.search(r"[가-힣]{2,4}동", name))


def extract_dong_from_place(p: Dict[str, Any]) -> str:
    """
    장소 레코드에서 동명을 추출.
    주소 우선, 없으면 명칭에서도 추출.
    """
    dong = extract_dong(p.get("address") or "")
    if not dong:
        dong = extract_dong(p.get("name") or "")
    return dong

def score_area_by_preferences(
    area: Dict[str, Any],
    main_taste: str,
    main_purpose: str,
    crowd_pref: str,
    crowd_now: str,
    tour_spot_index: Dict[str, List[str]],
    extra_people: List[PersonInput],
    extra_keywords: List[str],
) -> float:
    """
    간단 스코어링:
    - 텍스트(취향/목적) 키워드가 관광명소 row의 설명/명칭/분류 등에 얼마나 등장하는지
    - 혼잡도 선호와 현재 혼잡도 일치 가점
    - 동행자 취향/목적도 동일 방식으로 가점(공란이면 무시)
    """
    base = 0.0

    # crowd match score
    if crowd_now == crowd_pref:
        base += 2.0
    else:
        # 선호와 실제 차이가 크면 감점 (여유<->붐빔)
        dist = abs(CROWD_LEVELS.index(crowd_now) - CROWD_LEVELS.index(crowd_pref))
        base += max(0.0, 1.5 - 0.7 * dist)

    # keyword score from tour DB
    # TODO: OA-21050 필드에 맞게 area 매칭(구/동/권역/좌표 기반)을 정교화 권장
    # 여기서는 구(gu) 텍스트 포함 여부로 간이 매칭
    kws = tokenize_korean_keywords(f"{main_taste} {main_purpose}")
    for p in extra_people:
        if (p.taste or "").strip() or (p.purpose or "").strip():
            kws.extend(tokenize_korean_keywords(f"{p.taste} {p.purpose}"))
    if extra_keywords:
        for k in extra_keywords:
            kws.extend(tokenize_korean_keywords(k))

    kws = list(dict.fromkeys(kws))  # unique

    if tour_spot_index and kws:
        gu = area.get("gu", "")
        rel_texts = tour_spot_index.get(gu, []) if gu else []

        # 등장 횟수 기반 간이 스코어
        hit = 0
        for text in rel_texts[:300]:
            for k in kws:
                if k in text:
                    hit += 1
        base += min(6.0, hit * 0.15)

    # 지역별 기본 성향(하드코딩) 가점(예시)
    vibe = {
        "인사동": ["전통", "문화", "공예", "전시"],
        "삼청동": ["전통", "카페", "산책", "전시"],
        "서촌": ["산책", "카페", "전통", "전시"],
        "익선동": ["데이트", "맛집", "카페", "한옥"],
        "성수": ["카페", "쇼핑", "액티비티", "전시"],
        "연남": ["카페", "산책", "맛집", "데이트"],
        "한남": ["의전", "레스토랑", "갤러리", "쇼핑"],
        "이태원": ["외국인", "다국적", "바", "레스토랑"],
        "여의도": ["가족", "쇼핑", "공연", "산책"],
        "잠실": ["가족", "쇼핑", "액티비티", "데이트"],
    }
    area_name_safe = area.get("area") or area.get("gu") or "미분류"
    area_tags = " ".join(vibe.get(area_name_safe, [])).lower()
    for k in tokenize_korean_keywords(f"{main_taste} {main_purpose}"):
        if k in area_tags:
            base += 0.8

    return float(base)


# -----------------------------
# Recommendation engine
# -----------------------------
def _ensure_place_id(p: Dict[str, Any]) -> str:
    pid = p.get("place_id")
    if pid:
        return str(pid)
    base = f"{p.get('name','')}|{p.get('address','')}"
    return hashlib.sha1(base.encode("utf-8")).hexdigest()


def _score_place(
    p: Dict[str, Any],
    user_tokens: List[str],
    extra_keywords: List[str],
) -> float:
    text = " ".join(
        [
            str(p.get("name", "")),
            " ".join(p.get("tags", []) if isinstance(p.get("tags"), list) else []),
            str(p.get("description", "")),
        ]
    ).lower()
    score = 0.0
    for k in user_tokens:
        if k and k in text:
            score += 0.4
    for k in extra_keywords:
        if k and k.lower() in text:
            score += 0.2
    return score


def build_master_pool(
    places: List[Dict[str, Any]],
    user_text: str,
    extra_keywords: List[str],
) -> List[Dict[str, Any]]:
    user_tokens = tokenize_korean_keywords(user_text)
    pool = []
    for p in places:
        # '동' 기준 추천: 동이 없으면 후보에서 제외
        name = (p.get("name") or "").strip()
        address = (p.get("address") or "").strip()
        if is_excluded_place(name, address):
            continue
        dong = extract_dong_from_place(p)
        if not dong:
            continue
        if not is_korean_text(dong):
            continue
        if name and not is_korean_text(name):
            continue
        item = dict(p)
        item["place_id"] = _ensure_place_id(item)
        item["area"] = dong
        lat = p.get("lat")
        lng = p.get("lng")
        if isinstance(lat, (int, float)) and isinstance(lng, (int, float)):
            item["center"] = (lat, lng)
        else:
            item["center"] = (37.5665, 126.9780)
        item["score"] = _score_place(item, user_tokens, extra_keywords)
        pool.append(item)

    seed_base = abs(hash(user_text)) % 10000
    rng = random.Random(seed_base)
    for item in pool:
        item["score"] += rng.random() * 0.01

    pool.sort(key=lambda x: x["score"], reverse=True)
    return pool


def get_recommendations_from_places(
    places: List[Dict[str, Any]],
    signature: str,
    main_taste: str,
    main_purpose: str,
    crowd_pref: str,
    people: List[PersonInput],
    openai_api_key: str = "",
    refill_step: int = 50,
    base_limit: int = 200,
    max_limit: int = 500,
) -> List[Dict[str, Any]]:
    if not places:
        logger.warning("no places available; returning empty recommendations")
        return []

    extra_people = [p for p in people if not p.is_me]
    extra_keywords = expand_keywords_with_openai(openai_api_key, main_taste, main_purpose, extra_people)

    user_text = " ".join(
        [
            main_taste or "",
            main_purpose or "",
            " ".join([p.taste or "" for p in extra_people]),
            " ".join([p.purpose or "" for p in extra_people]),
        ]
    )

    # reset if signature changed
    if st.session_state.get("reco_signature") != signature:
        st.session_state.reco_signature = signature
        st.session_state.seen_place_ids = set()
        st.session_state.feed_buffer = []
        st.session_state.master_pool = build_master_pool(places, user_text, extra_keywords)
        st.session_state.cursor = 0
        st.session_state.pool_limit = base_limit

    master_pool = st.session_state.master_pool
    logger.info("master pool size=%d", len(master_pool))
    logger.info(
        "places=%d name_nonempty=%d gu_nonempty=%d",
        len(places),
        len([p for p in places if p.get("name")]),
        len([p for p in places if p.get("gu")]),
    )

    def refill():
        limit = min(st.session_state.pool_limit, len(master_pool))
        before = len(st.session_state.feed_buffer)
        while st.session_state.cursor < limit and len(st.session_state.feed_buffer) < limit:
            st.session_state.feed_buffer.append(master_pool[st.session_state.cursor])
            st.session_state.cursor += 1
        logger.info(
            "buffer refill: limit=%d cursor=%d size_before=%d size_after=%d",
            limit,
            st.session_state.cursor,
            before,
            len(st.session_state.feed_buffer),
        )

    def take(count: int) -> List[Dict[str, Any]]:
        results = []
        seen_areas: set = set()
        before = len(st.session_state.feed_buffer)
        while st.session_state.feed_buffer and len(results) < count:
            item = st.session_state.feed_buffer.pop(0)
            pid = item.get("place_id")
            if pid in st.session_state.seen_place_ids:
                continue
            area_name = item.get("area") or ""
            if area_name and area_name in seen_areas:
                continue
            st.session_state.seen_place_ids.add(pid)
            if area_name:
                seen_areas.add(area_name)
            results.append(item)
        logger.info("buffer take: before=%d after=%d returned=%d", before, len(st.session_state.feed_buffer), len(results))
        return results

    refill()
    results = take(RECOMMEND_COUNT)

    # relaxation: expand pool_limit and refill
    if len(results) < RECOMMEND_COUNT:
        st.session_state.pool_limit = min(max_limit, len(master_pool))
        refill()
        results += take(RECOMMEND_COUNT - len(results))

    if len(results) < RECOMMEND_COUNT:
        st.session_state.pool_limit = len(master_pool)
        refill()
        results += take(RECOMMEND_COUNT - len(results))

    # final top-up: allow duplicates (seen_place_ids 유지) if still short
    if len(results) < RECOMMEND_COUNT:
        logger.warning("topup relaxation: allowing duplicates to fill")
        for item in master_pool:
            if len(results) >= RECOMMEND_COUNT:
                break
            results.append(item)

    # 경고: 언어코드 형태 name 검출
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
    bad = [r.get("name") for r in results if is_language_code(str(r.get("name", "")))]
    if bad:
        logger.warning("language-code names in results: %s", bad)

    logger.info("returned=%d ids=%s", len(results), [r.get("place_id") for r in results])
    return results


# -----------------------------
# UI Components
# -----------------------------
def header_ui():
    st.markdown(
        f"""
        <style>
          @import url('https://fonts.googleapis.com/css2?family=Noto+Sans+KR:wght@600;700;800&display=swap');
          .app-title-kr {{
            font-family: 'Noto Sans KR', 'Apple SD Gothic Neo', 'Malgun Gothic', sans-serif;
            font-size: 42px;
            font-weight: 800;
            line-height: 1.05;
            letter-spacing: -0.5px;
          }}
          .app-title-en {{
            font-family: 'Noto Sans KR', 'Apple SD Gothic Neo', 'Malgun Gothic', sans-serif;
            font-size: 18px;
            font-weight: 600;
            color: #666;
          }}
        </style>
        <div style="padding: 4px 0 14px 0; text-align: center;">
          <div class="app-title-kr">🏙️ {APP_NAME_KR} 🏙️</div>
        </div>
        """,
        unsafe_allow_html=True
    )

def sidebar_keys_ui():
    st.sidebar.markdown("## API 키")
    openai_key = st.sidebar.text_input("OpenAI API Key", type="password", value=os.getenv("OPENAI_API_KEY", ""))
    seoul_key = st.sidebar.text_input("서울 열린데이터광장 인증키", type="password", value=os.getenv("SEOUL_API_KEY", ""))
    photo_korea_key = st.sidebar.text_input(
        "포토코리아 Service Key",
        type="password",
        value=os.getenv("PHOTO_KOREA_API_KEY", ""),
    )
    st.sidebar.markdown("---")
    st.sidebar.markdown(
        """
        - OpenAI 키: 카드 내 추천 이유/코스 문구 생성에 사용  
        - 서울 열린데이터 키: 실시간 인구/관광명소 데이터 호출에 사용  
        - 포토코리아 키: 지역 대표 이미지 다운로드 스크립트 실행 시 사용  
        """
    )
    return openai_key, seoul_key, photo_korea_key


# -----------------------------
# Local region images / metadata
# -----------------------------
@st.cache_data(ttl=3600)
def load_regions_config(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, list) else []
    except Exception:
        return []


def _normalize_region_text(text: str) -> str:
    if not text:
        return ""
    text = re.sub(r"[^\w\s가-힣]", " ", text)
    text = re.sub(r"\s+", "", text).lower()
    return text


@st.cache_data(ttl=3600)
def build_region_index(regions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    indexed = []
    for r in regions:
        if not isinstance(r, dict):
            continue
        name_ko = str(r.get("name_ko", "")).strip()
        keywords = r.get("keywords", [])
        if not isinstance(keywords, list):
            keywords = []
        indexed.append(
            {
                "id": str(r.get("id", "")).strip(),
                "name_ko": name_ko,
                "keywords": [str(k).strip() for k in keywords if str(k).strip()],
                "name_norm": _normalize_region_text(name_ko),
                "keywords_norm": [_normalize_region_text(str(k)) for k in keywords],
            }
        )
    return indexed


def match_region_id(area_name: str, indexed_regions: List[Dict[str, Any]]) -> str:
    if not area_name or not indexed_regions:
        return ""
    area_norm = _normalize_region_text(area_name)
    best_id = ""
    best_score = 0
    for r in indexed_regions:
        score = 0
        if area_norm and area_norm in r["name_norm"]:
            score = 2
        elif area_norm and any(area_norm == k for k in r["keywords_norm"]):
            score = 1
        if score > best_score and r["id"]:
            best_score = score
            best_id = r["id"]
    return best_id


def get_region_keywords(area_name: str, indexed_regions: List[Dict[str, Any]]) -> List[str]:
    region_id = match_region_id(area_name, indexed_regions)
    if not region_id:
        fallback_map = {
            "성수": ["서울숲", "뚝섬", "성수동", "성수동 카페거리", "뚝섬한강공원"],
            "잠실": ["롯데월드타워", "롯데월드", "석촌호수", "올림픽공원", "잠실종합운동장"],
            "한남": ["한남동", "한강진", "리움미술관", "블루스퀘어", "UN빌리지", "한남오거리"],
        }
        return fallback_map.get(area_name, [])
    for r in indexed_regions:
        if r.get("id") == region_id:
            kws = r.get("keywords", [])
            return [k for k in kws if k]
    return []


def expand_dong_terms(terms: List[str]) -> List[str]:
    expanded: List[str] = []
    for t in terms:
        t = str(t).strip()
        if not t:
            continue
        expanded.append(t)
        # "동"이 붙지 않은 행정구/지명에 대해 동 붙이기
        if not t.endswith("동") and len(t) >= 2:
            expanded.append(f"{t}동")
    # 중복 제거
    seen = set()
    uniq = []
    for t in expanded:
        if t not in seen:
            seen.add(t)
            uniq.append(t)
    return uniq


def get_nearby_keywords(area_name: str, indexed_regions: List[Dict[str, Any]]) -> List[str]:
    nearby = NEARBY_BEST.get(area_name, [])
    if not nearby:
        return []
    keywords = []
    for n in nearby:
        keywords.append(n)
        region_kws = get_region_keywords(n, indexed_regions)
        keywords.extend(region_kws)
    # 중복 제거
    seen = set()
    uniq = []
    for k in keywords:
        k = str(k).strip()
        if k and k not in seen:
            seen.add(k)
            uniq.append(k)
    return uniq


@st.cache_data(ttl=3600)
def load_regions_meta(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def resolve_region_image_url(
    area_name: str,
    indexed_regions: List[Dict[str, Any]],
    regions_meta: Dict[str, Any],
) -> Tuple[str, str]:
    region_id = match_region_id(area_name, indexed_regions)
    if region_id:
        meta = regions_meta.get(region_id, {})
        url = str(
            meta.get("origin_url")
            or meta.get("image_url")
            or meta.get("url")
            or ""
        ).strip()
        if url.startswith("http://"):
            url = "https://" + url[len("http://"):]
        if url:
            return url, region_id
    return "", ""


@st.cache_data(ttl=3600)
def fetch_photo_korea_image_url(
    api_key: str,
    keyword: str,
    avoid_urls: Tuple[str, ...] = (),
    required_terms: Tuple[str, ...] = (),
    required_city: str = "서울",
) -> Dict[str, Any]:
    """
    포토코리아 API에서 keyword로 대표 이미지 URL/캡션/촬영자 정보를 1건 가져온다.
    TODO: 포토코리아 OpenAPI 문서에 맞춰 엔드포인트/파라미터를 조정하세요.
    """
    if not api_key or not keyword:
        return {}

    url = "https://apis.data.go.kr/B551011/PhotoGalleryService1/gallerySearchList1"
    params = {
        "serviceKey": api_key,
        "numOfRows": 10,
        "pageNo": 1,
        "MobileOS": "ETC",
        "MobileApp": "SeoulNomads",
        "_type": "json",
        "keyword": keyword,
    }

    def parse_int(v: Any) -> int:
        try:
            return int(str(v).strip())
        except Exception:
            return 0

    def pick_first_item(items: List[Dict[str, Any]]) -> Dict[str, Any]:
        if not items:
            return {}
        scored = []
        landscape_items = []
        for it in items:
            image_url = (
                it.get("galWebImageUrl")
                or it.get("galWebImageUrl1")
                or it.get("galWebImageUrl2")
                or it.get("originImgUrl")
                or it.get("imageUrl")
                or ""
            )
            if image_url in avoid_urls:
                continue
            title = str(it.get("galTitle", "") or it.get("title", ""))
            loc = str(it.get("galPhotographyLocation", "") or it.get("location", ""))
            addr = str(it.get("addr") or it.get("address") or it.get("galPhotographyLocation") or "")
            w = parse_int(it.get("galWebImageWidth") or it.get("imageWidth") or it.get("width"))
            h = parse_int(it.get("galWebImageHeight") or it.get("imageHeight") or it.get("height"))
            text = f"{title} {loc} {addr}"
            if required_city and required_city not in text:
                continue
            if required_terms:
                if not any(term in text for term in required_terms):
                    continue
            is_seoul = 1 if "서울" in text else 0
            is_landscape = 1 if w and h and w >= h else 0
            # 풍경/전경/전망 같은 키워드 가점
            scenic = 1 if any(k in text for k in ["풍경", "전경", "전망", "야경"]) else 0
            # 4:3에 가까울수록 가점
            ratio_score = 0
            if w and h:
                ratio = w / h
                ratio_score = -abs(ratio - (4 / 3))
            scored.append((is_seoul, scenic, is_landscape, ratio_score, it))
            if is_landscape:
                landscape_items.append((is_seoul, scenic, ratio_score, it))

        if landscape_items:
            landscape_items.sort(key=lambda x: (x[0], x[1], x[2]), reverse=True)
            return landscape_items[0][3]

        scored.sort(key=lambda x: (x[0], x[1], x[2], x[3]), reverse=True)
        return scored[0][4]

    try:
        r = requests.get(url, params=params, timeout=15)
        r.raise_for_status()
        try:
            data = r.json()
            items = data.get("response", {}).get("body", {}).get("items", {}).get("item", [])
            if isinstance(items, dict):
                items = [items]
            item = pick_first_item(items)
        except Exception:
            # XML fallback
            import xml.etree.ElementTree as ET
            root = ET.fromstring(r.text)
            items = []
            for elem in root.iter():
                if elem.tag.lower().endswith("item"):
                    item = {}
                    for child in list(elem):
                        item[child.tag] = child.text or ""
                    if item:
                        items.append(item)
            item = pick_first_item(items)

        if not item:
            return {}

        image_url = (
            item.get("galWebImageUrl")
            or item.get("galWebImageUrl1")
            or item.get("galWebImageUrl2")
            or item.get("originImgUrl")
            or item.get("imageUrl")
            or ""
        )
        title = item.get("galTitle") or ""
        location = item.get("galPhotographyLocation") or ""
        photographer = item.get("galPhotographer") or item.get("photographer") or ""
        width = parse_int(item.get("galWebImageWidth") or item.get("imageWidth") or item.get("width"))
        height = parse_int(item.get("galWebImageHeight") or item.get("imageHeight") or item.get("height"))

        if isinstance(image_url, str) and image_url.startswith("http://"):
            image_url = "https://" + image_url[len("http://"):]

        caption = title or location or f"{keyword} 풍경"
        credit = "ⓒ한국관광공사 사진갤러리"
        if photographer:
            credit = f"ⓒ한국관광공사 사진갤러리-{photographer}"

        orientation = "unknown"
        if width and height:
            orientation = "landscape" if width >= height else "portrait"

        return {
            "url": image_url,
            "caption": caption,
            "credit": credit,
            "orientation": orientation,
        }
    except Exception:
        return {}


def get_image_and_meta(
    area_name: str,
    indexed_regions: List[Dict[str, Any]],
    regions_meta: Dict[str, Any],
    photo_korea_key: str,
    used_urls: set,
) -> Tuple[str, str, str, str]:
    img_url, region_id = resolve_region_image_url(area_name, indexed_regions, regions_meta)
    if img_url:
        meta = regions_meta.get(region_id, {}) if region_id else {}
        caption = meta.get("caption") or meta.get("title") or f"{area_name} 풍경"
        credit = meta.get("credit") or "ⓒ한국관광공사 사진갤러리"
        return img_url, caption, credit, "unknown"

    # 1차: 지역명/확장 키워드로 "엄격 매칭" 시도
    avoid = tuple(used_urls)
    region_keywords = get_region_keywords(area_name, indexed_regions)
    search_terms = expand_dong_terms([area_name] + region_keywords)
    required_terms = tuple(search_terms)

    fallback = fetch_photo_korea_image_url(
        photo_korea_key,
        f"서울 {area_name} 풍경",
        avoid_urls=avoid,
        required_terms=required_terms,
        required_city="서울",
    )
    if fallback.get("url"):
        return (
            fallback.get("url", ""),
            fallback.get("caption", f"{area_name} 풍경"),
            fallback.get("credit", "ⓒ한국관광공사 사진갤러리"),
            fallback.get("orientation", "unknown"),
        )

    for kw in search_terms:
        if not kw:
            continue
        fallback = fetch_photo_korea_image_url(
            photo_korea_key,
            f"서울 {kw} 풍경",
            avoid_urls=avoid,
            required_terms=required_terms,
            required_city="서울",
        )
        if fallback.get("url"):
            return (
                fallback.get("url", ""),
                fallback.get("caption", f"{area_name} 풍경"),
                fallback.get("credit", "ⓒ한국관광공사 사진갤러리"),
                fallback.get("orientation", "unknown"),
            )

    # 2차: 엄격 매칭 실패 시, 동일 키워드로 완화 재시도(그래도 관련성 높은 것 우선)
    fallback = fetch_photo_korea_image_url(
        photo_korea_key,
        f"서울 {area_name} 풍경",
        avoid_urls=avoid,
        required_terms=(),
        required_city="서울",
    )
    if fallback.get("url"):
        return (
            fallback.get("url", ""),
            fallback.get("caption", f"{area_name} 풍경"),
            fallback.get("credit", "ⓒ한국관광공사 사진갤러리"),
            fallback.get("orientation", "unknown"),
        )

    for kw in search_terms:
        if not kw:
            continue
        fallback = fetch_photo_korea_image_url(
            photo_korea_key,
            f"서울 {kw} 풍경",
            avoid_urls=avoid,
            required_terms=(),
            required_city="서울",
        )
        if fallback.get("url"):
            return (
                fallback.get("url", ""),
                fallback.get("caption", f"{area_name} 풍경"),
                fallback.get("credit", "ⓒ한국관광공사 사진갤러리"),
                fallback.get("orientation", "unknown"),
            )

    # 3차: "풍경" 없이도 검색 (일부 키워드는 풍경과 같이 검색 시 결과가 줄어듦)
    fallback = fetch_photo_korea_image_url(
        photo_korea_key,
        f"서울 {area_name}",
        avoid_urls=avoid,
        required_terms=(),
        required_city="서울",
    )
    if fallback.get("url"):
        return (
            fallback.get("url", ""),
            fallback.get("caption", f"{area_name} 풍경"),
            fallback.get("credit", "ⓒ한국관광공사 사진갤러리"),
            fallback.get("orientation", "unknown"),
        )

    for kw in search_terms:
        if not kw:
            continue
        fallback = fetch_photo_korea_image_url(
            photo_korea_key,
            f"서울 {kw}",
            avoid_urls=avoid,
            required_terms=(),
            required_city="서울",
        )
        if fallback.get("url"):
            return (
                fallback.get("url", ""),
                fallback.get("caption", f"{area_name} 풍경"),
                fallback.get("credit", "ⓒ한국관광공사 사진갤러리"),
                fallback.get("orientation", "unknown"),
            )

    # 인근 지역 키워드 fallback은 정확도 저하 가능성이 있어 기본적으로 사용하지 않음

    return "", f"{area_name} 풍경", "ⓒ한국관광공사 사진갤러리", "unknown"


def render_region_card(
    area_name: str,
    crowd_label: str,
    image_url: str,
    orientation: str = "unknown",
    height_px: int = 300,
):
    color = CROWD_COLOR.get(crowd_label, "gray")
    # 인라인 SVG placeholder (디코더 의존 없이 사용)
    svg_placeholder = (
        "data:image/svg+xml;utf8,"
        "<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 800 600'>"
        "<defs><linearGradient id='g' x1='0' x2='1' y1='0' y2='1'>"
        "<stop offset='0%' stop-color='%23f2f2f2'/>"
        "<stop offset='100%' stop-color='%23e6e6e6'/>"
        "</linearGradient></defs>"
        "<rect width='800' height='600' fill='url(%23g)'/>"
        "<text x='50%' y='50%' dominant-baseline='middle' text-anchor='middle' "
        "fill='%23999' font-size='28' font-family='sans-serif'>"
        "Image Unavailable</text>"
        "</svg>"
    )
    if image_url:
        # 4:3 비율 카드로 강제 (가로형 우선)
        safe_url = image_url.replace("'", "%27")
        fit = "cover" if orientation != "portrait" else "contain"
        html = f"""
        <div style="position:relative; border-radius:16px; overflow:hidden; border:1px solid #e6e6e6;">
          <img src="{safe_url}" onerror="this.onerror=null;this.src='{svg_placeholder}';"
               style="width:100%; height:{height_px}px; object-fit:{fit}; display:block; background:#f2f2f2;">
          <div style="position:absolute; top:10px; left:10px;
                      padding:6px 10px; border-radius:8px; background:white;
                      font-weight:700; border:1px solid #e0e0e0; color:{color};">
            {crowd_label}
          </div>
          <div style="position:absolute; bottom:10px; left:10px;
                      padding:6px 10px; border-radius:8px; background:rgba(0,0,0,0.55);
                      color:white; font-weight:700;">
            {area_name}
          </div>
        </div>
        """
        st.markdown(html, unsafe_allow_html=True)
        return

    # 이미지 URL이 없으면 HTML 카드로 fallback (Streamlit 디코더 사용 안 함)
    html = f"""
    <div style="position:relative; border-radius:16px; overflow:hidden; border:1px solid #e6e6e6;
                background:linear-gradient(135deg,#f2f2f2,#e8e8e8); height:{height_px}px;">
      <div style="position:absolute; top:10px; left:10px;
                  padding:6px 10px; border-radius:8px; background:white;
                  font-weight:700; border:1px solid #e0e0e0; color:{color};">
        {crowd_label}
      </div>
      <div style="position:absolute; bottom:10px; left:10px;
                  padding:6px 10px; border-radius:8px; background:rgba(0,0,0,0.55);
                  color:white; font-weight:700;">
        {area_name}
      </div>
    </div>
    """
    st.markdown(html, unsafe_allow_html=True)

def crowd_slider_ui() -> str:
    return st.select_slider(
        "혼잡도",
        options=CROWD_LEVELS,
        value=CROWD_LEVELS[1],
        label_visibility="collapsed",
    )

def parse_taste_purpose(raw: str) -> Tuple[str, str]:
    if not raw:
        return "", ""

    text = raw.strip()
    taste = ""
    purpose = ""

    # "취향: ..." / "목적: ..." 형식 우선 파싱
    mt = re.search(r"취향\s*[:\-]\s*(.+)", text)
    mp = re.search(r"목적\s*[:\-]\s*(.+)", text)
    if mt:
        taste = mt.group(1).splitlines()[0].strip()
    if mp:
        purpose = mp.group(1).splitlines()[0].strip()
    if taste or purpose:
        return taste, purpose

    # 구분자 기반 파싱
    if "/" in text:
        parts = [p.strip() for p in text.split("/", 1)]
        taste = parts[0]
        purpose = parts[1] if len(parts) > 1 else ""
        return taste, purpose

    # 줄바꿈 기반 파싱
    lines = [l.strip() for l in text.splitlines() if l.strip()]
    if len(lines) >= 2:
        return lines[0], lines[1]

    return text, ""

def location_ui(prefix: str, default_scope: str = "서울 내") -> StartLocation:
    scope = st.radio(
        "출발 지역",
        options=["서울 내", "서울 외부"],
        horizontal=True,
        index=0 if default_scope == "서울 내" else 1,
        key=f"{prefix}_scope",
        label_visibility="collapsed",
    )

    if scope == "서울 내":
        gu = st.text_input(
            "구",
            key=f"{prefix}_gu",
            placeholder="구",
            label_visibility="collapsed",
        )
        dong = st.text_input(
            "동",
            key=f"{prefix}_dong",
            placeholder="동",
            label_visibility="collapsed",
        )
        return StartLocation(scope=scope, gu=gu, dong=dong, si="")
    else:
        si = st.text_input(
            "시",
            key=f"{prefix}_si",
            placeholder="시",
            label_visibility="collapsed",
        )
        dong = st.text_input(
            "동",
            key=f"{prefix}_dong_out",
            placeholder="동",
            label_visibility="collapsed",
        )
        return StartLocation(scope=scope, si=si, dong=dong, gu="")

def person_block_ui(i: int, person: PersonInput):
    st.markdown("#### 관계")
    person.relationship = st.text_input(
        "관계",
        value=person.relationship,
        key=f"p{i}_rel",
        placeholder="관계",
        label_visibility="collapsed",
    )
    st.markdown("#### 취향과 목적")
    combined = ""
    if person.taste or person.purpose:
        lines = []
        if person.taste:
            lines.append(f"취향: {person.taste}")
        if person.purpose:
            lines.append(f"목적: {person.purpose}")
        combined = "\n".join(lines)
    raw = st.text_area(
        "취향과 목적 (자유입력)",
        value=combined,
        height=110,
        key=f"p{i}_taste_purpose",
        placeholder="취향과 목적 (자유입력)",
        label_visibility="collapsed",
    )
    person.taste, person.purpose = parse_taste_purpose(raw)
    st.markdown("#### 출발 지역")
    loc = location_ui(prefix=f"p{i}_", default_scope=person.start_location.scope)
    person.start_location = loc

def naver_map_link(area_name: str) -> str:
    # URL 직접 노출 요구가 있어 그대로 구성
    return f"https://map.naver.com/v5/search/{requests.utils.quote(area_name)}"

def kakao_map_link(area_name: str) -> str:
    return f"https://map.kakao.com/?q={requests.utils.quote(area_name)}"

def google_map_link(area_name: str) -> str:
    return f"https://www.google.com/maps/search/?api=1&query={requests.utils.quote(area_name)}"

def render_map(area_center: Tuple[float, float], label: str, height_px: int = 300):
    lat, lon = area_center
    deck = pdk.Deck(
        map_style="https://basemaps.cartocdn.com/gl/positron-gl-style/style.json",
        initial_view_state=pdk.ViewState(latitude=lat, longitude=lon, zoom=12, pitch=0),
        layers=[
            pdk.Layer(
                "ScatterplotLayer",
                data=[{"lat": lat, "lon": lon, "name": label}],
                get_position="[lon, lat]",
                get_radius=200,
                get_fill_color="[255, 0, 0, 180]",
                pickable=True,
            ),
            pdk.Layer(
                "TextLayer",
                data=[{"lat": lat, "lon": lon, "name": label}],
                get_position="[lon, lat]",
                get_text="name",
                get_size=22,
                get_color="[0, 0, 0, 230]",
                get_text_anchor="middle",
                get_alignment_baseline="bottom",
                pickable=False,
            ),
        ],
        tooltip={"text": "{name}"}
    )
    st.pydeck_chart(deck, use_container_width=True, height=height_px)
    st.markdown(
        "<div style='text-align:center; font-size:12px; color:#666;'>"
        "지도를 확대/이동하면 주변 지역도 함께 확인할 수 있습니다."
        "</div>",
        unsafe_allow_html=True,
    )

def crowd_badge(label: str, title: str = "실시간 혼잡도"):
    color = CROWD_COLOR.get(label, "gray")
    st.markdown(
        f"""
        <div style="display:inline-block; padding:6px 10px; border-radius:8px;
             border:1px solid #ddd; font-weight:700; color:{color};">
          {title}: {label}
        </div>
        """,
        unsafe_allow_html=True
    )


# -----------------------------
# Transit time estimation (heuristic)
# -----------------------------
def estimate_travel_time(start: StartLocation, area: Dict[str, Any]) -> Tuple[int, str]:
    """
    간이 추정:
    - 같은 구: 20~30분 (지하철)
    - 다른 구(서울 내): 40~60분 (지하철)
    - 서울 외부: 70~100분 (지하철/버스)
    """
    if not start:
        return 0, ""

    area_gu = area.get("gu", "")
    area_name = area.get("area", "")

    if start.scope == "서울 외부":
        return 90, "지하철/버스"

    # 서울 내
    if start.gu and area_gu and start.gu.strip() == area_gu.strip():
        return 25, "지하철"

    if start.dong and area_name and area_name in start.dong:
        return 15, "지하철"

    if start.gu:
        return 50, "지하철"

    return 55, "지하철"


def build_travel_time_lines(people: List[PersonInput], area: Dict[str, Any]) -> List[str]:
    lines: List[str] = []
    for idx, p in enumerate(people):
        loc = p.start_location
        if not loc:
            continue
        # 최소한 구/동/시 정보가 있을 때만 표기
        if not (loc.gu or loc.dong or loc.si):
            continue
        mins, mode = estimate_travel_time(loc, area)
        if mins <= 0:
            continue
        label = "본인" if p.is_me else (p.relationship or f"동행자 {idx}")
        lines.append(f"{label}: 약 {mins}분 ({mode})")
    return lines


def haversine_km(a: Tuple[float, float], b: Tuple[float, float]) -> float:
    lat1, lon1 = a
    lat2, lon2 = b
    r = 6371.0
    p1 = math.radians(lat1)
    p2 = math.radians(lat2)
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    s = math.sin(dlat / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlon / 2) ** 2
    c = 2 * math.atan2(math.sqrt(s), math.sqrt(1 - s))
    return r * c


def estimate_start_center(places: List[Dict[str, Any]], start: StartLocation) -> Optional[Tuple[float, float]]:
    if not start:
        return None
    key = (start.dong or "").strip()
    if key:
        matches = [
            p for p in places
            if isinstance(p.get("lat"), (int, float))
            and isinstance(p.get("lng"), (int, float))
            and key in str(p.get("address", ""))
        ]
    else:
        gu = (start.gu or "").strip()
        matches = [
            p for p in places
            if isinstance(p.get("lat"), (int, float))
            and isinstance(p.get("lng"), (int, float))
            and gu
            and gu in str(p.get("address", ""))
        ]
    if not matches:
        return None
    lats = [p["lat"] for p in matches]
    lngs = [p["lng"] for p in matches]
    return (sum(lats) / len(lats), sum(lngs) / len(lngs))


def build_distance_lines(people: List[PersonInput], area: Dict[str, Any], places_pool: List[Dict[str, Any]]) -> List[str]:
    lines: List[str] = []
    dest = area.get("center")
    if not dest:
        return lines
    for idx, p in enumerate(people):
        label = "본인" if p.is_me else (p.relationship or f"동행자 {idx}")
        start_center = estimate_start_center(places_pool, p.start_location)
        if not start_center:
            continue
        km = haversine_km(start_center, dest)
        lines.append(f"{label}: 약 {km:.1f}km")
    return lines


@st.cache_data(ttl=3600)
def get_nearby_stations_openai(
    openai_api_key: str,
    area_name: str,
    address: str,
) -> List[str]:
    if not openai_api_key:
        return []
    url = "https://api.openai.com/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {openai_api_key}",
        "Content-Type": "application/json",
    }
    sys = "서울 지하철역 추천기. JSON만 반환."
    user = {
        "place": area_name,
        "address": address,
        "format": {"stations": ["string", "string", "string"]},
    }
    payload = {
        "model": "gpt-4o-mini",
        "messages": [
            {"role": "system", "content": sys},
            {"role": "user", "content": json.dumps(user, ensure_ascii=False)},
        ],
        "temperature": 0.3,
        "response_format": {"type": "json_object"},
    }
    try:
        resp = requests.post(url, headers=headers, json=payload, timeout=20)
        resp.raise_for_status()
        content = resp.json()["choices"][0]["message"]["content"]
        data = json.loads(content)
        stations = data.get("stations", [])
        if isinstance(stations, list):
            return [str(s).strip() for s in stations if str(s).strip()]
    except Exception:
        return []
    return []


@st.cache_data(ttl=3600)
def get_travel_times_openai(
    openai_api_key: str,
    area_name: str,
    address: str,
    people: List[PersonInput],
) -> List[str]:
    if not openai_api_key:
        return []
    url = "https://api.openai.com/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {openai_api_key}",
        "Content-Type": "application/json",
    }
    sys = "대중교통 이동시간 추정기. JSON만 반환."
    user = {
        "destination": {"name": area_name, "address": address},
        "origins": [
            {
                "label": "본인" if p.is_me else (p.relationship or "동행자"),
                "scope": p.start_location.scope,
                "si": p.start_location.si,
                "gu": p.start_location.gu,
                "dong": p.start_location.dong,
            }
            for p in people
        ],
        "format": {"times": [{"label": "string", "minutes": 0, "mode": "지하철/버스"}]},
    }
    payload = {
        "model": "gpt-4o-mini",
        "messages": [
            {"role": "system", "content": sys},
            {"role": "user", "content": json.dumps(user, ensure_ascii=False)},
        ],
        "temperature": 0.2,
        "response_format": {"type": "json_object"},
    }
    try:
        resp = requests.post(url, headers=headers, json=payload, timeout=20)
        resp.raise_for_status()
        content = resp.json()["choices"][0]["message"]["content"]
        data = json.loads(content)
        times = data.get("times", [])
        lines = []
        if isinstance(times, list):
            for t in times:
                label = str(t.get("label", "")).strip()
                mins = t.get("minutes", None)
                mode = str(t.get("mode", "")).strip() or "지하철"
                if label and isinstance(mins, int):
                    lines.append(f"{label}: 약 {mins}분 ({mode})")
        return lines
    except Exception:
        return []


# -----------------------------
# Main App
# -----------------------------
def main():
    init_state()
    header_ui()
    openai_key, seoul_key, photo_korea_key = sidebar_keys_ui()

    regions_config = load_regions_config(REGIONS_JSON_PATH)
    indexed_regions = build_region_index(regions_config)
    regions_meta = load_regions_meta(REGIONS_META_PATH)

    # 핫링크 모드에서는 메타가 비어도 앱이 동작하도록 경고를 띄우지 않음

    if "view" not in st.session_state:
        st.session_state.view = "list"
    if "selected_area_name" not in st.session_state:
        st.session_state.selected_area_name = None
    if "used_image_urls" not in st.session_state:
        st.session_state.used_image_urls = set()

    # -------------------------
    # Main input area
    # -------------------------
    st.markdown("## 당신의 조건과 취향")
    left_col, right_col = st.columns([0.62, 0.38], gap="large")

    with left_col:
        with st.form("main_form"):
            st.markdown("### 취향과 목적")
            st.markdown(
                """
                <div style="display:inline-block; padding:8px 10px; border:1px solid #e0e0e0;
                            border-radius:8px; background:#f7f7f7; font-size:12px; color:#666; margin-bottom:8px;">
                  <div style="font-weight:600; margin-bottom:4px;">예시</div>
                  <div>취향 - 미식, 쇼핑, 전통, 액티비티, 카페투어, 자연</div>
                  <div>목적 - 데이트, 가족, 의전, 혼자여행, 친구모임, 기념일</div>
                </div>
                """,
                unsafe_allow_html=True,
            )
            main_text = st.text_area(
                "취향과 목적 (자유입력)",
                height=140,
                key="main_taste_purpose",
                placeholder="취향과 목적 (자유입력)",
                label_visibility="collapsed",
            )
            main_taste, main_purpose = parse_taste_purpose(main_text)

            st.markdown("### 혼잡도")
            crowd_pref = crowd_slider_ui()

            st.markdown("### 출발 지역")
            main_loc = location_ui(prefix="main_", default_scope="서울 내")

            # 인원 관리 버튼들은 form 밖에서 처리하는 게 안정적이지만,
            # UX를 위해 form 안에서도 보이게 하고 실제 동작은 form 밖에서 처리.

            # 버튼 배치: [모두 재추천] [추천 실행] [비선호 재추천]
            b_left, b_center, b_right = st.columns([1, 1, 1])
            with b_center:
                run = st.form_submit_button("추천 실행", use_container_width=True, type="primary")
            show_reco_buttons = bool(st.session_state.last_reco) or run
            with b_left:
                if show_reco_buttons:
                    rerank_all_clicked = st.form_submit_button("모두 재추천", use_container_width=True)
                else:
                    rerank_all_clicked = False
            with b_right:
                if show_reco_buttons:
                    rerank_dislike_clicked = st.form_submit_button("비선호 재추천", use_container_width=True)
                else:
                    rerank_dislike_clicked = False

        # form 바깥: 인원 추가/제거 컨트롤
        c1, c2 = st.columns([0.5, 0.5])
        with c1:
            if st.button("동행자 추가", use_container_width=True, disabled=(len(st.session_state.people) >= 1 + MAX_EXTRA_PEOPLE)):
                if len(st.session_state.people) < 1 + MAX_EXTRA_PEOPLE:
                    st.session_state.people.append(default_person(is_me=False))
                    st.rerun()
        with c2:
            if st.button("동행자 제거", use_container_width=True, disabled=(len(st.session_state.people) <= 1)):
                if len(st.session_state.people) > 1:
                    st.session_state.people.pop()
                    st.rerun()
        st.info("동행자는 최대 2명까지 추가 가능하며, 공란으로 둘 시 추천에 반영하지 않습니다.", icon=None)

    # 동행자 입력 UI (오른쪽 영역)
    with right_col:
        if len(st.session_state.people) > 1:
            for idx in range(1, len(st.session_state.people)):
                st.markdown(f"### 동행자 {idx}")
                with st.container(border=True):
                    person_block_ui(idx, st.session_state.people[idx])

    # -------------------------
    # Recommendation logic + render
    # -------------------------
    if run:
        # 메인 사용자 정보 세션 반영
        me = st.session_state.people[0]
        me.taste = main_taste
        me.purpose = main_purpose
        me.start_location = main_loc
        st.session_state.people[0] = me

        signature = make_signature(main_taste, main_purpose, crowd_pref, st.session_state.people)
        st.session_state.last_signature = signature

        # 데이터 fetch
        pop_raw = fetch_seoul_realtime_population(seoul_key)
        places = get_tour_places()
        assert all("area" in p for p in places), "area field missing in places"
        unique_gus = sorted({p.get("gu") for p in places if p.get("gu")})
        gu_list = tuple(sorted({p.get("gu", "") for p in places if p.get("gu")}))
        tour_spot_index = build_tour_spot_index(places, gu_list)
        with_coords = [p for p in places if isinstance(p.get("lat"), (int, float)) and isinstance(p.get("lng"), (int, float))]
        no_gu = [p for p in places if not p.get("gu")]
        name_empty = len([p for p in places if not p.get("name")])
        addr_empty = len([p for p in places if not p.get("address")])
        gu_empty = len([p for p in places if not p.get("gu")])
        logger.info("places=%d unique_gu=%d (%s)", len(places), len(unique_gus), unique_gus[:30])
        logger.info(
            "normalize empty ratios: name=%d addr=%d gu=%d",
            name_empty,
            addr_empty,
            gu_empty,
        )
        logger.info("places_with_coords=%d no_gu=%d", len(with_coords), len(no_gu))
        if places:
            sample = places[:5]
            logger.info(
                "sample places: %s",
                [
                    {
                        "name": s.get("name"),
                        "gu": s.get("gu"),
                        "lat": s.get("lat"),
                        "lng": s.get("lng"),
                        "address": s.get("address"),
                    }
                    for s in sample
                ],
            )
        logger.info("master pool init for signature=%s", signature)

        # 디버그: 투어 스팟/후보군 개수 표시
        st.sidebar.caption(f"투어 스팟: {len(places)}개")

        recos = get_recommendations_from_places(
            places=places,
            signature=signature,
            main_taste=main_taste,
            main_purpose=main_purpose,
            crowd_pref=crowd_pref,
            people=st.session_state.people,
            openai_api_key=openai_key,
        )
        st.session_state.last_reco = recos

    def crowd_pref_from_ui(signature: str) -> str:
        # slider는 rerun 시 main_form에서 설정된 값을 가져오기 어렵기 때문에
        # 가장 최근 입력값(세션에 남아있을 수 있음)을 최대한 사용
        # st.slider 값 key가 없어서 여기선 사용자 선택을 다시 추정 불가 → last_signature에 저장된 crowd를 이용
        try:
            sig = json.loads(signature)
            cp = sig.get("crowd_pref", "약간 붐빔")
            if cp in CROWD_LEVELS:
                return cp
        except Exception:
            pass
        return "약간 붐빔"

    def rerank_after_dislike(signature: str, selected_dislikes: List[str]):
        if not selected_dislikes:
            return

        disliked_set = st.session_state.disliked.get(signature, set())
        disliked_set.update(selected_dislikes)
        # place_id 기반 제외도 함께 반영
        for a in st.session_state.last_reco:
            if a.get("area") in selected_dislikes and a.get("place_id"):
                disliked_set.add(a.get("place_id"))
        st.session_state.disliked[signature] = disliked_set

        pop_raw = fetch_seoul_realtime_population(seoul_key)
        places = get_tour_places()
        assert all("area" in p for p in places), "area field missing in places"
        unique_gus = sorted({p.get("gu") for p in places if p.get("gu")})
        gu_list = tuple(sorted({p.get("gu", "") for p in places if p.get("gu")}))
        tour_spot_index = build_tour_spot_index(places, gu_list)
        with_coords = [p for p in places if isinstance(p.get("lat"), (int, float)) and isinstance(p.get("lng"), (int, float))]
        no_gu = [p for p in places if not p.get("gu")]
        name_empty = len([p for p in places if not p.get("name")])
        addr_empty = len([p for p in places if not p.get("address")])
        gu_empty = len([p for p in places if not p.get("gu")])
        logger.info("places=%d unique_gu=%d (%s)", len(places), len(unique_gus), unique_gus[:30])
        logger.info(
            "normalize empty ratios: name=%d addr=%d gu=%d",
            name_empty,
            addr_empty,
            gu_empty,
        )
        logger.info("places_with_coords=%d no_gu=%d", len(with_coords), len(no_gu))
        if places:
            sample = places[:5]
            logger.info(
                "sample places: %s",
                [
                    {
                        "name": s.get("name"),
                        "gu": s.get("gu"),
                        "lat": s.get("lat"),
                        "lng": s.get("lng"),
                        "address": s.get("address"),
                    }
                    for s in sample
                ],
            )
        logger.info("master pool reuse for signature=%s", signature)

        # 디버그: 투어 스팟/후보군 개수 표시
        st.sidebar.caption(f"투어 스팟: {len(places)}개")

        new_recos = get_recommendations_from_places(
            places=places,
            signature=signature,
            main_taste=st.session_state.people[0].taste,
            main_purpose=st.session_state.people[0].purpose,
            crowd_pref=crowd_pref_from_ui(signature),
            people=st.session_state.people,
            openai_api_key=openai_key,
        )
        st.session_state.last_reco = new_recos

    def rerank_all_recos(signature: str):
        # 현재 추천 결과를 비선호로 추가하여 동일 지역 재등장 방지
        current = st.session_state.last_reco or []
        if current:
            disliked_set = st.session_state.disliked.get(signature, set())
            for a in current:
                pid = a.get("place_id")
                if pid:
                    disliked_set.add(pid)
            st.session_state.disliked[signature] = disliked_set

        pop_raw = fetch_seoul_realtime_population(seoul_key)
        places = get_tour_places()
        assert all("area" in p for p in places), "area field missing in places"
        unique_gus = sorted({p.get("gu") for p in places if p.get("gu")})
        gu_list = tuple(sorted({p.get("gu", "") for p in places if p.get("gu")}))
        tour_spot_index = build_tour_spot_index(places, gu_list)
        with_coords = [p for p in places if isinstance(p.get("lat"), (int, float)) and isinstance(p.get("lng"), (int, float))]
        no_gu = [p for p in places if not p.get("gu")]
        name_empty = len([p for p in places if not p.get("name")])
        addr_empty = len([p for p in places if not p.get("address")])
        gu_empty = len([p for p in places if not p.get("gu")])
        logger.info("places=%d unique_gu=%d (%s)", len(places), len(unique_gus), unique_gus[:30])
        logger.info(
            "normalize empty ratios: name=%d addr=%d gu=%d",
            name_empty,
            addr_empty,
            gu_empty,
        )
        logger.info("places_with_coords=%d no_gu=%d", len(with_coords), len(no_gu))
        if places:
            sample = places[:5]
            logger.info(
                "sample places: %s",
                [
                    {
                        "name": s.get("name"),
                        "gu": s.get("gu"),
                        "lat": s.get("lat"),
                        "lng": s.get("lng"),
                        "address": s.get("address"),
                    }
                    for s in sample
                ],
            )
        logger.info("master pool reuse for signature=%s", signature)
        # 디버그: 투어 스팟/후보군 개수 표시
        st.sidebar.caption(f"투어 스팟: {len(places)}개")

        new_recos = get_recommendations_from_places(
            places=places,
            signature=signature,
            main_taste=st.session_state.people[0].taste,
            main_purpose=st.session_state.people[0].purpose,
            crowd_pref=crowd_pref_from_ui(signature),
            people=st.session_state.people,
            openai_api_key=openai_key,
        )
        st.session_state.last_reco = new_recos

    # 결과 화면: 추천 실행 시 메인 화면 아래 생성
    if st.session_state.last_reco:
        st.markdown("---")
        st.markdown("## 추천 결과")

        signature = st.session_state.last_signature
        recos = st.session_state.last_reco

        if rerank_all_clicked:
            rerank_all_recos(signature)
            st.rerun()

        if rerank_dislike_clicked:
            disliked_set = st.session_state.disliked.get(signature, set())
            rerank_after_dislike(signature, list(disliked_set))
            st.rerun()

        if st.session_state.view == "list":
            st.session_state.used_image_urls = set()
            rows = [recos[i:i + 2] for i in range(0, len(recos), 2)]
            for row in rows:
                row_cols = st.columns(2, gap="large")
                for col, area in zip(row_cols, row):
                    area_name = area.get("area") or ""
                    if is_excluded_place(area.get("name") or "", area.get("addr") or area.get("address") or ""):
                        continue
                    if not area_name or not is_korean_text(area_name):
                        continue
                    crowd_now = area.get("crowd_now", "약간 붐빔")
                    rank = recos.index(area) + 1

                    with col:
                        with st.container(border=True):
                            top_cols = st.columns([0.6, 0.4])
                            with top_cols[0]:
                                st.markdown(f"### ({rank}) {area_name}")
                            with top_cols[1]:
                                _spacer, _btn = st.columns([0.2, 0.8])
                                with _btn:
                                    if st.button("상세 보기", key=f"detail_{rank}", type="primary"):
                                        st.session_state.selected_area_name = area_name
                                        st.session_state.view = "detail"
                                        st.rerun()

                            # 1) 대표 이미지 (로컬 저장본)
                            img_url, caption, credit, orientation = get_image_and_meta(
                                area_name,
                                indexed_regions,
                                regions_meta,
                                photo_korea_key,
                                st.session_state.used_image_urls,
                            )
                            if img_url:
                                st.session_state.used_image_urls.add(img_url)
                            render_region_card(area_name, crowd_now, img_url, orientation, height_px=280)
                            st.markdown(
                                f"<div style='text-align:center; font-size:12px; color:#666;'>"
                                f"{caption}<br>[{credit}]</div>",
                                unsafe_allow_html=True,
                            )

                            # 2) 주소 (캡션 다음, 혼잡도 이전)
                            addr_for_ai = area.get("addr") or area.get("address") or ""
                            addr_display = to_road_address(addr_for_ai)
                            if not addr_display and area_name:
                                addr_display = f"서울 {area_name}"
                            if addr_display:
                                with st.container(border=True):
                                    st.write(addr_display)

                            # 3) 실시간 혼잡도
                            crowd_badge(crowd_now)

                            # 4) 인근 지하철역 박스 (OpenAI 우선)
                            stations = get_nearby_stations_openai(openai_key, area_name, addr_for_ai)
                            if not stations:
                                stations = NEARBY_STATIONS.get(area_name, [])
                            if stations:
                                with st.container(border=True):
                                    st.markdown("**인근 지하철역**")
                                    st.write(" / ".join(stations))

                            # 5) 명소 정보
                            with st.container(border=True):
                                desc = area.get("description") or ""
                                if desc:
                                    st.write(f"설명: {to_korean_display(desc)}")
                                homepage = area.get("homepage_url")
                                if homepage:
                                    st.link_button("홈페이지", homepage)

                            # 7) 비선호 옵션 (요약 카드 하단) - 외부 박스 제거
                            c1, c2 = st.columns([0.7, 0.3])
                            with c1:
                                st.write("")
                            with c2:
                                dislike_key = f"dislike_{signature}_{area_name}_card"
                                disliked_already = area_name in st.session_state.disliked.get(signature, set())
                                checked = st.checkbox("비선호", key=dislike_key, value=disliked_already)
                                if checked and not disliked_already:
                                    disliked_set = st.session_state.disliked.get(signature, set())
                                    disliked_set.add(area_name)
                                    st.session_state.disliked[signature] = disliked_set

        # 상세 화면
        if st.session_state.view == "detail" and st.session_state.selected_area_name:
            st.session_state.used_image_urls = set()
            area = next(
                (x for x in recos if (x.get("area") or "") == st.session_state.selected_area_name),
                None,
            )
            if area is None:
                st.session_state.view = "list"
                st.session_state.selected_area_name = None
                st.rerun()

            area_name = area.get("area") or ""
            if not area_name:
                st.session_state.view = "list"
                st.session_state.selected_area_name = None
                st.rerun()
            crowd_now = area.get("crowd_now", "약간 붐빔")

            st.markdown(f"## 상세 정보: {area_name}")
            if st.button("목록으로 돌아가기", type="primary"):
                st.session_state.view = "list"
                st.session_state.selected_area_name = None
                st.rerun()

            # 1) 대표 이미지 + 지도 (상단 나란히)
            top_cols = st.columns([0.55, 0.45], gap="large")
            with top_cols[0]:
                img_url, caption, credit, orientation = get_image_and_meta(
                    area_name,
                    indexed_regions,
                    regions_meta,
                    photo_korea_key,
                    st.session_state.used_image_urls,
                )
                if img_url:
                    st.session_state.used_image_urls.add(img_url)
                render_region_card(area_name, crowd_now, img_url, orientation, height_px=300)
                st.markdown(
                    f"<div style='text-align:center; font-size:12px; color:#666;'>"
                    f"{caption}<br>[{credit}]</div>",
                    unsafe_allow_html=True,
                )
            with top_cols[1]:
                center = area.get("center") or (37.5665, 126.9780)
                render_map(center, label=area_name, height_px=300)

            # 2) 상세 정보 (박스형)
            with st.container(border=True):
                st.markdown("#### 지역 상세")
                addr = area.get("addr") or area.get("address") or ""
                addr_display = to_road_address(addr)
                if not addr_display and area_name:
                    addr_display = f"서울 {area_name}"
                if addr_display:
                    st.write(addr_display)
                desc = area.get("description") or ""
                if desc:
                    st.write(f"설명: {to_korean_display(desc)}")
                homepage = area.get("homepage_url")
                if homepage:
                    st.link_button("홈페이지", homepage)

            addr_for_ai = area.get("addr") or area.get("address") or ""
            stations = get_nearby_stations_openai(openai_key, area_name, addr_for_ai)
            if not stations:
                stations = NEARBY_STATIONS.get(area_name, [])
            with st.container(border=True):
                st.markdown("#### 인근 지하철역")
                if stations:
                    st.write(" / ".join(stations))
                else:
                    st.write("인근 500m 지하철역: (데이터 준비 필요)")

            with st.container(border=True):
                st.markdown("#### 실시간 혼잡도")
                crowd_badge(crowd_now, title="실시간 혼잡도")
                # 과거 혼잡도는 실제 데이터가 없으므로 현재 혼잡도를 기반으로 표시 (추정)
                st.markdown("<div style='height:8px;'></div>", unsafe_allow_html=True)
                crowd_badge(crowd_now, title="1시간 전 혼잡도")
                crowd_badge(crowd_now, title="2시간 전 혼잡도")

            with st.container(border=True):
                st.markdown("#### 직선 거리")
                distance_lines = build_distance_lines(
                    st.session_state.people,
                    area,
                    st.session_state.master_pool,
                )
                if distance_lines:
                    for line in distance_lines:
                        st.write(f"- {line}")
                else:
                    st.write("출발 지역을 입력하면 거리를 표시합니다.")

            # 3) 추천 이유 / 코스 (OpenAI)
            extra_context = {
                "nearby": NEARBY_BEST.get(area_name, []),
                "stations": stations,
                "address": area.get("addr", ""),
            }
            gen = generate_reason_cached(
                openai_api_key=openai_key,
                area_name=area_name,
                crowd_label=crowd_now,
                main_taste=st.session_state.people[0].taste,
                main_purpose=st.session_state.people[0].purpose,
                extra_context=extra_context,
            )

            with st.container(border=True):
                st.markdown("#### 추천 이유")
                bullets = gen.get("bullets") or split_sentences_for_bullets(gen.get("one_liner", ""))
                if bullets:
                    render_bullet_list(bullets[:3])
                else:
                    st.write("추천 이유를 생성할 수 없습니다.")

            with st.container(border=True):
                st.markdown("#### 상세 코스 추천")
                course = gen.get("course", {}) or {}
                if course:
                    c1, c2 = st.columns(2, gap="medium")
                    with c1:
                        st.markdown("**전시·문화**")
                        render_bullet_list(course.get("culture", [])[:3])
                        st.markdown("**카페**")
                        render_bullet_list(course.get("cafe", [])[:3])
                    with c2:
                        st.markdown("**식당**")
                        render_bullet_list(course.get("food", [])[:3])
                        st.markdown("**공연·체험**")
                        render_bullet_list(course.get("activity", [])[:3])
                else:
                    st.write("코스 데이터가 없습니다.")

            # 지도 링크 (박스)
            with st.container(border=True):
                st.markdown("#### 지도 링크")
                naver = naver_map_link(area_name)
                kakao = kakao_map_link(area_name)
                google = google_map_link(area_name)
                # 간단 SVG 아이콘 (외부 리소스 로드 실패 대비)
                naver_icon = "data:image/svg+xml;utf8,<svg xmlns='http://www.w3.org/2000/svg' width='16' height='16'><rect width='16' height='16' rx='3' fill='%2303C75A'/><text x='8' y='12' font-size='10' text-anchor='middle' fill='white' font-family='Arial'>N</text></svg>"
                kakao_icon = "data:image/svg+xml;utf8,<svg xmlns='http://www.w3.org/2000/svg' width='16' height='16'><rect width='16' height='16' rx='3' fill='%23FEE500'/><text x='8' y='12' font-size='10' text-anchor='middle' fill='black' font-family='Arial'>K</text></svg>"
                google_icon = "data:image/svg+xml;utf8,<svg xmlns='http://www.w3.org/2000/svg' width='16' height='16'><rect width='16' height='16' rx='3' fill='%23FFFFFF' stroke='%23E0E0E0'/><text x='8' y='12' font-size='10' text-anchor='middle' fill='%23007AFF' font-family='Arial'>G</text></svg>"
                st.markdown(
                    f"""
                    <div style="display:flex; gap:8px; flex-wrap:wrap;">
                      <a href="{naver}" target="_blank" rel="noopener noreferrer"
                         style="flex:1; padding:10px 12px; border-radius:10px; border:1px solid #e0e0e0;
                                text-decoration:none; font-weight:600; color:#111; background:#fff;
                                display:flex; align-items:center; justify-content:center; gap:6px;">
                        <img src="{naver_icon}" style="width:16px; height:16px;" alt="naver icon">
                        네이버 지도
                      </a>
                      <a href="{kakao}" target="_blank" rel="noopener noreferrer"
                         style="flex:1; padding:10px 12px; border-radius:10px; border:1px solid #e0e0e0;
                                text-decoration:none; font-weight:600; color:#111; background:#fff;
                                display:flex; align-items:center; justify-content:center; gap:6px;">
                        <img src="{kakao_icon}" style="width:16px; height:16px;" alt="kakao icon">
                        카카오 지도
                      </a>
                      <a href="{google}" target="_blank" rel="noopener noreferrer"
                         style="flex:1; padding:10px 12px; border-radius:10px; border:1px solid #e0e0e0;
                                text-decoration:none; font-weight:600; color:#111; background:#fff;
                                display:flex; align-items:center; justify-content:center; gap:6px;">
                        <img src="{google_icon}" style="width:16px; height:16px;" alt="google icon">
                        구글 지도
                      </a>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )

            # 4) 함께 방문 추천
            with st.container(border=True):
                st.markdown("#### 함께 방문 추천")
                nearby = NEARBY_BEST.get(area_name, [])
                show6 = nearby[:6]
                if show6:
                    for n in show6:
                        st.write(f"- {n}")
                else:
                    st.write("인근 추천 데이터 준비 필요")

            st.markdown("---")
            # 5) 비선호 옵션 (상세 화면) - 맨 하단 배치
            with st.container(border=True):
                st.markdown("#### 비선호")
                st.caption("선택하면 해당 카드만 교체 추천되며, 같은 조건에서는 다시 추천되지 않습니다.")
                dislike_key = f"dislike_{signature}_{area_name}"
                disliked_already = area_name in st.session_state.disliked.get(signature, set())
                checked = st.checkbox("비선호로 표시", key=dislike_key, value=disliked_already)
                if checked and not disliked_already:
                    disliked_set = st.session_state.disliked.get(signature, set())
                    disliked_set.add(area_name)
                    st.session_state.disliked[signature] = disliked_set


if __name__ == "__main__":
    main()
