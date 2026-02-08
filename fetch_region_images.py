#!/usr/bin/env python3
import argparse
import json
import os
import sys
import time
import random
from dataclasses import dataclass
from datetime import datetime
from io import BytesIO
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import xml.etree.ElementTree as ET

import requests
from PIL import Image


DEFAULT_ENDPOINT = "https://apis.data.go.kr/B551011/PhotoGalleryService1/gallerySearchList1"


IMAGE_URL_KEYS = [
    "imageurl",
    "originimgurl",
    "galwebimageurl",
    "galwebimageurl2",
    "galwebimageurl1",
    "imageUrl",
    "originImgUrl",
    "galWebImageUrl",
    "galWebImageUrl2",
    "galWebImageUrl1",
]

TITLE_KEYS = [
    "title",
    "galtitle",
    "galTitle",
    "imgtitle",
    "imageTitle",
]

LOCATION_KEYS = [
    "location",
    "loc",
    "galphotographylocation",
    "galPhotographyLocation",
]

PHOTOGRAPHER_KEYS = [
    "photographer",
    "galphotographer",
    "galPhotographer",
    "shootingperson",
    "shootingPerson",
]

WIDTH_KEYS = ["width", "imgwidth", "imagewidth", "galwebimagewidth", "galWebImageWidth"]
HEIGHT_KEYS = ["height", "imgheight", "imageheight", "galwebimageheight", "galWebImageHeight"]

DATE_KEYS = ["createdtime", "modifiedtime", "regdate", "regDate", "galCreatedtime", "galModifiedtime"]


@dataclass
class PhotoItem:
    url: str
    title: str
    location: str
    photographer: str
    width: int
    height: int
    date_score: int


def log(msg: str):
    print(msg, flush=True)


def load_regions(path: Path) -> List[Dict[str, Any]]:
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, list):
        raise ValueError("regions.json은 리스트 형식이어야 합니다.")
    return data


def get_first_value(d: Dict[str, Any], keys: List[str]) -> str:
    for k in keys:
        if k in d and d[k]:
            return str(d[k]).strip()
        lk = k.lower()
        for key in d.keys():
            if key.lower() == lk and d[key]:
                return str(d[key]).strip()
    return ""


def parse_int(value: str) -> int:
    try:
        return int(str(value).strip())
    except Exception:
        return 0


def parse_date_score(value: str) -> int:
    if not value:
        return 0
    digits = "".join([c for c in str(value) if c.isdigit()])
    if len(digits) >= 8:
        try:
            return int(digits[:14])
        except Exception:
            return 0
    return 0


def walk_for_items(obj: Any) -> Optional[List[Dict[str, Any]]]:
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k.lower() in ("item", "items"):
                if isinstance(v, list):
                    return [x for x in v if isinstance(x, dict)]
                if isinstance(v, dict) and "item" in v and isinstance(v["item"], list):
                    return [x for x in v["item"] if isinstance(x, dict)]
            found = walk_for_items(v)
            if found:
                return found
    if isinstance(obj, list):
        for v in obj:
            found = walk_for_items(v)
            if found:
                return found
    return None


def parse_json_items(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    items = walk_for_items(data)
    return items or []


def parse_xml_items(xml_text: str) -> List[Dict[str, Any]]:
    try:
        root = ET.fromstring(xml_text)
    except Exception:
        return []
    items = []
    for elem in root.iter():
        if elem.tag.lower().endswith("item"):
            item = {}
            for child in list(elem):
                item[child.tag] = child.text or ""
            if item:
                items.append(item)
    return items


def request_with_retry(url: str, params: Dict[str, Any], retries: int = 3, timeout: int = 15) -> Optional[str]:
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(url, params=params, timeout=timeout)
            resp.raise_for_status()
            return resp.text
        except Exception as e:
            if attempt == retries:
                log(f"  - 요청 실패: {e}")
                return None
            sleep_s = (2 ** (attempt - 1)) + random.random()
            time.sleep(sleep_s)
    return None


def build_photo_items(items: List[Dict[str, Any]]) -> List[PhotoItem]:
    photos: List[PhotoItem] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        url = get_first_value(item, IMAGE_URL_KEYS)
        if not url:
            continue
        title = get_first_value(item, TITLE_KEYS)
        location = get_first_value(item, LOCATION_KEYS)
        photographer = get_first_value(item, PHOTOGRAPHER_KEYS)
        width = parse_int(get_first_value(item, WIDTH_KEYS))
        height = parse_int(get_first_value(item, HEIGHT_KEYS))
        date_score = parse_date_score(get_first_value(item, DATE_KEYS))
        photos.append(
            PhotoItem(
                url=url,
                title=title,
                location=location,
                photographer=photographer,
                width=width,
                height=height,
                date_score=date_score,
            )
        )
    return photos


def pick_best_photo(photos: List[PhotoItem]) -> Optional[PhotoItem]:
    if not photos:
        return None

    def seoul_score(p: PhotoItem) -> int:
        text = " ".join([p.title, p.location]).strip()
        return 1 if "서울" in text else 0

    def resolution(p: PhotoItem) -> int:
        if p.width and p.height:
            return p.width * p.height
        return 0

    photos.sort(
        key=lambda p: (
            seoul_score(p),
            resolution(p),
            p.date_score,
        ),
        reverse=True,
    )
    return photos[0]


def download_image(url: str, retries: int = 3, timeout: int = 20) -> Optional[bytes]:
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(url, timeout=timeout)
            resp.raise_for_status()
            return resp.content
        except Exception as e:
            if attempt == retries:
                log(f"  - 다운로드 실패: {e}")
                return None
            time.sleep((2 ** (attempt - 1)) + random.random())
    return None


def resize_long_edge(img: Image.Image, max_edge: int) -> Image.Image:
    w, h = img.size
    if max(w, h) <= max_edge:
        return img
    if w >= h:
        new_w = max_edge
        new_h = int(h * (max_edge / w))
    else:
        new_h = max_edge
        new_w = int(w * (max_edge / h))
    return img.resize((new_w, new_h), Image.LANCZOS)


def save_webp(img: Image.Image, path: Path, quality: int = 80):
    path.parent.mkdir(parents=True, exist_ok=True)
    img.save(path, format="WEBP", quality=quality, method=6)


def build_caption(title: str, location: str, fallback: str) -> str:
    for v in [title, location]:
        if v:
            return v.strip()
    return fallback


def fetch_for_region(
    region: Dict[str, Any],
    api_key: str,
    endpoint: str,
    retries: int,
) -> Optional[PhotoItem]:
    keywords = region.get("keywords", [])
    if not isinstance(keywords, list):
        keywords = []
    name_ko = str(region.get("name_ko", "")).strip()

    search_terms: List[str] = []
    if name_ko:
        search_terms.append(name_ko)
        if "서울" not in name_ko:
            search_terms.append(f"서울 {name_ko}")
    for kw in keywords:
        kw = str(kw).strip()
        if kw:
            search_terms.append(kw)
            if "서울" not in kw:
                search_terms.append(f"서울 {kw}")

    seen = set()
    ordered_terms = []
    for t in search_terms:
        if t not in seen:
            seen.add(t)
            ordered_terms.append(t)

    for term in ordered_terms:
        # TODO: 포토코리아 OpenAPI 문서에 맞춰 엔드포인트/파라미터를 조정하세요.
        params = {
            "serviceKey": api_key,
            "numOfRows": 10,
            "pageNo": 1,
            "MobileOS": "ETC",
            "MobileApp": "SeoulNomads",
            "_type": "json",
            "keyword": term,
        }

        raw = request_with_retry(endpoint, params=params, retries=retries)
        if not raw:
            continue

        items: List[Dict[str, Any]] = []
        try:
            data = json.loads(raw)
            items = parse_json_items(data)
        except Exception:
            items = parse_xml_items(raw)

        photos = build_photo_items(items)
        best = pick_best_photo(photos)
        if best:
            return best
    return None


def load_meta(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def save_meta(path: Path, meta: Dict[str, Any]):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)


def main():
    parser = argparse.ArgumentParser(description="포토코리아 API로 지역 대표 이미지를 다운로드합니다.")
    parser.add_argument("--regions", required=True, help="regions.json 경로")
    parser.add_argument("--out", required=True, help="출력 폴더 (예: assets/regions)")
    parser.add_argument("--force", action="store_true", help="기존 파일이 있어도 강제 갱신")
    parser.add_argument("--endpoint", default=DEFAULT_ENDPOINT, help="포토코리아 API 엔드포인트")
    parser.add_argument("--retries", type=int, default=3, help="요청 재시도 횟수")
    args = parser.parse_args()

    api_key = os.getenv("PHOTO_KOREA_API_KEY", "").strip()
    if not api_key:
        log("PHOTO_KOREA_API_KEY 환경변수가 필요합니다.")
        sys.exit(1)

    regions_path = Path(args.regions)
    if not regions_path.exists():
        log(f"regions.json을 찾을 수 없습니다: {regions_path}")
        sys.exit(1)

    out_dir = Path(args.out)
    thumbs_dir = out_dir / "thumbs"
    meta_path = out_dir / "regions_meta.json"

    regions = load_regions(regions_path)
    meta = load_meta(meta_path)

    total = len(regions)
    log(f"총 {total}개 지역 처리 시작")

    for idx, region in enumerate(regions, start=1):
        region_id = str(region.get("id", "")).strip()
        name_ko = str(region.get("name_ko", "")).strip()
        if not region_id:
            log(f"[{idx}/{total}] id 누락: {name_ko} - 건너뜀")
            continue

        out_path = out_dir / f"{region_id}.webp"
        thumb_path = thumbs_dir / f"{region_id}.webp"

        if out_path.exists() and not args.force:
            log(f"[{idx}/{total}] {region_id} - 기존 파일 유지")
            continue

        log(f"[{idx}/{total}] {region_id} - 검색/다운로드")
        best = fetch_for_region(region, api_key, args.endpoint, args.retries)
        if not best:
            log(f"  - 결과 없음: {name_ko}")
            continue

        img_bytes = download_image(best.url, retries=args.retries)
        if not img_bytes:
            log("  - 이미지 다운로드 실패")
            continue

        try:
            img = Image.open(BytesIO(img_bytes)).convert("RGB")
        except Exception as e:
            log(f"  - 이미지 파싱 실패: {e}")
            continue

        img_main = resize_long_edge(img, 1600)
        save_webp(img_main, out_path, quality=80)

        img_thumb = resize_long_edge(img, 600)
        save_webp(img_thumb, thumb_path, quality=80)

        credit = "ⓒ한국관광공사 사진갤러리"
        if best.photographer:
            credit = f"ⓒ한국관광공사 사진갤러리-{best.photographer}"

        meta[region_id] = {
            "source": "KTO Photo Korea OpenAPI",
            "credit": credit,
            "origin_url": best.url,
            "picked_at": datetime.utcnow().isoformat() + "Z",
            "title": best.title,
            "location": best.location,
            "caption": build_caption(best.title, best.location, f"{name_ko} 풍경"),
        }

        save_meta(meta_path, meta)
        log("  - 저장 완료")

    log("완료")


if __name__ == "__main__":
    main()
