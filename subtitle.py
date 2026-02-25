import os
import re
import json
import time
import difflib
import zipfile
import requests

BASE_URL = "https://indexsubtitle.cc"
SEARCH_URL = f"{BASE_URL}/search"
SUBTITLES_INFO_URL = f"{BASE_URL}/subtitlesInfo"
SUBTITLE_DOWNLOADS_DIR = "subtitle_downloads"
SUBTITLE_EXTENSIONS = (".srt", ".vtt", ".ass", ".ssa")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; rv:109.0) Gecko/20100101 Firefox/115.0",
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Accept-Language": "en-US,en;q=0.9",
    "X-Requested-With": "XMLHttpRequest",
    "Referer": f"{BASE_URL}/",
    "Origin": BASE_URL,
}


def _parse_title_year(text):
    m = re.match(r"^(.+?)\s*\((\d{4})\)\s*$", text)
    if m:
        return m.group(1).strip(), m.group(2)
    return text.strip(), None


def _split_query(query):
    parts = query.strip().rsplit(" ", 1)
    if len(parts) == 2 and parts[1].isdigit() and len(parts[1]) == 4:
        return parts[0].strip(), parts[1]
    return query.strip(), None


def _strip_parens(text):
    return re.sub(r"\s*\(.*?\)", "", text).strip()


def _pick_best(results, query):
    q_title, q_year = _split_query(query)
    q_lower = q_title.lower()
    best, best_score = None, -1
    for r in results:
        r_title, r_year = _parse_title_year(r.get("title", ""))
        r_lower = r_title.lower()
        r_stripped = _strip_parens(r_title).lower()
        sim_full = difflib.SequenceMatcher(None, q_lower, r_lower).ratio()
        sim_stripped = difflib.SequenceMatcher(None, q_lower, r_stripped).ratio()
        sim = max(sim_full, sim_stripped)
        if q_lower in r_lower or r_stripped == q_lower:
            sim = max(sim, 0.95)
        score = sim + (1.0 if q_year and r_year == q_year else 0.0)
        if score > best_score:
            best_score, best = score, r
    return best


def _extract_datatable_json(html):
    """Pull the JSON array from DataTable({ data: [...] })."""
    m = re.search(r"DataTable\(\{\s*data:\s*(\[.*?\])\s*,", html, re.DOTALL)
    if not m:
        return []
    try:
        return json.loads(m.group(1))
    except json.JSONDecodeError:
        return []


# Long language name -> ISO 639-1 short code
LANG_TO_SHORT = {
    "arabic": "ar", "bengali": "bn", "bosnian": "bs", "brazilian_portuguese": "pt",
    "big_5_code": "zh", "chinese_bg_code": "zh", "croatian": "hr", "czech": "cs",
    "danish": "da", "dutch": "nl", "english": "en", "espranto": "eo", "estonian": "et",
    "farsi_persian": "fa", "finnish": "fi", "french": "fr", "german": "de", "greek": "el",
    "hebrew": "he", "hindi": "hi", "hungarian": "hu", "indonesian": "id", "italian": "it",
    "japanese": "ja", "korean": "ko", "latvian": "lv", "lithuanian": "lt", "malay": "ms",
    "malayalam": "ml", "norwegian": "no", "polish": "pl", "portuguese": "pt",
    "romanian": "ro", "russian": "ru", "serbian": "sr", "sinhala": "si", "slovak": "sk",
    "slovenian": "sl", "spanish": "es", "spanish_latin_america": "es", "swedish": "sv",
    "tamil": "ta", "telugu": "te", "thai": "th", "turkish": "tr", "ukrainian": "uk",
    "urdu": "ur", "vietnamese": "vi",
}
SHORT_TO_LANG = {}
for _long, _short in LANG_TO_SHORT.items():
    SHORT_TO_LANG.setdefault(_short, _long)


def get_available_languages(subs: list[dict]) -> list[dict]:
    """
    From the subtitle list, return unique languages by short (ISO 639-1) with long form and total count.
    Merges variants that share the same short code (e.g. big_5_code + chinese_bg_code -> one zh entry).
    Returns: [{ "short": "en", "long": "english", "count": 52 }, ...]
    """
    counts: dict[str, int] = {}
    for s in subs:
        lang = (s.get("language") or "").strip().lower()
        if lang:
            counts[lang] = counts.get(lang, 0) + 1
    # Merge by short code so e.g. zh appears once (big_5_code + chinese_bg_code)
    merged: dict[str, tuple[int, str]] = {}  # short -> (total_count, long_name for display)
    for long_name, count in sorted(counts.items(), key=lambda x: -x[1]):
        short = LANG_TO_SHORT.get(long_name, long_name[:2])
        if short in merged:
            merged[short] = (merged[short][0] + count, merged[short][1])
        else:
            merged[short] = (count, long_name)
    result = [
        {"short": short, "long": long_name, "count": total}
        for short, (total, long_name) in sorted(merged.items(), key=lambda x: -x[1][0])
    ]
    return result


def short_to_long(code: str) -> str:
    """Convert ISO 639-1 short code to the long language name used by the site."""
    return SHORT_TO_LANG.get(code.strip().lower(), code.strip().lower())


def _subtitles_page_url(sub_entry: dict) -> str:
    """Build the subtitles page URL (e.g. for Referer and for fetching cookies)."""
    url_path = (sub_entry.get("url") or "").strip().rstrip("/")
    if not url_path:
        return f"{BASE_URL}/subtitles"
    slug = url_path.split("/")[0]
    return f"{BASE_URL}/subtitles/{slug}"


def _dump_response(resp: requests.Response, label: str) -> None:
    """Optional debug: log full response when JSON parse fails. No-op by default."""
    pass


def _get_download_token(sub_entry: dict, session: requests.Session, referer: str) -> str | None:
    """POST /subtitlesInfo to get the download token; uses session cookies and Referer like browser."""
    url_path = sub_entry.get("url", "")
    sub_id = url_path.rstrip("/").split("/")[-1]
    lang = sub_entry.get("language", "")
    headers = {**HEADERS, "Referer": referer, "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8"}
    resp = session.post(
        SUBTITLES_INFO_URL,
        data={"id": sub_id, "lang": lang, "url": url_path},
        headers=headers,
        timeout=15,
    )
    resp.raise_for_status()
    text = (resp.text or "").strip()
    if not text or not (text.startswith("{") or text.startswith("[")):
        _dump_response(resp, "/subtitlesInfo non-JSON")
        return None
    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        _dump_response(resp, "/subtitlesInfo JSON error")
        return None
    return data.get("token") if isinstance(data, dict) else None


def _build_download_url(sub_entry: dict, token: str) -> str:
    """Build the /d/{id}/{ttl}/{token}/{zipname}.zip URL. TTL is current time so link is valid."""
    url_path = sub_entry.get("url", "")
    sub_id = url_path.rstrip("/").split("/")[-1]
    zp = re.sub(r"[^\w ]", "", url_path).replace("/", "_")
    zp = f"[indexsubtitle.cc]_{zp}"
    ttl = int(time.time())
    return f"{BASE_URL}/d/{sub_id}/{ttl}/{token}/{zp}.zip"


def download_subtitle(
    sub_entry: dict,
    dest_dir: str = SUBTITLE_DOWNLOADS_DIR,
    *,
    session: requests.Session | None = None,
    referer: str | None = None,
) -> str | None:
    """
    Fetch token (with session + cookies like browser), download the zip, extract subtitle files.
    If session/referer are provided (e.g. caller already opened the subtitles page), they are reused.
    Returns path to the extracted .srt (or first subtitle file), or None on failure.
    """
    os.makedirs(dest_dir, exist_ok=True)
    referer_url = referer or _subtitles_page_url(sub_entry)
    if session is None:
        session = requests.Session()
        session.headers.update(HEADERS)
        session.get(referer_url, timeout=15)
    token = _get_download_token(sub_entry, session, referer_url)
    if not token:
        return None
    dl_url = _build_download_url(sub_entry, token)
    resp = session.get(dl_url, timeout=30)
    resp.raise_for_status()

    # Download result is a zip; save then extract subtitle files from it
    zip_path = os.path.join(dest_dir, "subtitle_download.zip")
    with open(zip_path, "wb") as f:
        f.write(resp.content)

    if not zipfile.is_zipfile(zip_path):
        # Not a zip (e.g. error page or raw srt)
        ext = ".srt"
        for e in SUBTITLE_EXTENSIONS:
            if resp.headers.get("content-type", "").lower().find(e.lstrip(".")) >= 0:
                ext = e
                break
        raw_path = os.path.join(dest_dir, f"subtitle{ext}")
        os.rename(zip_path, raw_path)
        return raw_path

    # Extract subtitle files from zip before using
    extracted = []
    with zipfile.ZipFile(zip_path, "r") as zf:
        for name in zf.namelist():
            lower = name.lower()
            if any(lower.endswith(e) for e in SUBTITLE_EXTENSIONS):
                zf.extract(name, dest_dir)
                extracted.append(os.path.join(dest_dir, name))
    os.remove(zip_path)

    if not extracted:
        return None
    # Prefer .srt
    for p in extracted:
        if p.lower().endswith(".srt"):
            return p
    return extracted[0]


if __name__ == "__main__":
    query = "Spirited Away 2001"
    lang = "english"

    # 1) Search
    resp = requests.post(SEARCH_URL, data={"query": query}, headers=HEADERS, timeout=15)
    resp.raise_for_status()
    try:
        results = resp.json()
    except json.JSONDecodeError:
        _dump_response(resp, "Search response")
        exit(1)
    match = _pick_best(results, query)
    if not match:
        exit(1)

    # 2) Detail page (use session so cookies are reused for token + download)
    detail_url = BASE_URL + match["url"]
    session = requests.Session()
    session.headers.update(HEADERS)
    detail = session.get(detail_url, timeout=15)
    subs = _extract_datatable_json(detail.text)

    # 3) Available languages
    langs = get_available_languages(subs)

    # 4) Filter by language and pick first
    filtered = [s for s in subs if s.get("language", "").lower() == lang]
    if not filtered:
        exit(1)
    pick = filtered[0]

    # 5) Get token (same session + Referer) and download
    path = download_subtitle(pick, session=session, referer=detail_url)
    if not path:
        exit(1)
