import os
import re
import json
import difflib
import zipfile
import requests

BASE_URL = "https://indexsubtitle.cc"
SEARCH_URL = f"{BASE_URL}/search"
SUBTITLES_INFO_URL = f"{BASE_URL}/subtitlesInfo"
DOWNLOAD_TTL = 1771853823
SUBTITLE_DOWNLOADS_DIR = "subtitle_downloads"
SUBTITLE_EXTENSIONS = (".srt", ".vtt", ".ass", ".ssa")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; rv:109.0) Gecko/20100101 Firefox/115.0",
    "X-Requested-With": "XMLHttpRequest",
    "Referer": BASE_URL,
    "Authorization": BASE_URL,
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


def _get_download_token(sub_entry: dict) -> str | None:
    """POST /subtitlesInfo to get the download token for a subtitle entry."""
    url_path = sub_entry.get("url", "")
    sub_id = url_path.rstrip("/").split("/")[-1]
    lang = sub_entry.get("language", "")
    resp = requests.post(
        SUBTITLES_INFO_URL,
        data={"id": sub_id, "lang": lang, "url": url_path},
        headers=HEADERS,
        timeout=15,
    )
    resp.raise_for_status()
    data = resp.json()
    return data.get("token")


def _build_download_url(sub_entry: dict, token: str) -> str:
    """Build the /d/{id}/{ttl}/{token}/{zipname}.zip URL."""
    url_path = sub_entry.get("url", "")
    sub_id = url_path.rstrip("/").split("/")[-1]
    zp = re.sub(r"[^\w ]", "", url_path).replace("/", "_")
    zp = f"[indexsubtitle.cc]_{zp}"
    return f"{BASE_URL}/d/{sub_id}/{DOWNLOAD_TTL}/{token}/{zp}.zip"


def download_subtitle(sub_entry: dict, dest_dir: str = SUBTITLE_DOWNLOADS_DIR) -> str | None:
    """
    Fetch token, download the zip, extract subtitle files.
    Returns path to the extracted .srt (or first subtitle file), or None on failure.
    """
    os.makedirs(dest_dir, exist_ok=True)
    token = _get_download_token(sub_entry)
    if not token:
        print("[subtitle2] Failed to get download token")
        return None

    dl_url = _build_download_url(sub_entry, token)
    resp = requests.get(dl_url, headers=HEADERS, timeout=30)
    resp.raise_for_status()

    zip_path = os.path.join(dest_dir, "subtitle_download.zip")
    with open(zip_path, "wb") as f:
        f.write(resp.content)

    if not zipfile.is_zipfile(zip_path):
        # Might be a raw srt
        ext = ".srt"
        for e in SUBTITLE_EXTENSIONS:
            if resp.headers.get("content-type", "").lower().find(e.lstrip(".")) >= 0:
                ext = e
                break
        raw_path = os.path.join(dest_dir, f"subtitle{ext}")
        os.rename(zip_path, raw_path)
        return raw_path

    # Extract subtitle files from zip
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
    print(f"Searching: {query}")

    # 1) Search
    resp = requests.post(SEARCH_URL, data={"query": query}, headers=HEADERS, timeout=15)
    results = resp.json()
    match = _pick_best(results, query)
    if not match:
        print("No match found")
        exit()
    print(f"Best match: {match['title']} -> {match['url']}")

    # 2) Detail page
    detail_url = BASE_URL + match["url"]
    detail = requests.get(detail_url, headers=HEADERS, timeout=15)
    subs = _extract_datatable_json(detail.text)
    print(f"Total subtitles: {len(subs)}")

    # 3) Available languages
    langs = get_available_languages(subs)
    print(f"\nAvailable languages ({len(langs)}):")
    for l in langs:
        print(f"  {l['short']:4s}  {l['long']:30s}  ({l['count']})")

    # 4) Filter by language and pick first
    filtered = [s for s in subs if s.get("language", "").lower() == lang]
    print(f"\n{lang.title()} subtitles: {len(filtered)}")
    if not filtered:
        print("No subtitles for this language")
        exit()
    pick = filtered[0]
    print(f"Picking: {pick['title']}  |  {pick['author']['name']}  |  {pick['url']}")

    # 5) Get token and download
    print("\nFetching token...")
    token = _get_download_token(pick)
    print(f"Token: {token}")
    if not token:
        print("Failed to get token")
        exit()

    print("Downloading...")
    path = download_subtitle(pick)
    if path:
        print(f"Downloaded to: {path}")
    else:
        print("Download failed")
