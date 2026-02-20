"""
Sniffer: bot-resistant GET agent and helpers for fetching embed/stream pages.
Uses browser-like headers and a persistent session to reduce bot detection.
"""

import json
import os
import shutil
import subprocess
import sys
import threading
import time

import requests
from urllib.parse import urlparse
import re

from bs4 import BeautifulSoup

from process_state import set_state

# -----------------------------------------------------------------------------
# Browser-like headers (Chrome on Windows) — resist basic bot checks
# -----------------------------------------------------------------------------

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)

# Use "gzip, deflate" only — "br" (Brotli) is not decoded by requests by default, so you get binary in r.text
DEFAULT_HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate",
    "Sec-Ch-Ua": '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": '"Windows"',
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Upgrade-Insecure-Requests": "1",
    "Connection": "keep-alive",
    "Cache-Control": "max-age=0",
    "DNT": "1",
    "Priority": "u=0, i",
}


def _origin_for_url(url: str) -> str:
    parsed = urlparse(url)
    return f"{parsed.scheme}://{parsed.netloc}"


def create_agent(**kwargs) -> requests.Session:
    """
    Create a requests.Session that sends browser-like headers by default.
    Use this for multiple requests to the same site (cookies + connection reuse).
    """
    session = requests.Session()
    session.headers.update(DEFAULT_HEADERS)
    for k, v in kwargs.items():
        if v is not None:
            session.headers[k] = v
    return session


def get(
    url: str,
    *,
    referer: str | None = None,
    session: requests.Session | None = None,
    timeout: float = 30,
    **kwargs,
) -> requests.Response:
    """
    GET `url` with bot-resistant headers. Uses a one-off request or your session.

    - referer: if None, set to the origin of `url` so the request looks same-origin.
    - session: if given, use it (and its cookies); otherwise use a one-off request with default headers.
    - timeout, **kwargs: passed through to requests.get().
    """
    headers = kwargs.pop("headers", None) or {}
    if referer is None:
        referer = _origin_for_url(url) + "/"
    if referer:
        headers["Referer"] = referer

    if session is not None:
        return session.get(url, headers=headers, timeout=timeout, **kwargs)

    h = {**DEFAULT_HEADERS, **headers}
    return requests.get(url, headers=h, timeout=timeout, **kwargs)


def parse_html(html: str, parser: str = "html.parser") -> BeautifulSoup:
    """Parse HTML string into a BeautifulSoup tree. Query with .find(), .find_all(), .select()."""
    return BeautifulSoup(html, parser)


# -----------------------------------------------------------------------------
# Cloudnestra: build prorcp link + referer (same as play.html iframe)
# -----------------------------------------------------------------------------

CLOUDNESTRA_BASE = "https://cloudnestra.com"


def build_prorcp_url(token: str, base: str = CLOUDNESTRA_BASE) -> str:
    """Build full URL for iframe src: base + /prorcp/ + token (from play.html loadIframe)."""
    token = (token or "").strip().lstrip("/")
    if token.startswith("prorcp/"):
        token = token[7:]
    return f"{base.rstrip('/')}/prorcp/{token}"


def build_rcp_referer(token: str, base: str = CLOUDNESTRA_BASE) -> str:
    """Build Referer for prorcp request: the parent /rcp/ page (same as address bar when you click play)."""
    token = (token or "").strip().lstrip("/")
    if token.startswith("rcp/"):
        token = token[4:]
    return f"{base.rstrip('/')}/rcp/{token}"


def get_prorcp(
    prorcp_token: str,
    rcp_token: str,
    *,
    base: str = CLOUDNESTRA_BASE,
    session: requests.Session | None = None,
    timeout: float = 30,
    **kwargs,
) -> requests.Response:
    """
    GET the player iframe content (prorcp). Uses correct Referer so server returns 200.

    - prorcp_token: the token from iframe src in play.html (path after /prorcp/).
    - rcp_token: the token of the parent /rcp/ page (the page with the play button).
    """
    prorcp_url = build_prorcp_url(prorcp_token, base)
    referer = build_rcp_referer(rcp_token, base)
    return get(prorcp_url, referer=referer, session=session, timeout=timeout, **kwargs)


# Patterns to extract prorcp token from rcp page HTML (shared by normal GET and browser fallback)
_PRORCP_PATTERNS = (
    r"src:\s*['\"]\/prorcp\/([^'\"]+)['\"]",
    r"src\s*:\s*['\"]\/prorcp\/([^'\"]+)['\"]",
    r"['\"]\/prorcp\/([^'\"]+)['\"]",
    r"\/prorcp\/([^\s'\"<>]{20,})",
)


def _extract_prorcp_token(html: str) -> str | None:
    """Try all patterns; return first non-empty match or None."""
    for pattern in _PRORCP_PATTERNS:
        m = re.search(pattern, html)
        if m:
            token = m.group(1).strip()
            if token:
                return token
    return None


def _fetch_rcp_page_with_browser(rcp_url: str, log_fn=None) -> tuple[str | None, list]:
    """
    Load the rcp page in a real browser (Puppeteer) so Cloudflare Turnstile can run.
    Uses downloader/fetch_rcp_browser.js (Node + Puppeteer, headed). Returns (html_content, cookie_list).
    On failure returns (None, []). Run: cd downloader && npm install && node fetch_rcp_browser.js <url>
    """
    log = log_fn if callable(log_fn) else (lambda _: None)
    script_dir = os.path.dirname(os.path.abspath(__file__))
    script_path = os.path.join(script_dir, "fetch_rcp_browser.js")
    if not os.path.isfile(script_path):
        log("      fetch_rcp_browser.js not found. Expect it in downloader/.")
        return (None, [])

    set_state(explanation="[3/8] Solving Cloudflare Turnstile (real browser)…")
    log("      Launching real browser (Puppeteer) to scrape rcp page…")
    try:
        proc = subprocess.run(
            ["node", script_path, rcp_url],
            cwd=script_dir,
            capture_output=True,
            text=True,
            timeout=90,
        )
    except FileNotFoundError:
        log("      Node.js not found. Install Node and run: cd downloader && npm install")
        return (None, [])
    except subprocess.TimeoutExpired:
        log("      Browser script timed out (90s).")
        return (None, [])

    out = (proc.stdout or "").strip()
    if not out:
        err = (proc.stderr or "").strip() or "no output"
        log("      Browser script failed: %s" % err)
        return (None, [])

    try:
        data = json.loads(out)
    except Exception as e:
        log("      Invalid JSON from browser script: %s" % e)
        return (None, [])

    if data.get("error"):
        log("      Browser: %s" % data["error"])
        return (None, [])

    html = data.get("html") or ""
    cookies = data.get("cookies") or []
    if not html or not _extract_prorcp_token(html):
        log("      Browser returned content but no prorcp token.")
        return (None, [])

    log("      Browser: got content and %s cookie(s)." % len(cookies))
    return (html, cookies)


# Example: from play.html you have iframe src="/prorcp/TOKEN1" and parent page is /rcp/TOKEN2
#   link = build_prorcp_url("TOKEN1")           # https://cloudnestra.com/prorcp/TOKEN1
#   referer = build_rcp_referer("TOKEN2")       # https://cloudnestra.com/rcp/TOKEN2
#   r = get_prorcp("TOKEN1", "TOKEN2")          # GET link with Referer: referer


# -----------------------------------------------------------------------------
# Example usage
# -----------------------------------------------------------------------------

# URL = "https://vidsrcme.ru/embed/movie?tmdb=129"
URL = "https://vidsrcme.ru/embed/movie?tmdb=326359"

# All downloads go here (same as main.py DOWNLOAD_DIR when set). Created if missing.
_script_dir = os.path.dirname(os.path.abspath(__file__))
DOWNLOADS_DIR = os.path.abspath(os.path.join(_script_dir, "downloads"))

# Preferred quality when master m3u8 has multiple variants: "low" | "med" | "high"
PREFERRED_QUALITY = os.environ.get("M3U8_QUALITY", "high").strip().lower()
if PREFERRED_QUALITY not in ("low", "med", "high"):
    PREFERRED_QUALITY = "low"

# Direct path to ffmpeg binary, or leave empty to use PATH. Example: r"C:\ffmpeg\bin\ffmpeg.exe"
FFMPEG_PATH = r"C:\Users\adria\Downloads\ffmpeg-master-latest-win64-gpl-shared\ffmpeg-master-latest-win64-gpl-shared\bin\ffmpeg.exe"


def _parse_master_m3u8(text: str) -> list[dict]:
    """Parse master m3u8; return list of {bandwidth, resolution, height, path, tier}."""
    lines = text.strip().splitlines()
    variants = []
    i = 0
    while i < len(lines):
        line = lines[i].strip()
        if line.startswith("#EXT-X-STREAM-INF"):
            bandwidth = 0
            resolution = ""
            # BANDWIDTH=123456
            mb = re.search(r"BANDWIDTH=(\d+)", line, re.I)
            if mb:
                bandwidth = int(mb.group(1))
            # RESOLUTION=1920x1024
            mr = re.search(r"RESOLUTION=(\d+x\d+)", line, re.I)
            if mr:
                resolution = mr.group(1)
            w, h = 0, 0
            if resolution:
                parts = resolution.split("x")
                if len(parts) == 2:
                    try:
                        w, h = int(parts[0]), int(parts[1])
                    except ValueError:
                        pass
            i += 1
            path = (lines[i].strip() if i < len(lines) else "").lstrip("/")
            if path and not path.startswith("#"):
                # Classify by height: low <= 480, med <= 720, high > 720
                if h <= 480:
                    tier = "low"
                elif h <= 720:
                    tier = "med"
                else:
                    tier = "high"
                variants.append({"bandwidth": bandwidth, "resolution": resolution, "height": h, "path": "/" + path, "tier": tier})
            i += 1
        else:
            i += 1
    return variants


def _pick_variant(variants: list[dict], preferred: str) -> dict | None:
    """Pick one variant by preferred tier (low/med/high). If multiple in tier, pick best in that tier."""
    if not variants:
        return None
    if len(variants) == 1:
        return variants[0]
    in_tier = [v for v in variants if v["tier"] == preferred]
    if not in_tier:
        # Fallback: high -> highest, low -> lowest, med -> middle by height
        if preferred == "high":
            return max(variants, key=lambda v: (v["height"], v["bandwidth"]))
        if preferred == "low":
            return min(variants, key=lambda v: (v["height"], -v["bandwidth"]))
        # med: pick middle by height
        by_height = sorted(variants, key=lambda v: v["height"])
        return by_height[len(by_height) // 2]
    if preferred == "high":
        return max(in_tier, key=lambda v: (v["height"], v["bandwidth"]))
    if preferred == "low":
        return min(in_tier, key=lambda v: (v["height"], -v["bandwidth"]))
    # med
    return max(in_tier, key=lambda v: v["bandwidth"])


# Regex to find page number in segment URLs (e.g. .../page-1235.html)
_PAGE_RE = re.compile(r"page-(\d+)\.html", re.I)


def _get_total_pages_from_m3u8(m3u8_url: str) -> int | None:
    """Fetch m3u8 (and follow variant if master), parse segment URLs for page-XXX.html; return largest page number or None."""
    try:
        r = get(m3u8_url, timeout=15)
        r.raise_for_status()
        text = r.text
        # If master playlist, follow first variant
        if "#EXT-X-STREAM-INF" in text:
            for line in text.splitlines():
                line = line.strip()
                if line and not line.startswith("#"):
                    base = m3u8_url.rsplit("/", 1)[0] + "/"
                    variant_url = line if line.startswith("http") else (base + line.lstrip("/"))
                    r2 = get(variant_url, timeout=15)
                    if r2.ok:
                        text = r2.text
                    break
        pages = [int(m.group(1)) for m in _PAGE_RE.finditer(text)]
        return max(pages) if pages else None
    except Exception:
        return None


def download_m3u8_with_ffmpeg(m3u8_url: str, output_path: str, timeout_sec: int | None = None, log: bool = False) -> bool:
    """Run ffmpeg to download m3u8 to output_path. Returns True on success."""
    ffmpeg = None
    if FFMPEG_PATH:
        if os.path.isfile(FFMPEG_PATH):
            ffmpeg = FFMPEG_PATH
        elif os.path.isdir(FFMPEG_PATH):
            ffmpeg = os.path.join(FFMPEG_PATH, "ffmpeg.exe" if sys.platform == "win32" else "ffmpeg")
            if not os.path.isfile(ffmpeg):
                ffmpeg = None
    if not ffmpeg:
        ffmpeg = "ffmpeg"
        if sys.platform == "win32":
            for name in ("ffmpeg.exe", "ffmpeg"):
                if shutil.which(name):
                    ffmpeg = name
                    break
        else:
            if shutil.which("ffmpeg"):
                ffmpeg = "ffmpeg"
    cmd = [
        ffmpeg, "-y", "-i", m3u8_url,
        "-c", "copy", "-bsf:a", "aac_adtstoasc",
        output_path,
    ]
    try:
        total_pages = _get_total_pages_from_m3u8(m3u8_url)
        if total_pages is not None:
            set_state(download={"total_pages": total_pages, "current_page": 0})

        proc = subprocess.Popen(cmd, stderr=subprocess.PIPE, stdout=subprocess.DEVNULL, universal_newlines=True, errors="replace")
        stderr_lines = []
        opening_re = re.compile(r"Opening\s+'([^']+)'\s+for\s+reading", re.I)

        def read_stderr():
            for line in proc.stderr:
                line = line.rstrip("\n\r")
                if line:
                    stderr_lines.append(line)
                    if log:
                        print("      [ffmpeg]", line, flush=True)
                    set_state(explanation="[ffmpeg] " + line)
                    # Extract current page from "Opening '...page-1232.html' for reading"
                    mo = opening_re.search(line)
                    if mo:
                        url = mo.group(1)
                        pm = _PAGE_RE.search(url)
                        if pm:
                            current = int(pm.group(1))
                            d = {"current_page": current}
                            if total_pages is not None:
                                d["total_pages"] = total_pages
                            set_state(download=d)

        reader = threading.Thread(target=read_stderr, daemon=True)
        reader.start()
        start = time.time()
        while proc.poll() is None:
            if timeout_sec is not None and (time.time() - start) > timeout_sec:
                proc.kill()
                proc.wait()
                print("      ffmpeg timeout", flush=True)
                return False
            time.sleep(0.2)
        reader.join(timeout=2.0)
        if proc.returncode != 0:
            tail = "\n".join(stderr_lines[-20:]) if stderr_lines else ""
            print("      ffmpeg failed:", proc.returncode, tail[-500:] if tail else "", flush=True)
            return False
        return True
    except FileNotFoundError as e:
        print("      ffmpeg failed:", e, flush=True)
        return False


def run_sniffer(
    url: str,
    *,
    preferred_quality: str | None = None,
    log: bool = True,
) -> dict:
    """
    Run the sniffer pipeline: get m3u8 link from vidsrc embed URL. Returns result dict.
    Does not download; caller runs ffmpeg separately.
    """
    quality = (preferred_quality or PREFERRED_QUALITY).strip().lower()
    if quality not in ("low", "med", "high"):
        quality = "high"
    result = {"success": False, "status": "idle", "m3u8_link": None, "output_path": None, "error": None}

    set_state(phase="searching", explanation="Starting sniffer pipeline")

    def _finish(res: dict) -> dict:
        set_state(phase="idle", snifferResult=res, explanation="Sniffer pipeline completed")
        return res

    def _log(msg: str) -> None:
        if log:
            print(msg)

    set_state(explanation="[1/8] Fetching vidsrc embed page")
    try:
        vidsrc = get(url)
    except Exception as e:
        result["status"] = "vidsrc_down"
        result["error"] = str(e)
        _log("      FAIL: vidsrc is down (connection/timeout/error).")
        _log("      " + str(e))
        return _finish(result)

    if vidsrc.status_code != 200:
        result["status"] = "vidsrc_down"
        result["error"] = "status %s" % vidsrc.status_code
        _log("      FAIL: vidsrc is down or returned error (status %s)." % vidsrc.status_code)
        return _finish(result)

    _log("      OK %s | length: %s" % (vidsrc.status_code, len(vidsrc.text)))
    soup = parse_html(vidsrc.text)
    set_state(explanation="[2/8] Finding Cloudnestra /rcp/ iframe")
    iframe = soup.find("iframe", id="player_iframe")
    if not (iframe and iframe.get("src")):
        result["status"] = "video_not_available"
        result["error"] = "no player_iframe"
        _log("      FAIL: vidsrc is up but video is not available (no player_iframe).")
        return _finish(result)
    src = iframe["src"].strip()
    if src.startswith("//"):
        src = "https:" + src
    _log("      Cloudnestra /rcp URL: " + (src[:70] + "..." if len(src) > 70 else src))

    set_state(explanation="[3/8] Extracting rcp token and fetching /rcp/ page (play.html)")
    parsed = urlparse(src)
    if "/rcp/" not in parsed.path:
        result["status"] = "error"
        result["error"] = "no /rcp/ segment"
        _log("      Unexpected path, no /rcp/ segment.")
        return _finish(result)
    rcp_token = parsed.path.split("/rcp/", 1)[1]
    _log("      rcp_token: " + (rcp_token[:40] + "..." if len(rcp_token) > 40 else rcp_token))

    rcp_html = None
    rcp_cookies = []  # list of {name, value, domain, path} from browser (if used)
    try:
        rcp_resp = get(src)
        rcp_resp.raise_for_status()
        rcp_html = rcp_resp.text
        _log("      OK %s" % rcp_resp.status_code)
    except Exception as e:
        _log("      GET rcp failed: %s" % e)

    # If normal GET didn't return content with prorcp token, try browser (Cloudflare Turnstile)
    prorcp_token = _extract_prorcp_token(rcp_html) if rcp_html else None
    if not prorcp_token:
        set_state(explanation="[3/8] Rcp page behind Turnstile, using browser…")
        rcp_html, rcp_cookies = _fetch_rcp_page_with_browser(src, log_fn=_log)
        prorcp_token = _extract_prorcp_token(rcp_html) if rcp_html else None

    set_state(explanation="[4/8] Finding /prorcp/ token in JS (loadIframe)")
    if not prorcp_token:
        result["status"] = "error"
        result["error"] = "no prorcp token (Turnstile or missing in page)"
        _log("      No /prorcp/ token found in rcp page.")
        return _finish(result)
    _log("      prorcp_token: " + (prorcp_token[:40] + "..." if len(prorcp_token) > 40 else prorcp_token))

    set_state(explanation="[5/8] GET prorcp player page (with Referer)")
    session_with_cookies = None
    if rcp_cookies:
        session_with_cookies = create_agent()
        for c in rcp_cookies:
            session_with_cookies.cookies.set(
                c["name"], c["value"],
                domain=c.get("domain") or parsed.hostname or "",
                path=c.get("path") or "/",
            )
    try:
        prorcp_resp = get_prorcp(prorcp_token, rcp_token, session=session_with_cookies)
        prorcp_resp.raise_for_status()
    except Exception as e:
        result["status"] = "error"
        result["error"] = str(e)
        return _finish(result)
    _log("      OK %s | length: %s" % (prorcp_resp.status_code, len(prorcp_resp.text)))

    set_state(explanation="[6/8] Extracting m3u8 path and test_doms from prorcp HTML")
    m3u8_path = None
    m_path = re.search(r'https?://[^/]+(/pl/[^\s"\'<>]+\.m3u8)', prorcp_resp.text, re.I)
    if m_path:
        m3u8_path = m_path.group(1)
    if not m3u8_path:
        m_path = re.search(r'(/pl/[^\s"\'<>]+\.m3u8)', prorcp_resp.text, re.I)
        if m_path:
            m3u8_path = m_path.group(1)
    if not m3u8_path:
        _log("      No m3u8 path found.")
    else:
        _log("      m3u8_path: " + (m3u8_path[:80] + "..." if len(m3u8_path) > 80 else m3u8_path))
    dom_candidates = []
    m_arr = re.search(r'test_doms\s*=\s*\[([^\]]*)\]', prorcp_resp.text, re.DOTALL)
    if m_arr:
        inner = m_arr.group(1)
        dom_candidates = [u.strip('"').strip() for u in re.findall(r'"https?://[^"]+"', inner)]
    if not dom_candidates:
        dom_candidates = [u.strip('"') for u in re.findall(r'"https?://[^"]+\.(?:com|net|org)[^"]*"', prorcp_resp.text)]
    _log("      test_doms: %s | %s" % (len(dom_candidates), dom_candidates[:4]))

    if not dom_candidates:
        result["status"] = "no_domains"
        result["error"] = "no test_doms found"
        _log("      No test_doms found.")
        return _finish(result)
    if not m3u8_path:
        result["status"] = "no_m3u8_path"
        result["error"] = "no m3u8 path in prorcp"
        _log("      Have domains but no m3u8 path; stopping.")
        return _finish(result)

    set_state(explanation="[7/8] Testing domains for availability (200)")
    ready_domain = None
    for dom in dom_candidates:
        try:
            r = get(dom.rstrip("/"), timeout=10)
            if r.status_code == 200:
                ready_domain = dom.rstrip("/")
                _log("      Ready: " + ready_domain)
                break
            _log("      %s -> %s" % (dom, r.status_code))
        except Exception as e:
            _log("      Skip %s | %s" % (dom, e))
    if not ready_domain:
        result["status"] = "no_ready_domain"
        result["error"] = "no domain returned 200"
        _log("      No domain returned 200.")
        return _finish(result)

    set_state(explanation="[8/8] Fetching master m3u8, parsing variants (low/med/high), picking %s" % quality)
    master_url = ready_domain + (m3u8_path if m3u8_path.startswith("/") else "/" + m3u8_path)
    _log("      master_url: " + (master_url[:80] + "..." if len(master_url) > 80 else master_url))
    try:
        master_resp = get(master_url, timeout=15)
        master_resp.raise_for_status()
        variants = _parse_master_m3u8(master_resp.text)
        _log("      Master OK | variants: %s" % len(variants))
    except Exception as e:
        _log("      Failed to fetch master m3u8: %s" % e)
        variants = []
    if not variants:
        _log("      No variants -> using master URL as final link.")
        m3u8_link = master_url
    else:
        for v in variants:
            _log("        %s %s %s -> %s" % (v.get("tier"), v.get("resolution"), v.get("bandwidth"), (v.get("path") or "")[:60] + "..."))
        chosen = _pick_variant(variants, quality)
        if chosen:
            final_path = chosen["path"]
            final_url = ready_domain + (final_path if final_path.startswith("/") else "/" + final_path)
            _log("      Picked: %s | %s | BANDWIDTH= %s" % (chosen.get("tier"), chosen.get("resolution"), chosen.get("bandwidth")))
            m3u8_link = final_url
        else:
            m3u8_link = master_url
    result["m3u8_link"] = m3u8_link
    result["success"] = True
    result["status"] = "ok"
    return _finish(result)


def main():
    """CLI: run sniffer for default URL and exit with 0/1."""
    res = run_sniffer(URL)
    if res["success"]:
        print("m3u8 link:", res.get("m3u8_link"))
        if res.get("output_path"):
            print("output_path:", res["output_path"])
    else:
        print("FAIL:", res.get("status"), res.get("error"))
        sys.exit(1)


if __name__ == "__main__":
    main()
