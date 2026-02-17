"""
Downloader service: search (TPB) → pick best match → download → upload to staging (same API as frontend chunk upload).
CLI:  python main.py "Movie Title"
Serve:  python main.py --serve  then POST /download {"title": "..."}
Set BACKEND_URL and STAGING_SERVICE_TOKEN to push downloaded file to staging (e.g. from another container).
"""
import argparse
import asyncio
import json
import os
import re
import shutil
import subprocess
import sys
import time
import uuid
from threading import Lock, Thread

import requests
from the_python_bay import tpb

# Load .env from downloader directory, cwd, or project root so BACKEND_URL, STAGING_SERVICE_TOKEN, etc. are set
try:
    from dotenv import load_dotenv
    _script_dir = os.path.dirname(os.path.abspath(__file__))
    _env_locations = [
        os.path.join(_script_dir, ".env"),
        ".env",
        os.path.join(_script_dir, "..", ".env"),
    ]
    for _p in _env_locations:
        if load_dotenv(_p):
            break
    load_dotenv()  # cwd
except ImportError:
    pass  # optional: run without .env if dotenv not installed

# -----------------------------------------------------------------------------
# Config
# -----------------------------------------------------------------------------

DOWNLOAD_DIR = os.path.abspath(os.environ.get("DOWNLOAD_DIR", "./downloads"))
START_TIMEOUT_MIN = 5
MAX_ATTEMPTS = 5
MIN_STARTED_BYTES = 100 * 1024  # 100 KB = consider "started"

# Backend: staging upload API (POST /api/staging/upload) + webhooks (download-done, upload-done, failed)
BACKEND_URL = os.environ.get("BACKEND_URL", "").rstrip("/")
STAGING_SERVICE_TOKEN = os.environ.get("STAGING_SERVICE_TOKEN", "")
WEBHOOK_SECRET = os.environ.get("WEBHOOK_SECRET", "")

# Prefer higher resolution (1080 > 720 > 480)
RESOLUTION_ORDER = ["1080", "2160", "720", "480"]
VIDEO_EXTENSIONS = (".mp4", ".mkv", ".webm")

def _log(msg: str) -> None:
    print(f"[download] {msg}", flush=True)


def _call_webhook(job_id: str, event: str, **kwargs: object) -> bool:
    """POST to backend webhook (download-done, upload-done, failed). Returns True if 2xx."""
    if not BACKEND_URL or not WEBHOOK_SECRET:
        return False
    url = f"{BACKEND_URL}/api/download-queue/webhook/{event}"
    headers = {"Content-Type": "application/json", "X-Webhook-Secret": WEBHOOK_SECRET}
    body = {"jobId": job_id, **kwargs}
    try:
        r = requests.post(url, json=body, headers=headers, timeout=10)
        return 200 <= r.status_code < 300
    except requests.RequestException as e:
        _log(f"Webhook {event} failed: {e}")
        return False


def _dir_size(path: str) -> int:
    total = 0
    try:
        for entry in os.scandir(path):
            if entry.is_file():
                total += entry.stat().st_size
            elif entry.is_dir():
                total += _dir_size(entry.path)
    except OSError:
        pass
    return total


# -----------------------------------------------------------------------------
# Title matching + resolution + seeders → sorted list of best results
# -----------------------------------------------------------------------------

def _normalize(s: str) -> str:
    if not s:
        return ""
    return re.sub(r"[^a-z0-9]", "", (s or "").lower())


def _extract_year(s: str) -> str | None:
    if not s:
        return None
    m = re.search(r"\((\d{4})\)|(?:^|\s)(\d{4})(?:\s|$|\))", (s or "").strip())
    return (m.group(1) or m.group(2)) if m else None


def _resolution_score(name: str) -> int:
    n = (name or "").lower()
    for i, res in enumerate(RESOLUTION_ORDER):
        if res in n:
            return len(RESOLUTION_ORDER) - i
    return 0


def _pick_best_matches(query: str, results: list, limit: int = MAX_ATTEMPTS) -> list:
    """Sorted by: title+year match, then resolution (1080>720>…), then seeders. Returns up to `limit`."""
    norm_query = _normalize(query)
    year = _extract_year(query)

    def score(torrent):
        name = (torrent.name or "") or ""
        norm_name = _normalize(name)
        title_in = 1 if norm_query in norm_name else 0
        year_in = 1 if year and year in name else 0
        match_score = title_in * 2 + year_in
        res_score = _resolution_score(name)
        try:
            seeders = int(getattr(torrent, "seeders", 0) or 0)
        except (TypeError, ValueError):
            seeders = 0
        return (match_score, res_score, seeders, torrent)

    scored = [score(r) for r in results]
    scored.sort(key=lambda x: (x[0], x[1], x[2]), reverse=True)
    return [x[3] for x in scored[:limit]]


# -----------------------------------------------------------------------------
# HTTP server (--serve)
# -----------------------------------------------------------------------------

_download_lock = Lock()


def _create_app():
    from flask import Flask, request, jsonify
    # Log so user can see if .env was loaded (no secrets printed)
    print(
        f"[download] Config: BACKEND_URL={'set' if BACKEND_URL else 'NOT SET'}, "
        f"STAGING_SERVICE_TOKEN={'set' if STAGING_SERVICE_TOKEN else 'NOT SET'}",
        flush=True,
    )
    app = Flask(__name__)

    @app.route("/health", methods=["GET"])
    def health():
        return jsonify({"ok": True})

    @app.route("/download", methods=["POST"])
    def download():
        if not _download_lock.acquire(blocking=False):
            return jsonify({"success": False, "message": "Download already in progress"}), 503
        data = request.get_json(silent=True) or {}
        title = (data.get("title") or "").strip()
        if not title:
            _download_lock.release()
            return jsonify({"success": False, "message": "Missing 'title'"}), 400
        job_id = (data.get("jobId") or "").strip() or None
        tmdb_id = data.get("tmdbId")
        if tmdb_id is not None:
            try:
                tmdb_id = int(tmdb_id)
            except (TypeError, ValueError):
                tmdb_id = None
        poster_path = (data.get("poster_path") or data.get("posterPath") or "").strip() or None
        kwargs = {"title": title, "job_id": job_id, "tmdb_id": tmdb_id, "poster_path": poster_path}

        def run_then_release():
            try:
                run_download(**kwargs)
            finally:
                _download_lock.release()

        Thread(target=run_then_release, daemon=True).start()
        if job_id:
            return jsonify({"success": True, "jobId": job_id})
        return jsonify({"success": True, "message": f"Started: {title}"})

    return app


# -----------------------------------------------------------------------------
# Aria2 (fallback on Windows / when torrentp fails)
# -----------------------------------------------------------------------------

ARIA2_TRACKERS = [
    "udp://tracker.opentrackr.org:1337/announce",
    "udp://open.stealth.si:80/announce",
    "udp://tracker.torrent.eu.org:451/announce",
    "udp://exodus.desync.com:6969/announce",
    "udp://tracker.openbittorrent.com:6969/announce",
    "udp://9.rarbg.com:2810/announce",
]


def _download_magnet_aria2(magnet: str, save_path: str, start_timeout_sec: int | None = None) -> bool:
    aria2 = shutil.which("aria2c")
    if not aria2:
        return False
    os.makedirs(save_path, exist_ok=True)
    cmd = [
        aria2, "--dir=" + save_path,
        "--bt-metadata-only=false", "--bt-save-metadata=true",
        "--bt-tracker=" + ",".join(ARIA2_TRACKERS),
        "--file-allocation=none",
        "--seed-ratio=0",  
        "--seed-time=0",
        magnet,
    ]
    if start_timeout_sec is None:
        return subprocess.run(cmd, cwd=save_path, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode == 0

    # Start with timeout: if no data in start_timeout_sec, kill and return False
    proc = subprocess.Popen(cmd, cwd=save_path, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    deadline = time.monotonic() + start_timeout_sec
    check_interval = 30
    while time.monotonic() < deadline:
        time.sleep(min(check_interval, max(0, deadline - time.monotonic())))
        if proc.poll() is not None:
            return proc.returncode == 0
        if _dir_size(save_path) >= MIN_STARTED_BYTES:
            proc.wait()
            return proc.returncode == 0
    proc.terminate()
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        proc.kill()
    return False


# -----------------------------------------------------------------------------
# Torrentp (uses libtorrent; often fails on Windows due to DLL)
# -----------------------------------------------------------------------------

async def _download_torrentp_async(magnet: str, save_path: str) -> bool:
    try:
        from torrentp import TorrentDownloader
    except ImportError:
        return False
    os.makedirs(save_path, exist_ok=True)
    try:
        downloader = TorrentDownloader(magnet, save_path)
        await downloader.start_download()
        return True
    except Exception:
        return False


def _download_torrentp(magnet: str, save_path: str) -> bool:
    return asyncio.run(_download_torrentp_async(magnet, save_path))


# -----------------------------------------------------------------------------
# Upload to staging (POST /api/staging/upload-chunk, same as frontend)
# -----------------------------------------------------------------------------

def _find_latest_video(path: str) -> str | None:
    """Crawl path (file or dir) and return the most recently modified video file."""
    best_path, best_mtime = None, 0
    if os.path.isfile(path):
        if path.lower().endswith(VIDEO_EXTENSIONS):
            return path
        return None
    for root, _dirs, files in os.walk(path):
        for f in files:
            if f.lower().endswith(VIDEO_EXTENSIONS):
                p = os.path.join(root, f)
                mtime = os.path.getmtime(p)
                if mtime > best_mtime:
                    best_mtime = mtime
                    best_path = p
    return best_path


def _clear_after_upload(file_path: str, download_root: str) -> None:
    """Delete everything inside the downloads dir so it is empty for the next job."""
    try:
        if not os.path.isdir(download_root):
            return
        for name in os.listdir(download_root):
            path = os.path.join(download_root, name)
            try:
                if os.path.isfile(path):
                    os.remove(path)
                elif os.path.isdir(path):
                    shutil.rmtree(path)
            except OSError as e:
                _log(f"Clear failed for {path}: {e}")
        _log("Cleared downloads dir.")
    except OSError as e:
        _log(f"Clear failed: {e}")


def _mimetype_for_path(path: str) -> str:
    ext = os.path.splitext(path)[1].lower()
    if ext == ".mp4":
        return "video/mp4"
    if ext == ".webm":
        return "video/webm"
    if ext in (".mkv", ".mka"):
        return "video/x-matroska"
    return "video/mp4"


def upload_file_to_staging(
    file_path: str,
    base_url: str,
    token: str,
    *,
    title: str = "",
    tmdb_id: int | None = None,
    poster_path: str | None = None,
    job_id: str | None = None,
) -> tuple[bool, str | None]:
    """POST whole file to backend POST /api/staging/upload (single multipart). Same LAN as backend avoids chunk timeouts.
    Reports progress via webhook if job_id set. Returns (success, stagingId or None)."""
    if not base_url or not token:
        return False, None
    url = f"{base_url}/api/staging/upload"
    headers = {"Authorization": f"Bearer {token}"}
    filename = os.path.basename(file_path)
    mimetype = _mimetype_for_path(file_path)
    data: dict[str, str] = {}
    if title:
        data["title"] = title
    if tmdb_id is not None:
        data["tmdbId"] = str(tmdb_id)
    if poster_path:
        data["poster_path"] = poster_path

    staging_id: str | None = None
    try:
        with open(file_path, "rb") as f:
            files = {"file": (filename, f, mimetype)}
            # Long timeout for large files on same LAN (e.g. 15 min)
            r = requests.post(url, headers=headers, data=data, files=files, timeout=900, stream=True)
        if r.status_code >= 400:
            _log(f"Upload failed: {r.status_code} {r.text[:300]}")
            return False, None
        for line in r.iter_lines(decode_unicode=True):
            if not line:
                continue
            try:
                obj = json.loads(line)
                if obj.get("stage") == "writing" and obj.get("progress") is not None and job_id:
                    _call_webhook(job_id, "upload-progress", progress=int(obj["progress"]))
                if obj.get("stage") == "done" and obj.get("stagingId"):
                    staging_id = str(obj["stagingId"])
                if obj.get("stage") == "error":
                    _log(f"Upload error: {obj.get('message', '')}")
                    return False, None
            except Exception:
                pass
    except requests.RequestException as e:
        _log(f"Upload error: {e}")
        return False, None
    _log("Upload to staging done.")
    return True, staging_id


# -----------------------------------------------------------------------------
# Unified download: Windows → aria2 first, else torrentp then aria2
# -----------------------------------------------------------------------------

def download_magnet_to_dir(magnet: str, save_path: str, start_timeout_min: int | None = None) -> bool:
    os.makedirs(save_path, exist_ok=True)
    timeout_sec = int(start_timeout_min * 60) if start_timeout_min else None
    if sys.platform == "win32":
        if _download_magnet_aria2(magnet, save_path, start_timeout_sec=timeout_sec):
            return True
        if not start_timeout_min and _download_torrentp(magnet, save_path):
            return True
    else:
        if _download_torrentp(magnet, save_path):
            return True
        if _download_magnet_aria2(magnet, save_path, start_timeout_sec=timeout_sec):
            return True
    return False


# -----------------------------------------------------------------------------
# Pipeline: search → best matches (title+year, resolution, seeders) → try up to 5 with 10 min start timeout
# -----------------------------------------------------------------------------

def run_download(
    title: str,
    job_id: str | None = None,
    tmdb_id: int | None = None,
    poster_path: str | None = None,
) -> None:
    def fail(msg: str) -> None:
        _log(msg)
        if job_id:
            _call_webhook(job_id, "failed", errorMessage=msg)

    try:
        _log(f"Searching: {title!r}")
        results = tpb.search(title)
        if not results:
            fail("No results.")
            return

        candidates = _pick_best_matches(title, results, limit=MAX_ATTEMPTS)
        for attempt, chosen in enumerate(candidates, 1):
            magnet = (chosen.magnet or "").strip()
            if not magnet.lower().startswith("magnet:"):
                continue
            seeders = getattr(chosen, "seeders", None)
            s = f" ({seeders} seeders)" if seeders is not None else ""
            _log(f"Attempt {attempt}/{MAX_ATTEMPTS}: {chosen.name or '—'}{s} → {DOWNLOAD_DIR}")
            ok = download_magnet_to_dir(magnet, DOWNLOAD_DIR, start_timeout_min=START_TIMEOUT_MIN)
            print(f"Downloaded: {ok} script continue")
            print("-" * 100)
            if ok:
                video_path = _find_latest_video(DOWNLOAD_DIR)
                if not video_path:
                    fail("No video file found in downloads dir.")
                    return
                if job_id:
                    _call_webhook(job_id, "download-done")
                if BACKEND_URL and STAGING_SERVICE_TOKEN:
                    _log(f"Uploading to staging: {video_path}")
                    upload_title = (chosen.name or "").strip() or title
                    ok_upload, staging_id = upload_file_to_staging(
                        video_path,
                        BACKEND_URL,
                        STAGING_SERVICE_TOKEN,
                        title=upload_title,
                        tmdb_id=tmdb_id,
                        poster_path=poster_path,
                        job_id=job_id,
                    )
                    if ok_upload:
                        _log("Staging upload done.")
                        if job_id:
                            _call_webhook(job_id, "upload-done", stagingId=staging_id)
                        _clear_after_upload(video_path, DOWNLOAD_DIR)
                    else:
                        fail("Staging upload failed.")
                else:
                    _log("Download done. (Set BACKEND_URL and STAGING_SERVICE_TOKEN to push to staging.)")
                    if job_id:
                        _call_webhook(job_id, "upload-done")
                return
            _log(f"No start in {START_TIMEOUT_MIN} min, trying next.")
        fail(f"Failed after {MAX_ATTEMPTS} attempts.")
    except Exception as e:
        fail(f"Error: {e}")


# -----------------------------------------------------------------------------
# CLI
# -----------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Downloader: title → search → download")
    parser.add_argument("title", nargs="?", help="Video title (CLI)")
    parser.add_argument("--serve", action="store_true", help="Run HTTP server")
    parser.add_argument("--port", type=int, default=3502, help="Server port")
    args = parser.parse_args()

    if args.serve:
        _create_app().run(host="0.0.0.0", port=args.port, debug=False, use_reloader=False)
        return
    if args.title:
        run_download(args.title)
        return
    parser.print_help()
    sys.exit(1)


if __name__ == "__main__":
    main()
