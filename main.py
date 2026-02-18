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
import traceback
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

# Upload: connect 30s, read 2h so long uploads don't false-timeout; connection drops still raise
UPLOAD_CONNECT_TIMEOUT = 30
UPLOAD_READ_TIMEOUT = 7200

# Stall detection: no progress for this many minutes → fail attempt and try next torrent
STALL_TIMEOUT_MIN = int(os.environ.get("STALL_TIMEOUT_MIN", "15"))
STALL_CHECK_INTERVAL_SEC = int(os.environ.get("STALL_CHECK_INTERVAL_SEC", "60"))
# Disk: fail before download/upload if free space below this (GB); 0 = disabled
MIN_FREE_DISK_GB = float(os.environ.get("MIN_FREE_DISK_GB", "10"))

# -----------------------------------------------------------------------------
# Progress state (download + upload) — backend can GET /progress to see where we are
# -----------------------------------------------------------------------------

_progress_lock = Lock()
_progress: dict = {"phase": "idle", "download": {}, "upload": {}, "updatedAt": None}

def _progress_state() -> dict:
    """Current progress (thread-safe read)."""
    with _progress_lock:
        return {
            "jobId": _progress.get("jobId"),
            "phase": _progress.get("phase", "idle"),  # idle|searching|downloading|uploading|done|failed
            "download": dict(_progress.get("download") or {}),
            "upload": dict(_progress.get("upload") or {}),
            "updatedAt": _progress.get("updatedAt"),
        }

def _set_progress(**kwargs: object) -> None:
    with _progress_lock:
        _progress["updatedAt"] = time.time()
        for k, v in kwargs.items():
            if v is not None:
                if k == "download" and isinstance(v, dict):
                    _progress.setdefault("download", {}).update(v)
                elif k == "upload" and isinstance(v, dict):
                    _progress.setdefault("upload", {}).update(v)
                else:
                    _progress[k] = v

def _clear_progress() -> None:
    with _progress_lock:
        _progress.clear()
        _progress["phase"] = "idle"
        _progress["updatedAt"] = time.time()

def _log(msg: str) -> None:
    print(f"[download] {msg}", flush=True)


def _check_disk_space(path: str, min_free_gb: float) -> bool:
    """Return True if path has at least min_free_gb free (GB). If min_free_gb <= 0, always True."""
    if min_free_gb <= 0:
        return True
    try:
        usage = shutil.disk_usage(os.path.abspath(path))
        free_gb = usage.free / (1024**3)
        return free_gb >= min_free_gb
    except OSError:
        return True


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


def _bdecode(s: bytes, i: list) -> object:
    """Minimal bencode decode (enough for info.length / info.files)."""
    if i[0] >= len(s):
        return None
    if s[i[0] : i[0] + 1] == b"i":
        i[0] += 1
        end = s.index(b"e", i[0])
        val = int(s[i[0] : end])
        i[0] = end + 1
        return val
    if s[i[0] : i[0] + 1] == b"l":
        i[0] += 1
        out = []
        while s[i[0] : i[0] + 1] != b"e":
            out.append(_bdecode(s, i))
        i[0] += 1
        return out
    if s[i[0] : i[0] + 1] == b"d":
        i[0] += 1
        out = {}
        while s[i[0] : i[0] + 1] != b"e":
            k = _bdecode(s, i)
            out[k] = _bdecode(s, i)
        i[0] += 1
        return out
    colon = s.index(b":", i[0])
    length = int(s[i[0] : colon])
    i[0] = colon + 1 + length
    return s[colon + 1 : colon + 1 + length]


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


def _torrent_total_from_dir(path: str) -> int | None:
    """If aria2 saved a .torrent in path, parse it and return total size in bytes (for progress)."""
    try:
        for entry in os.scandir(path):
            if entry.is_file() and entry.name.endswith(".torrent"):
                try:
                    with open(entry.path, "rb") as f:
                        data = f.read()
                    idx = [0]
                    top = _bdecode(data, idx)
                    if not isinstance(top, dict) or b"info" not in top:
                        return None
                    info = top[b"info"]
                    if not isinstance(info, dict):
                        return None
                    if b"length" in info:
                        return int(info[b"length"])
                    if b"files" in info:
                        return sum(int(f.get(b"length", 0)) for f in info[b"files"] if isinstance(f, dict))
                except (ValueError, IndexError, KeyError, OSError):
                    pass
                return None
    except OSError:
        pass
    return None


def _fetch_metadata_only(magnet: str, save_path: str, timeout_sec: int = 90) -> str | None:
    """Run aria2 with --bt-metadata-only to get .torrent only. Returns path to .torrent or None."""
    aria2 = shutil.which("aria2c")
    if not aria2:
        return None
    os.makedirs(save_path, exist_ok=True)
    cmd = [
        aria2,
        "--dir=" + save_path,
        "--bt-metadata-only=true",
        "--bt-save-metadata=true",
        "--bt-tracker=" + ",".join(ARIA2_TRACKERS),
        magnet,
    ]
    try:
        r = subprocess.run(cmd, cwd=save_path, capture_output=True, timeout=timeout_sec)
        if r.returncode != 0:
            return None
        for entry in os.scandir(save_path):
            if entry.is_file() and entry.name.endswith(".torrent"):
                return entry.path
    except (subprocess.TimeoutExpired, OSError):
        pass
    return None


def _torrent_best_video_index(torrent_path: str) -> tuple[int, int | None]:
    """Parse .torrent and return (total_bytes, 1-based_index of largest video file, or None for all).
    Single-file: return (length, 1). Multi-file: return (total, index of largest .mp4/.mkv/.webm), or (total, None) if none."""
    try:
        with open(torrent_path, "rb") as f:
            data = f.read()
    except OSError:
        return 0, None
    idx = [0]
    top = _bdecode(data, idx)
    if not isinstance(top, dict) or b"info" not in top:
        return 0, None
    info = top[b"info"]
    if not isinstance(info, dict):
        return 0, None
    if b"length" in info:
        return int(info[b"length"]), 1
    if b"files" not in info:
        return 0, None
    files = info[b"files"]
    if not isinstance(files, list):
        return 0, None
    total = 0
    best_index: int | None = None
    best_size = 0
    for i, f in enumerate(files):
        if not isinstance(f, dict):
            continue
        length = int(f.get(b"length", 0))
        total += length
        path_list = f.get(b"path", f.get(b"path.utf-8", []))
        if isinstance(path_list, list) and path_list:
            last = path_list[-1]
            if isinstance(last, bytes):
                name = last.decode("utf-8", "replace").lower()
            else:
                name = str(last).lower()
        else:
            name = ""
        if any(name.endswith(ext) for ext in VIDEO_EXTENSIONS) and length > best_size:
            best_size = length
            best_index = i + 1
    if total == 0:
        return 0, None
    return total, best_index


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
#ANCHOR - SERVER
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

    @app.route("/progress", methods=["GET"])
    def progress():
        """Return current download + upload progress so backend can poll (e.g. to see if failure is downloader→backend or backend→db)."""
        print(_progress_state())
        return jsonify(_progress_state())

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


def _run_aria2_download(
    aria2: str,
    save_path: str,
    start_timeout_sec: int | None,
    *extra_args: str,
    input_arg: str,
) -> bool:
    """Run aria2 for full download. input_arg is magnet URI or path to .torrent."""
    cmd = [
        aria2,
        "--dir=" + save_path,
        "--bt-metadata-only=false",
        "--bt-save-metadata=true",
        "--bt-tracker=" + ",".join(ARIA2_TRACKERS),
        "--file-allocation=none",
        "--seed-ratio=0",
        "--seed-time=0",
        "--bt-hash-check-seed=false",
        *extra_args,
        input_arg,
    ]
    if start_timeout_sec is None:
        return subprocess.run(
            cmd, cwd=save_path, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
        ).returncode == 0
    proc = subprocess.Popen(cmd, cwd=save_path, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    deadline = time.monotonic() + start_timeout_sec
    check_interval = 30
    while time.monotonic() < deadline:
        time.sleep(min(check_interval, max(0, deadline - time.monotonic())))
        if proc.poll() is not None:
            return proc.returncode == 0
        if _dir_size(save_path) >= MIN_STARTED_BYTES:
            break
    else:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
        return False

    # Monitor loop: detect stall (no progress for STALL_TIMEOUT_MIN) or process exit
    stall_timeout_sec = STALL_TIMEOUT_MIN * 60
    interval = min(STALL_CHECK_INTERVAL_SEC, max(5, stall_timeout_sec // 4))
    last_bytes = _dir_size(save_path)
    last_progress_time = time.monotonic()
    while True:
        time.sleep(interval)
        if proc.poll() is not None:
            return proc.returncode == 0
        current_bytes = _dir_size(save_path)
        if current_bytes > last_bytes:
            last_bytes = current_bytes
            last_progress_time = time.monotonic()
        # Stream countdown: seconds until considered stalled (so frontend can show "Stall in Xs")
        elapsed = time.monotonic() - last_progress_time
        stall_seconds_remaining = max(0, int(stall_timeout_sec - elapsed))
        _set_progress(download={"stall_seconds_remaining": stall_seconds_remaining})
        if elapsed >= stall_timeout_sec:
            stall_min = STALL_TIMEOUT_MIN
            msg = f"Download stalled (no progress for {stall_min} min), terminating."
            _log(msg)
            _set_progress(download={"error": f"Download stalled (no progress for {stall_min} min)"})
            proc.terminate()
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                proc.kill()
            return False


def _download_magnet_aria2(magnet: str, save_path: str, start_timeout_sec: int | None = None) -> bool:
    aria2 = shutil.which("aria2c")
    if not aria2:
        return False
    os.makedirs(save_path, exist_ok=True)

    # Multi-file fix: fetch metadata only, then download just the main video (--select-file)
    # so we don't wait for samples, .nfo, .jpg, subs, etc.
    torrent_path = _fetch_metadata_only(magnet, save_path, timeout_sec=90)
    if torrent_path:
        _log("Metadata-only: got .torrent")
        total, select_index = _torrent_best_video_index(torrent_path)
        if select_index is not None and total > 0:
            _log(f"Selecting single file index {select_index} (of {total} bytes) from torrent.")
            return _run_aria2_download(
                aria2,
                save_path,
                start_timeout_sec,
                "--select-file=" + str(select_index),
                input_arg=torrent_path,
            )

    # Fallback: metadata-only failed or no single video identified; download full torrent
    _log("Metadata-only: failed/timeout or no single video, using full torrent")
    return _run_aria2_download(aria2, save_path, start_timeout_sec, input_arg=magnet)


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


class _CountingFileReader:
    """File-like that counts bytes read and reports upload progress (downloader→backend)."""

    def __init__(self, path: str, on_progress: object) -> None:
        self._path = path
        self._on_progress = on_progress
        self._f = open(path, "rb")
        self._total = os.path.getsize(path)
        self._read = 0

    def read(self, size: int = -1) -> bytes:
        data = self._f.read(size)
        self._read += len(data)
        if self._on_progress and self._total:
            pct = min(100, round(100 * self._read / self._total))
            _set_progress(upload={"bytes_sent": self._read, "bytes_total": self._total, "percent": pct})
        return data

    def __iter__(self):
        return self

    def __next__(self) -> bytes:
        chunk = self.read(1024 * 1024)
        if not chunk:
            raise StopIteration
        return chunk

    def close(self) -> None:
        self._f.close()

    def __enter__(self) -> "_CountingFileReader":
        return self

    def __exit__(self, *args: object) -> None:
        self.close()


def _notify_upload_failed(job_id: str | None, err_msg: str) -> None:
    """So backend never stays stuck in 'uploading' (connection cut, HTTP error, etc.)."""
    _set_progress(upload={"phase": "error", "error": err_msg})
    if job_id:
        _call_webhook(job_id, "failed", errorMessage=err_msg)


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
    Reports progress via webhook if job_id set. On any failure (connection cut, HTTP error, server error) calls failed webhook so job does not stay stuck."""
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
    file_size = os.path.getsize(file_path)
    try:
        # Pass truthy on_progress so _CountingFileReader updates bytes_sent during upload
        with _CountingFileReader(file_path, True) as reader:
            files = {"file": (filename, reader, mimetype)}
            r = requests.post(
                url, headers=headers, data=data, files=files,
                timeout=(UPLOAD_CONNECT_TIMEOUT, UPLOAD_READ_TIMEOUT),
                stream=True,
            )
        if r.status_code >= 400:
            err_msg = f"HTTP {r.status_code}: {r.text[:200]}"
            _log(f"Upload failed: {r.status_code} {r.text[:300]}")
            _notify_upload_failed(job_id, err_msg)
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
                    msg = obj.get("message", "Upload failed")
                    _log(f"Upload error: {msg}")
                    _notify_upload_failed(job_id, msg)
                    return False, None
            except Exception:
                pass
        if staging_id is None:
            _log("Upload response had no stagingId; treating as failure.")
            _notify_upload_failed(job_id, "Upload response had no stagingId")
            return False, None
    except requests.RequestException as e:
        err_msg = str(e)
        _log(f"Upload error (connection/timeout): {e}")
        _log(traceback.format_exc())
        if staging_id is None:
            _notify_upload_failed(job_id, err_msg)
            return False, None
        _log("Already got stagingId before connection drop; reporting success.")
        return True, staging_id
    except Exception as e:
        err_msg = str(e)
        _log(f"Upload error: {e}")
        _log(traceback.format_exc())
        if staging_id is None:
            _notify_upload_failed(job_id, err_msg)
            return False, None
        return True, staging_id
    _set_progress(upload={"bytes_sent": file_size, "bytes_total": file_size, "phase": "done", "percent": 100})
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
        _log(f"phase=failed error={msg}")
        _set_progress(phase="failed", download={"error": msg})
        if job_id:
            _call_webhook(job_id, "failed", errorMessage=msg)

    try:
        _set_progress(jobId=job_id, phase="searching", download={}, upload={})
        _log("phase=searching")
        try:
            results = tpb.search(title)
        except Exception as e:
            _log(f"Search failed: {e}")
            _log(traceback.format_exc())
            fail(f"Search failed: {e}")
            return
        if not results:
            fail("No results.")
            return

        candidates = _pick_best_matches(title, results, limit=MAX_ATTEMPTS)
        if MIN_FREE_DISK_GB > 0 and not _check_disk_space(DOWNLOAD_DIR, MIN_FREE_DISK_GB):
            fail(f"Insufficient disk space in DOWNLOAD_DIR (need at least {MIN_FREE_DISK_GB} GB free).")
            return
        for attempt, chosen in enumerate(candidates, 1):
            magnet = (chosen.magnet or "").strip()
            if not magnet.lower().startswith("magnet:"):
                continue
            seeders = getattr(chosen, "seeders", None)
            torrent_name = (chosen.name or "").strip() or "—"
            s = f" ({seeders} seeders)" if seeders is not None else ""
            _log(f"phase=downloading attempt={attempt} torrent_name={torrent_name}{s}")
            _set_progress(
                phase="downloading",
                download={
                    "bytes_done": 0,
                    "bytes_total": None,
                    "error": None,
                    "seeders": seeders,
                    "attempt": attempt,
                    "torrent_name": torrent_name,
                },
            )
            download_done: list[bool] = []
            download_result: list[bool] = []

            def run_download_thread() -> None:
                try:
                    result = download_magnet_to_dir(magnet, DOWNLOAD_DIR, start_timeout_min=START_TIMEOUT_MIN)
                    download_result.append(result)
                except Exception as e:
                    _log(f"Download thread error: {e}")
                    _log(traceback.format_exc())
                    _set_progress(download={"error": str(e)})
                    download_result.append(False)
                finally:
                    download_done.append(True)

            t = Thread(target=run_download_thread, daemon=True)
            t.start()
            while not download_done:
                bytes_done = _dir_size(DOWNLOAD_DIR)
                # Total from .torrent (aria2 saves metadata); None until metadata is there
                bytes_total = _torrent_total_from_dir(DOWNLOAD_DIR)
                _set_progress(
                    download={
                        "bytes_done": bytes_done,
                        "bytes_total": bytes_total,
                        "attempt": attempt,
                        "torrent_name": torrent_name,
                    }
                )
                time.sleep(2)

            ok = download_result[0] if download_result else False
            if ok:
                bytes_total = _dir_size(DOWNLOAD_DIR)
                _set_progress(download={"bytes_done": bytes_total, "bytes_total": bytes_total})
            else:
                # Preserve error set by download (e.g. stall); only set default if none
                with _progress_lock:
                    existing = (_progress.get("download") or {}).get("error")
                if not existing:
                    _set_progress(download={"error": "No start in timeout"})

            _log(f"Downloaded: {ok}")
            if ok:
                video_path = _find_latest_video(DOWNLOAD_DIR)
                if not video_path:
                    fail("No video file found in downloads dir.")
                    return
                if MIN_FREE_DISK_GB > 0 and not _check_disk_space(DOWNLOAD_DIR, MIN_FREE_DISK_GB):
                    fail(f"Insufficient disk space before upload (need at least {MIN_FREE_DISK_GB} GB free).")
                    return
                if job_id:
                    _call_webhook(job_id, "download-done")

                if BACKEND_URL and STAGING_SERVICE_TOKEN:
                    _set_progress(phase="uploading", upload={"bytes_sent": 0, "bytes_total": os.path.getsize(video_path), "percent": 0, "phase": "uploading", "error": None})
                    _log("phase=uploading")
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
                        _set_progress(phase="done", upload={"phase": "done", "percent": 100})
                        _log("phase=done")
                        if job_id:
                            _call_webhook(job_id, "upload-done", stagingId=staging_id)
                        _clear_after_upload(video_path, DOWNLOAD_DIR)
                    else:
                        _set_progress(phase="failed", upload={"phase": "error", "error": "Staging upload failed"})
                        fail("Staging upload failed.")
                    return
                else:
                    _set_progress(phase="done")
                    _log("phase=done (no staging configured)")
                    if job_id:
                        _call_webhook(job_id, "upload-done")
                    return
            _log(f"No start in {START_TIMEOUT_MIN} min, trying next.")
        fail(f"Failed after {MAX_ATTEMPTS} attempts.")
    except Exception as e:
        _log(f"phase=failed error={e}")
        _log(traceback.format_exc())
        fail(f"Error: {e}")
    finally:
        # Keep state for a bit so backend can read "done" or "failed"; caller can clear later
        pass


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

