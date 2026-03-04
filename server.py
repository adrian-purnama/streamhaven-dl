"""
Sniffer HTTP server: GET /status, GET /process. Can connect to MongoDB for later DB use.
Loop: GET /download starts the worker loop (picks next waiting job from DB, processes, repeats).
Run: python sniffer_server.py [--port 3503]
"""
import os
import sys
import time
from threading import Lock, Thread
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError

# Load .env
try:
    from dotenv import load_dotenv
    _dir = os.path.dirname(os.path.abspath(__file__))
    for _p in [os.path.join(_dir, ".env"), ".env", os.path.join(_dir, "..", ".env")]:
        if load_dotenv(_p):
            break
    load_dotenv()
except ImportError:
    pass

from bson import ObjectId
from flask import Flask, jsonify, request
from gridfs import GridFSBucket

# Sniffer logic (run_sniffer, download_m3u8_with_ffmpeg, URL, DOWNLOADS_DIR)
from sniffer import run_sniffer, download_m3u8_with_ffmpeg, DOWNLOADS_DIR, URL
# Shared process state (phase, snifferResult)
from process_state import get_state, set_state

# -----------------------------------------------------------------------------
# MongoDB (optional). Set MONGO_URI or MONGODB_URI to enable.
# -----------------------------------------------------------------------------

MONGO_URI = (os.environ.get("MONGODB_URI") or os.environ.get("MONGO_URI") or "").strip()
MONGO_DB_NAME = 'app'
# MONGO_DB_NAME = (os.environ.get("MONGODB_DATABASE") or os.environ.get("MONGO_DB_NAME") or "").strip()
_mongo_client = None
_mongo_db = None


def _get_mongo_status() -> str:
    """Return 'connected' | 'disconnected' | 'not_configured'."""
    if not MONGO_URI:
        return "not_configured"
    global _mongo_client, _mongo_db
    try:
        if _mongo_client is None:
            try:
                from pymongo import MongoClient
            except ImportError:
                return "not_configured"  # pymongo not installed
            _mongo_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=10000)
            _mongo_db = _mongo_client.get_database(MONGO_DB_NAME) if MONGO_DB_NAME else _mongo_client.get_database()
        _mongo_client.admin.command("ping")
        return "connected"
    except Exception:
        return "disconnected"


def get_db():
    """Return MongoDB database instance or None. Call after _get_mongo_status() == 'connected'."""
    _get_mongo_status()
    return _mongo_db


# -----------------------------------------------------------------------------
# Run lock: only one sniffer run at a time (process_state is in process_state.py)
# Loop: only one worker loop running (picks next waiting job from DB, processes, repeats)
# -----------------------------------------------------------------------------

_run_lock = Lock()
_loop_lock = Lock()
_loop_running = False

# Mongoose default collections for DownloadQueue and DownloadSeriesQueue
DOWNLOAD_QUEUE_COLLECTION = os.environ.get("DOWNLOAD_QUEUE_COLLECTION", "downloadqueues")
DOWNLOAD_SERIES_QUEUE_COLLECTION = os.environ.get("DOWNLOAD_SERIES_QUEUE_COLLECTION", "downloadseriesqueues")
# GridFS bucket and StagingVideo collection (same as backend)
GRIDFS_BUCKET = "stagingVideos"
STAGING_VIDEO_COLLECTION = "stagingvideos"
# Subtitle staging (same as backend stagingSubtitle.model.js + subtitleGridFs.model.js)
SUBTITLE_GRIDFS_BUCKET = "stagingSubtitles"
STAGING_SUBTITLE_COLLECTION = "stagingsubtitles"
UPLOADED_VIDEO_COLLECTION = "uploadedvideos"
BACKEND_URL = (os.environ.get("BACKEND_URL") or "").strip().rstrip("/")
WEBHOOK_SECRET = (os.environ.get("WEBHOOK_SECRET") or "").strip()

# -----------------------------------------------------------------------------
# Flask app
# -----------------------------------------------------------------------------

app = Flask(__name__)


@app.route("/status", methods=["GET"])
def status():
    """GET /status -> ok, full process state, and mongodb status."""
    mongodb = _get_mongo_status()
    st = get_state()
    return jsonify({
        "ok": True,
        **st,
        "mongodb": mongodb,
    })


def _sanitize_filename(name: str) -> str:
    """Replace invalid filename chars. Returns safe basename."""
    invalid = '<>:"/\\|?*'
    for c in invalid:
        name = name.replace(c, "_")
    return name.strip() or "download"


class _ProgressFileWrapper:
    """File-like wrapper that reports upload progress to process_state (bytes_sent, bytes_total, percent)."""
    def __init__(self, file_obj, total_bytes: int, on_progress):
        self._file = file_obj
        self._total = total_bytes
        self._sent = 0
        self._on_progress = on_progress
        self._last_percent = -1

    def read(self, size: int = -1):
        data = self._file.read(size)
        self._sent += len(data)
        if self._total and self._on_progress:
            percent = round(100 * self._sent / self._total)
            if percent != self._last_percent:
                self._last_percent = percent
                self._on_progress(self._sent, self._total)
        return data


def upload_to_staging(file_path: str, filename: str, doc: dict) -> tuple[ObjectId | None, str | None]:
    """
    Upload mp4 to GridFS (stagingVideos.files, stagingVideos.chunks) and create StagingVideo doc.
    Reports upload progress via set_state(upload={bytes_sent, bytes_total, percent}) for UI progress bar.
    Only updates the 'upload' key; never touches 'download' so download progress (e.g. page bar) stays visible.
    Returns (staging_id, None) on success or (None, error_message) on failure.
    """
    db = get_db()
    if db is None:
        return (None, "Database not connected")
    if not os.path.isfile(file_path):
        return (None, "File not found")
    try:
        bucket = GridFSBucket(db, bucket_name=GRIDFS_BUCKET)
        size = os.path.getsize(file_path)
        # Only set upload state; do not pass download= so existing download (e.g. current_page/total_pages) is preserved
        set_state(upload={"bytes_sent": 0, "bytes_total": size, "percent": 0})

        def on_progress(sent: int, total: int) -> None:
            set_state(upload={"bytes_sent": sent, "bytes_total": total, "percent": round(100 * sent / total) if total else 0})

        with open(file_path, "rb") as f:
            wrapper = _ProgressFileWrapper(f, size, on_progress)
            file_id = bucket.upload_from_stream(filename, wrapper, metadata={"contentType": "video/mp4"})
        staging_coll = db[STAGING_VIDEO_COLLECTION]
        staging_doc = {
            "gridFsFileId": file_id,
            "filename": filename,
            "size": size,
            "contentType": "video/mp4",
            "tmdbId": doc.get("tmdbId"),
            "imdbId": None,
            "poster_path": doc.get("poster_path"),
            "title": doc.get("title") or "",
            "status": "pending",
        }
        if doc.get("mediaType") == "tv":
            staging_doc["mediaType"] = "tv"
            if doc.get("seasonNumber") is not None:
                staging_doc["seasonNumber"] = doc["seasonNumber"]
            if doc.get("episodeNumber") is not None:
                staging_doc["episodeNumber"] = doc["episodeNumber"]
        r = staging_coll.insert_one(staging_doc)
        return (r.inserted_id, None)
    except Exception as e:
        return (None, str(e))


def _content_type_for_subtitle(filename: str) -> str:
    """Return MIME type for subtitle extension."""
    ext = os.path.splitext(filename)[1].lower()
    if ext == ".srt":
        return "application/x-subrip"
    if ext == ".vtt":
        return "text/vtt"
    if ext in (".ass", ".ssa"):
        return "text/x-ssa"
    return "application/x-subrip"


def upload_subtitle_to_staging(
    file_path: str, filename: str, tmdb_id: int, language: str
) -> tuple[ObjectId | None, str | None]:
    """
    Upload subtitle file to GridFS (stagingSubtitles bucket) and create StagingSubtitle doc.
    Returns (staging_subtitle_id, None) on success or (None, error_message) on failure.
    """
    db = get_db()
    if db is None:
        return (None, "Database not connected")
    if not os.path.isfile(file_path):
        return (None, "File not found")
    try:
        bucket = GridFSBucket(db, bucket_name=SUBTITLE_GRIDFS_BUCKET)
        size = os.path.getsize(file_path)
        content_type = _content_type_for_subtitle(filename)
        with open(file_path, "rb") as f:
            file_id = bucket.upload_from_stream(
                filename, f, metadata={"contentType": content_type}
            )
        coll = db[STAGING_SUBTITLE_COLLECTION]
        staging_doc = {
            "gridFsFileId": file_id,
            "filename": filename,
            "size": size,
            "contentType": content_type,
            "language": language,
            "tmdbId": tmdb_id,
            "stagingVideoId": None,
            "status": "pending",
        }
        r = coll.insert_one(staging_doc)
        return (r.inserted_id, None)
    except Exception as e:
        return (None, str(e))


def _watch_url_for_job(tmdb_id: int, media_type: str | None = None, season: int | None = None, episode: int | None = None) -> str:
    """
    Build vidsrc-style embed URL from TMDB id and, for TV episodes, season/episode.

    Defaults:
    - Movies: https://vidsrcme.ru/embed/movie/3332
    - Series: https://vidsrcme.ru/embed/tv?tmdb=1&season=1&episode=1

    Override with:
    - SNIFFER_MOVIE_BASE_URL (e.g. https://vidsrcme.ru/embed/movie)
    - SNIFFER_TV_BASE_URL    (e.g. https://vidsrcme.ru/embed/tv)
    """
    if tmdb_id is None:
        return ""
    media = (media_type or "").lower()
    if media == "tv":
        base_tv = os.environ.get("SNIFFER_TV_BASE_URL", "https://vidsrcme.ru/embed/tv").rstrip("/")
        if season is None or episode is None:
            return ""
        return f"{base_tv}?tmdb={tmdb_id}&season={season}&episode={episode}"
    # default: movie
    base_movie = os.environ.get("SNIFFER_MOVIE_BASE_URL", "https://vidsrcme.ru/embed/movie").rstrip("/")
    return f"{base_movie}/{tmdb_id}"


def _get_queue_coll():
    """Download queue collection (same DB as backend Mongoose 'downloadqueues')."""
    db = get_db()
    if db is None:
        return None
    return db[DOWNLOAD_QUEUE_COLLECTION]


def _get_series_queue_coll():
    """DownloadSeriesQueue collection (per-episode TV jobs)."""
    db = get_db()
    if db is None:
        return None
    return db[DOWNLOAD_SERIES_QUEUE_COLLECTION]


def get_next_waiting():
    """
    Atomically claim the next waiting job (oldest by createdAt).
    Looks at:
      - DownloadQueue (movies, mediaType='movie')
      - DownloadSeriesQueue (per-episode TV jobs)
    Returns a doc with a private field "__coll" indicating which collection it came from.
    """
    print("[sniffer_server] getting next waiting job")
    db = get_db()
    if db is None:
        return None

    movie_coll = _get_queue_coll()
    series_coll = _get_series_queue_coll()
    if movie_coll is None and series_coll is None:
        return None

    movie_doc = None
    series_doc = None

    if movie_coll is not None:
        movie_doc = movie_coll.find_one(
            {"status": "waiting", "mediaType": "movie"},
            sort=[("createdAt", 1)],
        )
    if series_coll is not None:
        series_doc = series_coll.find_one(
            {"status": "waiting"},
            sort=[("createdAt", 1)],
        )

    chosen_doc = None
    chosen_coll = None
    if movie_doc and series_doc:
        # Choose the oldest by createdAt
        if movie_doc.get("createdAt") <= series_doc.get("createdAt"):
            chosen_doc, chosen_coll = movie_doc, movie_coll
        else:
            chosen_doc, chosen_coll = series_doc, series_coll
    elif movie_doc:
        chosen_doc, chosen_coll = movie_doc, movie_coll
    elif series_doc:
        chosen_doc, chosen_coll = series_doc, series_coll
    else:
        return None

    r = chosen_coll.update_one(
        {"_id": chosen_doc["_id"], "status": "waiting"},
        {"$set": {"status": "searching"}},
    )
    if r.modified_count == 0:
        return None

    # Mark which collection this doc came from so _process_one_job can update the right place
    chosen_doc["__coll"] = chosen_coll.name
    return chosen_doc


def _process_one_job(doc) -> dict:
    """Run sniffer for one queue doc (already claimed as searching). Step 1: get m3u8 link. Step 2: run ffmpeg."""
    db = get_db()
    coll = None
    if db is not None:
        coll_name = doc.get("__coll") or DOWNLOAD_QUEUE_COLLECTION
        coll = db[coll_name]
    doc_id = doc["_id"]
    # For TV episodes, tmdbId and mediaType live on the parent DownloadQueue doc.
    tmdb_id = doc.get("tmdbId")
    media_type = (doc.get("mediaType") or "").lower()
    season = doc.get("seasonNumber")
    episode = doc.get("episodeNumber")

    parent = None
    # Heuristic: series episodes live in DownloadSeriesQueue and have parentId + seasonNumber + episodeNumber.
    if (tmdb_id is None or media_type != "movie") and db is not None and "parentId" in doc:
        parent_id = doc.get("parentId")
        try:
            # parentId is an ObjectId in MongoDB; trust it as-is.
            parent_coll = db[DOWNLOAD_QUEUE_COLLECTION]
            parent = parent_coll.find_one({"_id": parent_id})
        except Exception:
            parent = None
        if parent:
            tmdb_id = tmdb_id or parent.get("tmdbId")
            media_type = (parent.get("mediaType") or "tv").lower()

    quality = (doc.get("quality") or "high").strip().lower()
    if quality == "medium":
        quality = "med"

    url = doc.get("url")
    if not url and tmdb_id is not None:
        url = _watch_url_for_job(tmdb_id, media_type=media_type, season=season, episode=episode)
    if not url:
        set_state(phase="idle")
        if coll is not None:
            coll.update_one(
                {"_id": doc_id},
                {"$set": {"status": "failed", "errorMessage": "Missing tmdbId or url"}},
            )
        return {"success": False, "message": "Missing tmdbId or url", "url": None}

    # Step 1: get m3u8 link (no ffmpeg)
    result = run_sniffer(url, log=True, preferred_quality=quality)
    m3u8_link = result.get("m3u8_link")
    if not result.get("success") or not m3u8_link:
        set_state(phase="idle", snifferResult=result)
        if coll is not None:
            coll.update_one(
                {"_id": doc_id},
                {"$set": {"status": "failed", "errorMessage": result.get("error") or "Failed to get m3u8 link"}},
            )
        return {"success": False, "message": result.get("error") or "Failed to get m3u8 link", "url": url}

    # Step 2: run ffmpeg to download
    os.makedirs(DOWNLOADS_DIR, exist_ok=True)
    title = doc.get("title") or "download"
    safe_name = _sanitize_filename(str(title)) + ".mp4"
    output_mp4 = os.path.join(DOWNLOADS_DIR, safe_name)
    set_state(phase="downloading", explanation="[9/9] Downloading with ffmpeg -> " + output_mp4)
    if coll is not None:
        coll.update_one({"_id": doc_id}, {"$set": {"status": "downloading", "errorMessage": None}})
    ok, ffmpeg_err = download_m3u8_with_ffmpeg(m3u8_link, output_mp4, timeout_sec=3600, log=True)
    if not ok:
        result["success"] = False
        result["status"] = "ffmpeg_failed"
        result["error"] = ffmpeg_err or "ffmpeg download failed"
        if coll is not None:
            coll.update_one(
                {"_id": doc_id},
                {"$set": {"status": "failed", "errorMessage": result["error"]}},
            )
        set_state(phase="failed", snifferResult=result, explanation=result["error"])
        try:
            if os.path.isfile(output_mp4):
                os.remove(output_mp4)
                print("[sniffer_server] deleted partial download on ffmpeg failure:", output_mp4)
        except OSError as e:
            print("[sniffer_server] failed to delete partial download:", e)
        set_state(phase="idle", snifferResult=result)
        return {"success": False, "message": result["error"], "url": url}

    result["output_path"] = output_mp4
    if coll is not None:
        coll.update_one({"_id": doc_id}, {"$set": {"status": "uploading", "errorMessage": None}})
    # Build staging meta: for TV, tmdbId/poster_path come from parent; include mediaType/season/episode for series.
    staging_meta = {
        "tmdbId": tmdb_id,
        "title": doc.get("title") or "download",
        "poster_path": parent.get("poster_path") if parent else doc.get("poster_path"),
    }
    if media_type == "tv":
        staging_meta["mediaType"] = "tv"
        staging_meta["seasonNumber"] = season
        staging_meta["episodeNumber"] = episode
    set_state(phase="uploading", explanation="[10/10] Uploading to staging")
    staging_id, upload_err = upload_to_staging(output_mp4, safe_name, staging_meta)
    if staging_id is not None:
        result["success"] = True
        result["status"] = "ok"
        result["error"] = None
        if coll is not None:
            coll.update_one({"_id": doc_id}, {"$set": {"status": "done", "stagingId": str(staging_id), "errorMessage": None}})
        # delete everything in downloads dir after done
        try:
            for f in os.listdir(DOWNLOADS_DIR):
                p = os.path.join(DOWNLOADS_DIR, f)
                if os.path.isfile(p):
                    os.remove(p)
        except OSError as e:
            print("[sniffer_server] cleanup downloads dir failed:", e)
    else:
        result["success"] = False
        result["status"] = "upload_failed"
        result["error"] = upload_err or "Upload to staging failed"
        if coll is not None:
            coll.update_one({"_id": doc_id}, {"$set": {"status": "failed", "errorMessage": upload_err or "Upload failed"}})
    set_state(phase="idle", snifferResult=result, explanation="Previous Sniffer pipeline completed")
    return {"success": result["success"], "message": "Done" if staging_id else (result["error"] or "Failed"), "stagingId": str(staging_id) if staging_id else None, "url": url}


def _worker_loop():
    """Process jobs until none waiting. Sleep 5 sec, check again. If still none, exit (wait for next ping)."""
    global _loop_running
    while _loop_running:
        doc = get_next_waiting()
        if doc:
            _process_one_job(doc)
            continue
        # no job - sleep 5 sec, check once more
        time.sleep(5)
        doc = get_next_waiting()
        if doc:
            _process_one_job(doc)
            continue
        # still no job - exit
        break
    with _loop_lock:
        _loop_running = False


@app.route("/download", methods=["GET"])
def download():
    """GET /download -> start the worker loop (picks next waiting job from DB, processes, repeats)."""
    global _loop_running
    with _loop_lock:
        if _loop_running:
            return jsonify({"success": False, "message": "Loop already running"}), 503
        if _get_mongo_status() != "connected":
            return jsonify({"success": False, "message": "MongoDB not connected"}), 503
        _loop_running = True
    Thread(target=_worker_loop, daemon=True).start()
    return jsonify({"success": True, "message": "Loop started"})


def _subtitle_already_exists(tmdb_id: int, language: str) -> bool:
    """Return True if this language is already on the uploaded video or in staging subtitles."""
    db = get_db()
    if db is None:
        return False
    lang = (language or "").strip()
    if not lang:
        return False
    # 1) Uploaded video: subtitle.downloadedSubtitles is [String] of language codes
    uv = db[UPLOADED_VIDEO_COLLECTION].find_one({"externalId": tmdb_id})
    if uv:
        sub_obj = uv.get("subtitle") or {}
        if isinstance(sub_obj, dict):
            downloaded = sub_obj.get("downloadedSubtitles") or []
            if lang in downloaded:
                return True
        elif isinstance(sub_obj, list) and lang in sub_obj:
            return True
    # 2) Staging subtitle: already have a doc for this tmdbId + language
    existing = db[STAGING_SUBTITLE_COLLECTION].find_one({"tmdbId": tmdb_id, "language": lang})
    if existing:
        return True
    return False


def _call_process_subtitle_webhook(tmdb_id: int, language: str) -> None:
    """Fire-and-forget: notify backend that subtitle was uploaded to staging (add to uploaded video)."""
    if not BACKEND_URL:
        return
    url = f"{BACKEND_URL}/api/languages/process-subtitle?externalId={tmdb_id}&language={language}"
    headers = {}
    if WEBHOOK_SECRET:
        headers["X-Webhook-Secret"] = WEBHOOK_SECRET

    def _do():
        try:
            req = Request(url, headers=headers, method="GET")
            urlopen(req, timeout=10)
        except (URLError, HTTPError, OSError) as e:
            print(f"[download-subtitle] webhook call failed: {e}", flush=True)

    t = Thread(target=_do, daemon=True)
    t.start()


@app.route("/available-subtitles", methods=["GET"])
def available_subtitles():
    """
    GET /available-subtitles?title=Movie+Name+2024
    Search indexsubtitle.cc, return available languages for the best-matching title.
    Response: { success, data: { languages: [{short, long, count}], totalSubtitles } }
    """
    from subtitle import (
        SEARCH_URL, HEADERS as SUB_HEADERS, BASE_URL as SUB_BASE,
        _pick_best, _extract_datatable_json, get_available_languages,
    )
    import requests as sub_requests

    title = (request.args.get("title") or "").strip()
    if not title:
        return jsonify({"success": False, "message": "title query param required"}), 400

    try:
        resp = sub_requests.post(SEARCH_URL, data={"query": title}, headers=SUB_HEADERS, timeout=15)
        resp.raise_for_status()
        try:
            results = resp.json()
        except ValueError as e:
            return jsonify({"success": False, "message": f"Search returned non-JSON: {e}"}), 500
        match = _pick_best(results, title)
        if not match:
            return jsonify({"success": True, "data": {"languages": [], "totalSubtitles": 0}})
        detail_url = SUB_BASE + match["url"]
        detail = sub_requests.get(detail_url, headers=SUB_HEADERS, timeout=15)
        detail.raise_for_status()
        subs = _extract_datatable_json(detail.text)
        langs = get_available_languages(subs)
        return jsonify({
            "success": True,
            "data": {"languages": langs, "totalSubtitles": len(subs)},
        })
    except Exception as e:
        return jsonify({"success": False, "message": str(e)}), 500


@app.route("/download-subtitle", methods=["POST"])
def download_subtitle_route():
    """
    POST /download-subtitle  Body: { externalId, language (ISO 639-1 e.g. "en"), title }
    Searches indexsubtitle.cc, picks a subtitle for the language, downloads it,
    uploads to GridFS staging, and calls the backend webhook.
    """
    from subtitle import (
        SEARCH_URL, HEADERS as SUB_HEADERS, BASE_URL as SUB_BASE,
        _pick_best, _extract_datatable_json, short_to_long,
        download_subtitle as sub_download,
    )
    import requests as sub_requests

    if _get_mongo_status() != "connected":
        return jsonify({"success": False, "message": "MongoDB not connected"}), 503
    data = request.get_json() or {}
    external_id = data.get("externalId")
    language = (data.get("language") or "").strip()
    title = (data.get("title") or "").strip()
    if external_id is None or not language:
        return jsonify({"success": False, "message": "externalId and language are required"}), 400
    if not title:
        return jsonify({"success": False, "message": "title is required"}), 400
    try:
        tmdb_id = int(external_id)
    except (TypeError, ValueError):
        return jsonify({"success": False, "message": "externalId must be a number"}), 400

    if _subtitle_already_exists(tmdb_id, language):
        _call_process_subtitle_webhook(tmdb_id, language)
        return jsonify({"success": True, "message": "Subtitle already exists"}), 200

    _subtitle_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "subtitle_downloads")
    os.makedirs(_subtitle_dir, exist_ok=True)

    try:
        # 1) Search (same headers as Postman: Referer, Origin, X-Requested-With – no cookies)
        resp = sub_requests.post(SEARCH_URL, data={"query": title}, headers=SUB_HEADERS, timeout=15)
        resp.raise_for_status()
        try:
            results = resp.json()
        except ValueError as e:
            return jsonify({"success": False, "message": f"Search returned non-JSON: {e}"}), 500
        match = _pick_best(results, title)
        if not match:
            return jsonify({"success": False, "message": "Subtitle not found"}), 404

        # 2) Detail page -> get all subtitles
        detail_url = SUB_BASE + match["url"]
        detail = sub_requests.get(detail_url, headers=SUB_HEADERS, timeout=15)
        detail.raise_for_status()
        subs = _extract_datatable_json(detail.text)
        lang_long = short_to_long(language)
        filtered = [s for s in subs if (s.get("language") or "").strip().lower() == lang_long]
        if not filtered:
            return jsonify({"success": False, "message": f"No subtitle for language '{language}'"}), 404

        # 3) Download: try each subtitle until we get .srt or .vtt (skip .sub/.ass/.ssa-only)
        filepath = None
        for pick in filtered:
            filepath = sub_download(pick, _subtitle_dir)
            if filepath and os.path.isfile(filepath):
                break
        if not filepath or not os.path.isfile(filepath):
            return jsonify({"success": False, "message": "Download failed (no .srt/.vtt found for this language)"}), 500

        # 4) Upload to GridFS staging
        filename = os.path.basename(filepath)
        sid, err = upload_subtitle_to_staging(filepath, filename, tmdb_id, language)
        if sid is None:
            return jsonify({"success": False, "message": err or "Upload to staging failed"}), 500

        # 5) Cleanup
        try:
            os.remove(filepath)
        except OSError:
            pass

        # 6) Notify backend webhook
        _call_process_subtitle_webhook(tmdb_id, language)
        return jsonify({
            "success": True,
            "message": "Subtitle downloaded and staged",
            "stagingSubtitleIds": [str(sid)],
        })
    except Exception as e:
        return jsonify({"success": False, "message": str(e)}), 500


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"ok": True})


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------

def main():
    port = int(os.environ.get("SNIFFER_PORT", "3502"))
    for i, arg in enumerate(sys.argv):
        if arg == "--port" and i + 1 < len(sys.argv):
            port = int(sys.argv[i + 1])
            break
    if MONGO_URI:
        print("[sniffer_server] waiting for mongodb...")
        while _get_mongo_status() != "connected":
            time.sleep(2)
        print("[sniffer_server] mongodb connected")
    db_name = (_mongo_db.name if _mongo_db is not None else None) or MONGO_DB_NAME or "(not connected)"
    print("[sniffer_server] port=%s | mongodb=%s | db=%s" % (port, _get_mongo_status(), db_name))
    app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)


if __name__ == "__main__":
    main()
