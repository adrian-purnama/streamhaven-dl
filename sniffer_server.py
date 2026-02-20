"""
Sniffer HTTP server: GET /status, GET /process. Can connect to MongoDB for later DB use.
Loop: GET /download starts the worker loop (picks next waiting job from DB, processes, repeats).
Run: python sniffer_server.py [--port 3503]
"""
import os
import sys
import time
from threading import Lock, Thread

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

# Mongoose default collection for DownloadQueue is "downloadqueues"
DOWNLOAD_QUEUE_COLLECTION = os.environ.get("DOWNLOAD_QUEUE_COLLECTION", "downloadqueues")
# GridFS bucket and StagingVideo collection (same as backend)
GRIDFS_BUCKET = "stagingVideos"
STAGING_VIDEO_COLLECTION = "stagingvideos"

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
        r = staging_coll.insert_one(staging_doc)
        return (r.inserted_id, None)
    except Exception as e:
        return (None, str(e))


def _watch_url_from_tmdb(tmdb_id) -> str:
    """Build vidsrc-style embed URL from TMDB id. Override with env SNIFFER_BASE_URL."""
    base = os.environ.get("SNIFFER_BASE_URL", "https://vidsrc.xyz/embed/movie/").rstrip("/")
    return f"{base}/{tmdb_id}"


def _get_queue_coll():
    """Download queue collection (same DB as backend Mongoose 'downloadqueues')."""
    db = get_db()
    if db is None:
        return None
    return db[DOWNLOAD_QUEUE_COLLECTION]


def get_next_waiting():
    """Atomically claim the next waiting job (oldest by createdAt). Returns doc or None."""
    print("[sniffer_server] getting next waiting job")
    coll = _get_queue_coll()
    if coll is None:
        return None
    doc = coll.find_one({"status": "waiting"}, sort=[("createdAt", 1)])
    if not doc:
        return None
    r = coll.update_one(
        {"_id": doc["_id"], "status": "waiting"},
        {"$set": {"status": "searching"}},
    )
    if r.modified_count == 0:
        return None
    return doc


def _process_one_job(doc) -> dict:
    """Run sniffer for one queue doc (already claimed as searching). Step 1: get m3u8 link. Step 2: run ffmpeg."""
    coll = _get_queue_coll()
    doc_id = doc["_id"]
    tmdb_id = doc.get("tmdbId")
    quality = (doc.get("quality") or "high").strip().lower()
    if quality == "medium":
        quality = "med"

    url = doc.get("url")
    if not url and tmdb_id is not None:
        url = _watch_url_from_tmdb(tmdb_id)
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
    ok = download_m3u8_with_ffmpeg(m3u8_link, output_mp4, timeout_sec=3600, log=True)
    if not ok:
        result["success"] = False
        result["status"] = "ffmpeg_failed"
        result["error"] = "ffmpeg download failed"
        if coll is not None:
            coll.update_one({"_id": doc_id}, {"$set": {"status": "failed", "errorMessage": "ffmpeg failed"}})
        set_state(phase="failed", snifferResult=result, explanation="ffmpeg failed")
        try:
            if os.path.isfile(output_mp4):
                os.remove(output_mp4)
                print("[sniffer_server] deleted partial download on ffmpeg failure:", output_mp4)
        except OSError as e:
            print("[sniffer_server] failed to delete partial download:", e)
        set_state(phase="idle", snifferResult=result)
        return {"success": False, "message": "ffmpeg failed", "url": url}

    result["output_path"] = output_mp4
    if coll is not None:
        coll.update_one({"_id": doc_id}, {"$set": {"status": "uploading", "errorMessage": None}})
    set_state(phase="uploading", explanation="[10/10] Uploading to staging")
    staging_id, upload_err = upload_to_staging(output_mp4, safe_name, doc)
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
