import os
from pathlib import Path


def _env_int(name: str, default: int) -> int:
    value = os.environ.get(name)
    if value is None or value == "":
        return default
    try:
        return int(value)
    except ValueError:
        return default


def _env_bool(name: str, default: bool) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "t", "yes", "y", "on"}


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _default_data_root() -> Path:
    configured = os.getenv("DATA_ROOT")
    if configured:
        return Path(configured)

    docker_root = Path("/data")
    try:
        if docker_root.exists() and os.access(str(docker_root), os.W_OK):
            return docker_root
    except Exception:
        pass

    return PROJECT_ROOT


DATA_ROOT = _default_data_root()

DOWNLOAD_FOLDER = Path(os.getenv("DOWNLOAD_FOLDER", str(DATA_ROOT / "downloads")))
PDF_DOWNLOAD_FOLDER = Path(os.getenv("PDF_DOWNLOAD_FOLDER", str(DATA_ROOT / "pdf_uploads")))
EXCEL_DOWNLOAD_FOLDER = Path(os.getenv("EXCEL_DOWNLOAD_FOLDER", str(DATA_ROOT / "excel_outputs")))
WORD_DOWNLOAD_FOLDER = Path(os.getenv("WORD_DOWNLOAD_FOLDER", str(DATA_ROOT / "word_outputs")))
IMAGE_DOWNLOAD_FOLDER = Path(os.getenv("IMAGE_DOWNLOAD_FOLDER", str(DATA_ROOT / "image_outputs")))


for folder in (
    DOWNLOAD_FOLDER,
    PDF_DOWNLOAD_FOLDER,
    EXCEL_DOWNLOAD_FOLDER,
    WORD_DOWNLOAD_FOLDER,
    IMAGE_DOWNLOAD_FOLDER,
):
    folder.mkdir(parents=True, exist_ok=True)

CHUNK_SIZE = 1024 * 1024  # 1MB
YOUTUBE_REMOTE_ENDPOINT = os.environ.get("YOUTUBE_REMOTE_ENDPOINT")
YOUTUBE_COOKIES_PATH = os.environ.get("YOUTUBE_COOKIES_PATH")

REDIS_URL = os.environ.get("REDIS_URL")

# Retention / cleanup
# - *_RETENTION_SECONDS controls how long files stay on disk.
# - CLEANUP_INTERVAL_SECONDS controls how often the background sweeper runs.
# - Set CLEANUP_ENABLED=false to disable the sweeper entirely.
DOWNLOAD_RETENTION_SECONDS = _env_int("DOWNLOAD_RETENTION_SECONDS", 600)
UPLOAD_RETENTION_SECONDS = _env_int("UPLOAD_RETENTION_SECONDS", 300)
CLEANUP_INTERVAL_SECONDS = _env_int("CLEANUP_INTERVAL_SECONDS", 300)
CLEANUP_ENABLED = _env_bool("CLEANUP_ENABLED", True)

# Folder-specific retention policy (seconds).
RETENTION_BY_FOLDER = {
    DOWNLOAD_FOLDER: DOWNLOAD_RETENTION_SECONDS,
    EXCEL_DOWNLOAD_FOLDER: DOWNLOAD_RETENTION_SECONDS,
    WORD_DOWNLOAD_FOLDER: DOWNLOAD_RETENTION_SECONDS,
    IMAGE_DOWNLOAD_FOLDER: DOWNLOAD_RETENTION_SECONDS,
    PDF_DOWNLOAD_FOLDER: UPLOAD_RETENTION_SECONDS,
}
