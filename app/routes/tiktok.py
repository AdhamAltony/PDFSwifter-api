import asyncio
import os
import time

from fastapi import APIRouter

from app.config import DOWNLOAD_FOLDER
from app.config import DOWNLOAD_RETENTION_SECONDS
from app.downloaders.common import download_video
from app.services.download_tracker import DOWNLOAD_TRACKER
from app.utils.file_ops import delete_file_later

router = APIRouter(prefix="/tiktok", tags=["TikTok"])


@router.post("/download")
async def request_tiktok_download(url: str):
    """Kick off a TikTok download and return a process identifier."""
    job = DOWNLOAD_TRACKER.create_job(source="tiktok", url=url)

    output_template = os.path.join(
        DOWNLOAD_FOLDER, "tiktok_%(id)s_%(upload_date)s_%(timestamp)s.%(ext)s"
    )
    custom_options = {
        "retries": 5,
        "fragment_retries": 5,
        "skip_unavailable_fragments": True,
        "extractor_retries": 3,
        "http_headers": {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            "Referer": "https://www.tiktok.com/",
        },
    }

    async def runner():
        DOWNLOAD_TRACKER.update_job(job.process_id, status="running", progress=0.0)

        last_update_time = 0.0
        last_progress_bucket = -1
        last_bytes_reported = 0

        def hook(data):
            nonlocal last_update_time, last_progress_bucket, last_bytes_reported
            status = data.get("status")
            if status == "downloading":
                downloaded = int(data.get("downloaded_bytes") or 0)
                total = data.get("total_bytes") or data.get("total_bytes_estimate")
                progress = (
                    (downloaded / total) * 100 if total and total > 0 else 0.0
                )

                now = time.monotonic()
                progress_bucket = int(progress)
                should_emit = False
                if progress_bucket != last_progress_bucket:
                    should_emit = True
                elif downloaded - last_bytes_reported >= 1024 * 1024:
                    should_emit = True
                elif now - last_update_time >= 0.75:
                    should_emit = True

                if not should_emit:
                    return

                last_update_time = now
                last_progress_bucket = progress_bucket
                last_bytes_reported = downloaded

                DOWNLOAD_TRACKER.update_job(
                    job.process_id,
                    bytes_downloaded=downloaded,
                    total_bytes=int(total) if total else None,
                    progress=progress,
                )
            elif status == "finished":
                DOWNLOAD_TRACKER.update_job(job.process_id, progress=100.0)

        try:
            filename = await asyncio.to_thread(
                download_video,
                url,
                output_template,
                custom_options,
                hook,
            )
        except Exception as exc:
            message = str(exc).replace("\n", " ").strip()
            DOWNLOAD_TRACKER.update_job(
                job.process_id, status="failed", error=message
            )
            return

        DOWNLOAD_TRACKER.update_job(
            job.process_id,
            status="completed",
            progress=100.0,
            file_path=filename,
            suggested_name=os.path.basename(filename),
        )
        delete_file_later(filename, delay=DOWNLOAD_RETENTION_SECONDS)

    asyncio.create_task(runner())
    return {"process_id": job.process_id}
