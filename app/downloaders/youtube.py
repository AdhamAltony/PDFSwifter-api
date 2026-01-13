import asyncio
import os
import time
import uuid
from abc import ABC, abstractmethod
from typing import Optional
from urllib.parse import unquote

import httpx

from app.config import (
    DOWNLOAD_FOLDER,
    DOWNLOAD_RETENTION_SECONDS,
    YOUTUBE_COOKIES_PATH,
    YOUTUBE_REMOTE_ENDPOINT,
)
from app.downloaders.common import download_video
from app.services.download_tracker import DOWNLOAD_TRACKER
from app.utils.file_ops import delete_file_later


def extract_filename_from_disposition(content_disposition: str) -> Optional[str]:
    """Return filename value from a Content-Disposition header if present."""
    if not content_disposition:
        return None

    parts = content_disposition.split(";")
    filename = None
    for part in parts:
        part = part.strip()
        if part.lower().startswith("filename*="):
            value = part.split("=", 1)[1].strip().strip('"')
            _, _, encoded = value.partition("''")
            filename = unquote(encoded or value)
            break
        if part.lower().startswith("filename="):
            filename = part.split("=", 1)[1].strip().strip('"')
            break

    return filename


def build_youtube_download_options() -> dict:
    """Return yt-dlp options for authenticated YouTube downloads when configured."""
    if not YOUTUBE_COOKIES_PATH:
        return {}
    cookies_path = os.path.expanduser(YOUTUBE_COOKIES_PATH)
    if not os.path.exists(cookies_path):
        raise FileNotFoundError(
            f"YOUTUBE_COOKIES_PATH not found: {cookies_path}"
        )
    return {"cookiefile": cookies_path}


def map_youtube_download_error(error: Exception) -> Optional[str]:
    """Map known yt-dlp errors to friendly messages."""
    message = str(error)
    lowered = message.lower()
    if "confirm you" in lowered and "not a bot" in lowered:
        return (
            "YouTube requires sign-in to pass the bot check. Set YOUTUBE_COOKIES_PATH "
            "to a cookies.txt exported from a logged-in account, then retry."
        )
    if "members-only" in lowered or "join this channel" in lowered:
        return (
            "YouTube video is members-only. Set YOUTUBE_COOKIES_PATH to a cookies.txt "
            "exported from a logged-in account with access, then retry."
        )
    return None


class BaseYouTubeDownloader(ABC):
    """Strategy interface for downloading YouTube videos."""

    @abstractmethod
    async def download(self, video_url: str, process_id: str) -> None:
        """Download video and update tracker status."""
        raise NotImplementedError


class RemoteYouTubeDownloader(BaseYouTubeDownloader):
    """Streams downloads through a remote API endpoint."""

    def __init__(self, endpoint: str, download_folder: str) -> None:
        self.endpoint = endpoint
        self.download_folder = download_folder

    async def download(self, video_url: str, process_id: str) -> None:
        params = {"url": video_url}
        DOWNLOAD_TRACKER.update_job(process_id, status="running", progress=0.0)

        async with httpx.AsyncClient(timeout=None) as client:
            try:
                async with client.stream(
                    "GET", self.endpoint, params=params, follow_redirects=True
                ) as response:
                    if response.status_code >= 400:
                        error_body = (await response.aread()).decode(errors="ignore")
                        raise RuntimeError(
                            f"Remote API error ({response.status_code}): {error_body.strip() or 'unexpected response'}"
                        )

                    remote_name = extract_filename_from_disposition(
                        response.headers.get("content-disposition", "")
                    )
                    ext = os.path.splitext(remote_name)[1] if remote_name else ".mp4"
                    filename = f"{uuid.uuid4().hex}{ext}"
                    file_path = os.path.join(self.download_folder, filename)

                    total_bytes_header = response.headers.get("content-length")
                    total_bytes = int(total_bytes_header) if total_bytes_header else None
                    if total_bytes:
                        DOWNLOAD_TRACKER.update_job(
                            process_id, total_bytes=total_bytes, progress=0.0
                        )

                    bytes_downloaded = 0
                    last_update_time = 0.0
                    last_progress_bucket = -1
                    last_bytes_reported = 0
                    with open(file_path, "wb") as file_handle:
                        async for chunk in response.aiter_bytes():
                            if chunk:
                                file_handle.write(chunk)
                                bytes_downloaded += len(chunk)
                                progress = (
                                    (bytes_downloaded / total_bytes) * 100
                                    if total_bytes
                                    else 0.0
                                )
                                now = time.monotonic()
                                progress_bucket = int(progress)
                                should_emit = False
                                if progress_bucket != last_progress_bucket:
                                    should_emit = True
                                elif bytes_downloaded - last_bytes_reported >= 1024 * 1024:
                                    should_emit = True
                                elif now - last_update_time >= 0.75:
                                    should_emit = True

                                if should_emit:
                                    last_update_time = now
                                    last_progress_bucket = progress_bucket
                                    last_bytes_reported = bytes_downloaded
                                    DOWNLOAD_TRACKER.update_job(
                                        process_id,
                                        bytes_downloaded=bytes_downloaded,
                                        progress=progress,
                                    )
            except httpx.RequestError as exc:
                raise RuntimeError(f"Failed to reach remote API: {exc}") from exc

        DOWNLOAD_TRACKER.update_job(
            process_id,
            status="completed",
            progress=100.0,
            file_path=file_path,
            suggested_name=remote_name or filename,
        )
        delete_file_later(file_path, delay=DOWNLOAD_RETENTION_SECONDS)


class LocalYouTubeDownloader(BaseYouTubeDownloader):
    """Executes downloads with yt-dlp locally."""

    def __init__(self, download_folder: str) -> None:
        self.download_folder = download_folder

    async def download(self, video_url: str, process_id: str) -> None:
        output_template = os.path.join(
            self.download_folder, "%(id)s_%(title)s.%(ext)s"
        )
        DOWNLOAD_TRACKER.update_job(process_id, status="running", progress=0.0)
        custom_options = build_youtube_download_options()

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
                    process_id,
                    bytes_downloaded=downloaded,
                    total_bytes=int(total) if total else None,
                    progress=progress,
                )
            elif status == "finished":
                DOWNLOAD_TRACKER.update_job(process_id, progress=100.0)

        try:
            file_path = await asyncio.to_thread(
                download_video,
                video_url,
                output_template,
                custom_options,
                hook,
            )
        except Exception as exc:
            mapped_error = map_youtube_download_error(exc)
            if mapped_error:
                raise RuntimeError(mapped_error) from exc
            raise

        DOWNLOAD_TRACKER.update_job(
            process_id,
            status="completed",
            progress=100.0,
            file_path=file_path,
            suggested_name=os.path.basename(file_path),
        )
        delete_file_later(file_path, delay=DOWNLOAD_RETENTION_SECONDS)


def build_youtube_downloader() -> BaseYouTubeDownloader:
    """Factory to choose the appropriate download strategy."""
    if YOUTUBE_REMOTE_ENDPOINT:
        return RemoteYouTubeDownloader(YOUTUBE_REMOTE_ENDPOINT, DOWNLOAD_FOLDER)
    return LocalYouTubeDownloader(DOWNLOAD_FOLDER)


YOUTUBE_DOWNLOADER = build_youtube_downloader()
