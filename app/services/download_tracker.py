from __future__ import annotations

import os
import threading
import uuid
from dataclasses import dataclass, asdict
from typing import Dict, Optional

from app.services.redis_client import get_redis


@dataclass
class DownloadJob:
    process_id: str
    source: str
    url: str
    status: str = "pending"
    progress: float = 0.0
    bytes_downloaded: int = 0
    total_bytes: Optional[int] = None
    file_path: Optional[str] = None
    suggested_name: Optional[str] = None
    error: Optional[str] = None


class DownloadTracker:
    def __init__(self) -> None:
        self._jobs: Dict[str, DownloadJob] = {}
        self._lock = threading.Lock()
        self._redis = get_redis()
        self._redis_prefix = "download_job:"
        self._redis_ttl_seconds = int(os.environ.get("DOWNLOAD_JOB_TTL_SECONDS", "7200"))

    def _redis_key(self, process_id: str) -> str:
        return f"{self._redis_prefix}{process_id}"

    def _redis_encode(self, value) -> Optional[str]:
        if value is None:
            return None
        if isinstance(value, bool):
            return "1" if value else "0"
        return str(value)

    def _redis_decode_job(self, data: Dict[str, str]) -> Optional[DownloadJob]:
        if not data:
            return None

        def opt_int(name: str) -> Optional[int]:
            raw = data.get(name)
            if raw is None or raw == "":
                return None
            try:
                return int(raw)
            except ValueError:
                return None

        def opt_float(name: str, default: float = 0.0) -> float:
            raw = data.get(name)
            if raw is None or raw == "":
                return default
            try:
                return float(raw)
            except ValueError:
                return default

        return DownloadJob(
            process_id=data.get("process_id") or "",
            source=data.get("source") or "",
            url=data.get("url") or "",
            status=data.get("status") or "pending",
            progress=opt_float("progress", 0.0),
            bytes_downloaded=opt_int("bytes_downloaded") or 0,
            total_bytes=opt_int("total_bytes"),
            file_path=data.get("file_path") or None,
            suggested_name=data.get("suggested_name") or None,
            error=data.get("error") or None,
        )

    def create_job(self, source: str, url: str) -> DownloadJob:
        process_id = uuid.uuid4().hex
        job = DownloadJob(process_id=process_id, source=source, url=url)

        if self._redis:
            key = self._redis_key(process_id)
            mapping = {
                "process_id": process_id,
                "source": source,
                "url": url,
                "status": job.status,
                "progress": self._redis_encode(job.progress) or "0",
                "bytes_downloaded": self._redis_encode(job.bytes_downloaded) or "0",
            }
            pipe = self._redis.pipeline()
            pipe.hset(key, mapping=mapping)
            pipe.expire(key, self._redis_ttl_seconds)
            pipe.execute()
            return job

        with self._lock:
            self._jobs[process_id] = job
        return job

    def get_job(self, process_id: str) -> Optional[DownloadJob]:
        if self._redis:
            data = self._redis.hgetall(self._redis_key(process_id))
            return self._redis_decode_job(data)

        with self._lock:
            return self._jobs.get(process_id)

    def update_job(self, process_id: str, **updates) -> None:
        if self._redis:
            key = self._redis_key(process_id)
            mapping: Dict[str, str] = {}
            deletions = []

            for field, value in updates.items():
                if field not in DownloadJob.__dataclass_fields__:
                    continue
                encoded = self._redis_encode(value)
                if encoded is None:
                    deletions.append(field)
                else:
                    mapping[field] = encoded

            if not mapping and not deletions:
                return

            pipe = self._redis.pipeline()
            if mapping:
                pipe.hset(key, mapping=mapping)
            if deletions:
                pipe.hdel(key, *deletions)
            pipe.expire(key, self._redis_ttl_seconds)
            pipe.execute()
            return

        with self._lock:
            job = self._jobs.get(process_id)
            if not job:
                return
            for key, value in updates.items():
                if hasattr(job, key):
                    setattr(job, key, value)

    def serialize_job(self, process_id: str) -> Optional[Dict[str, object]]:
        job = self.get_job(process_id)
        if not job:
            return None
        payload = asdict(job)
        if payload.get("file_path"):
            payload["file_exists"] = os.path.exists(payload["file_path"])
        else:
            payload["file_exists"] = False
        return payload

    def protected_file_paths(self) -> set[str]:
        """Return file paths that should not be deleted yet (best-effort)."""
        protected: set[str] = set()

        if self._redis:
            # Best-effort: scan all known job keys.
            for key in self._redis.scan_iter(match=f"{self._redis_prefix}*"):
                try:
                    data = self._redis.hmget(key, "status", "file_path")
                except Exception:
                    continue
                status = data[0] if len(data) > 0 else None
                file_path = data[1] if len(data) > 1 else None
                if status in {"pending", "running"} and file_path:
                    protected.add(file_path)
            return protected

        with self._lock:
            for job in self._jobs.values():
                if job.status in {"pending", "running"} and job.file_path:
                    protected.add(job.file_path)
        return protected


DOWNLOAD_TRACKER = DownloadTracker()
