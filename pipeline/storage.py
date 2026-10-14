"""Where finished raw files get put: S3 in real runs, a folder in tests."""

from __future__ import annotations

import logging
import shutil
from pathlib import Path
from typing import Protocol

from pipeline.config import StorageSettings

log = logging.getLogger(__name__)


class Storage(Protocol):
    def upload(self, local_path: Path, key: str) -> str:
        """Store a local file under `key` and return where it ended up."""
        ...


class S3Storage:
    def __init__(self, bucket: str, client=None) -> None:
        if client is None:
            import boto3  # imported lazily so local dry runs do not need AWS

            client = boto3.client("s3")
        self._bucket = bucket
        self._s3 = client

    def upload(self, local_path: Path, key: str) -> str:
        # Writing to the same key replaces the object in one step, so a reader
        # sees either the old file or the new one, never half of each.
        self._s3.upload_file(str(local_path), self._bucket, key)
        uri = f"s3://{self._bucket}/{key}"
        log.info("Uploaded %s", uri)
        return uri


class LocalStorage:
    def __init__(self, root: str | Path) -> None:
        self._root = Path(root)

    def upload(self, local_path: Path, key: str) -> str:
        destination = self._root / key
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(local_path, destination)
        log.info("Wrote %s", destination)
        return str(destination)


def get_storage(settings: StorageSettings) -> Storage:
    if settings.local_dir:
        return LocalStorage(settings.local_dir)
    return S3Storage(settings.bucket)
