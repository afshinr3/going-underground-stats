from __future__ import annotations

import logging
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path

import requests

from config.settings import Settings


@dataclass
class PublishResult:
    platform: str
    remote_id: str | None = None
    url: str | None = None


class PublishError(RuntimeError):
    """Raised by a publisher; the message is stored in the deliveries table."""


class Publisher(ABC):
    name: str = "base"

    def __init__(self, settings: Settings):
        self.s = settings
        self.log = logging.getLogger(f"publisher.{self.name}")
        self.http = requests.Session()
        self.http.headers["User-Agent"] = "gu-crosspost/1.0"

    @abstractmethod
    def validate(self) -> None:
        """Raise PublishError if required config is missing."""

    @abstractmethod
    def publish(self, video: Path, caption: str, post: dict) -> PublishResult: ...

    # Shared helper: poll `check()` until it returns truthy or `timeout` elapses.
    @staticmethod
    def wait_until(check, timeout: int = 600, interval: int = 5, what: str = "processing"):
        deadline = time.time() + timeout
        while time.time() < deadline:
            result = check()
            if result:
                return result
            time.sleep(interval)
        raise PublishError(f"timed out after {timeout}s waiting for {what}")
