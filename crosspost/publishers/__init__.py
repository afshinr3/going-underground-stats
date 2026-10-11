from .base import PublishError, Publisher, PublishResult
from .instagram import InstagramPublisher
from .linkedin import LinkedInPublisher
from .rednote import RednotePublisher
from .telegram import TelegramPublisher

REGISTRY: dict[str, type[Publisher]] = {
    "telegram": TelegramPublisher,
    "instagram": InstagramPublisher,
    "linkedin": LinkedInPublisher,
    "rednote": RednotePublisher,
}

__all__ = ["REGISTRY", "PublishError", "Publisher", "PublishResult"]
