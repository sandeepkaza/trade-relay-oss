"""
vision_ocr.py - Google Cloud Vision OCR wrapper.

Async-friendly: actual Vision SDK call is sync, but we offload to a thread.
Reads creds from GOOGLE_APPLICATION_CREDENTIALS env var (path to service
account JSON). If creds or SDK missing, returns empty string and logs a
warning — caller should treat OCR as best-effort.
"""

import asyncio
import logging
import os
from typing import Optional

import httpx

log = logging.getLogger(__name__)

_VISION_AVAILABLE: Optional[bool] = None
_vision_client = None


def _try_import_client():
    """Lazy import — Vision SDK is heavy and optional."""
    global _VISION_AVAILABLE, _vision_client
    if _VISION_AVAILABLE is not None:
        return _VISION_AVAILABLE
    creds_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if not creds_path or not os.path.isfile(creds_path):
        log.warning(
            "[VISION] GOOGLE_APPLICATION_CREDENTIALS not set or file missing — OCR disabled. "
            "Set env var to your service-account JSON path to enable."
        )
        _VISION_AVAILABLE = False
        return False
    try:
        from google.cloud import vision  # type: ignore
        _vision_client = vision.ImageAnnotatorClient()
        _VISION_AVAILABLE = True
        log.info("[VISION] google-cloud-vision client initialized (creds: %s)", creds_path)
        return True
    except ImportError:
        log.warning("[VISION] google-cloud-vision not installed — OCR disabled. Install: pip install google-cloud-vision")
        _VISION_AVAILABLE = False
        return False
    except Exception as e:
        log.error("[VISION] Failed to initialize client: %s", e)
        _VISION_AVAILABLE = False
        return False


def is_available() -> bool:
    """True if Vision OCR is usable."""
    return _try_import_client()


async def _fetch_image_bytes(url: str, timeout: float = 10.0) -> Optional[bytes]:
    try:
        async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
            resp = await client.get(url, headers={"User-Agent": "trader-vision/1.0"})
            resp.raise_for_status()
            return resp.content
    except Exception as e:
        log.warning("[VISION] Failed to download %s: %s", url, e)
        return None


def _ocr_sync(image_bytes: bytes) -> str:
    """Sync Vision call — use DOCUMENT_TEXT_DETECTION for chart-style images
    (better at structured text than basic TEXT_DETECTION)."""
    from google.cloud import vision  # type: ignore
    import app.core.usage_tracker as _ut
    image = vision.Image(content=image_bytes)
    try:
        response = _vision_client.document_text_detection(image=image)
        if response.error.message:
            _ut.record_vision(success=False)
            raise RuntimeError(f"Vision API error: {response.error.message}")
        _ut.record_vision(success=True)
        return (response.full_text_annotation.text or "").strip()
    except Exception:
        _ut.record_vision(success=False)
        raise


async def ocr_image_url(
    url: str,
    *,
    retries: int = 3,
    backoff_base: float = 1.5,
) -> str:
    """OCR a single image URL. Returns extracted text or empty string on
    failure. Retries up to `retries` times with exponential backoff."""
    if not _try_import_client():
        return ""

    img_bytes = await _fetch_image_bytes(url)
    if not img_bytes:
        return ""

    last_err: Optional[Exception] = None
    for attempt in range(1, retries + 1):
        try:
            text = await asyncio.to_thread(_ocr_sync, img_bytes)
            log.debug("[VISION] OCR success url=%s chars=%d", url, len(text))
            return text
        except Exception as e:
            last_err = e
            if attempt < retries:
                delay = backoff_base ** attempt
                log.warning("[VISION] OCR attempt %d/%d failed: %s — retry in %.1fs",
                            attempt, retries, e, delay)
                await asyncio.sleep(delay)
    log.error("[VISION] OCR failed after %d attempts for %s: %s", retries, url, last_err)
    return ""


async def ocr_image_urls(urls: list[str]) -> str:
    """OCR multiple images concurrently. Returns concatenated text separated
    by newlines (empty string if all failed)."""
    if not urls:
        return ""
    results = await asyncio.gather(*[ocr_image_url(u) for u in urls], return_exceptions=False)
    return "\n".join(t for t in results if t)
