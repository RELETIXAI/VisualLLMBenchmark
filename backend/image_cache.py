"""Local image cache — every image used by a benchmark run lives on disk.

Goals:
  • Zero external image fetches during runs. Cloud and local LLMs both
    receive a base64-encoded local file.
  • One-time resize to a vision-LLM-friendly size: 1024px on the longest
    edge, JPEG quality 85. Most VLMs internally downscale to ~768–1024px,
    so this is the sweet spot for quality vs bandwidth/disk.
  • Idempotent: hydrate is safe to re-run; existing files are kept.

Layout:
    data/images/<dataset_id>/<image_id>.jpg

The dataset_id segment scopes images per dataset so two datasets that
share an image_id never collide.
"""
from __future__ import annotations

import io
import os
from pathlib import Path
from typing import Optional

import httpx
from PIL import Image

ROOT = Path(__file__).resolve().parent.parent
_DATA_DIR = Path(os.environ.get("LLMBENCH_DATA_DIR") or (ROOT / "data"))
IMAGES_DIR = _DATA_DIR / "images"
MAX_DIM = 1024            # longest edge after resize
JPEG_QUALITY = 85         # quality 85 is a strong sweet spot
HTTP_TIMEOUT = 30.0


def cache_path(dataset_id: int, image_id: str) -> Path:
    """Where the resized JPEG lives on disk."""
    return IMAGES_DIR / str(dataset_id) / f"{image_id}.jpg"


def has_local(dataset_id: int, image_id: str) -> bool:
    return cache_path(dataset_id, image_id).exists()


def _resize_and_save(raw: bytes, dest: Path) -> dict:
    """Resize bytes (any common format) to ≤MAX_DIM longest edge and save
    as optimized JPEG. Returns metadata for the task log."""
    src = Image.open(io.BytesIO(raw))
    orig_size = src.size
    if src.mode != "RGB":
        src = src.convert("RGB")
    if max(src.size) > MAX_DIM:
        src.thumbnail((MAX_DIM, MAX_DIM), Image.LANCZOS)
    dest.parent.mkdir(parents=True, exist_ok=True)
    src.save(dest, "JPEG", quality=JPEG_QUALITY, optimize=True)
    return {
        "orig_size": orig_size,
        "saved_size": src.size,
        "bytes_in": len(raw),
        "bytes_out": dest.stat().st_size,
    }


def fetch_and_cache(dataset_id: int, image_id: str, image_url: str,
                    client: Optional[httpx.Client] = None) -> Path:
    """Idempotent: download → resize → save. Returns local path. Raises
    on network/decode error.

    When LLMBENCH_IMAGES_ONLY_LOCAL=1, this raises if the image is not
    already on disk — fine-tuning workflows must never silently pull
    from S3/HTTP and accidentally include a holdout image."""
    dest = cache_path(dataset_id, image_id)
    if dest.exists():
        return dest
    if os.environ.get("LLMBENCH_IMAGES_ONLY_LOCAL", "0") == "1":
        raise RuntimeError(
            f"LLMBENCH_IMAGES_ONLY_LOCAL=1 but image not hydrated: "
            f"dataset_id={dataset_id} image_id={image_id!r} url={image_url!r}. "
            f"Hydrate first or unset the env var."
        )
    own_client = client is None
    cli = client or httpx.Client(timeout=HTTP_TIMEOUT, follow_redirects=True)
    try:
        r = cli.get(image_url)
        r.raise_for_status()
        _resize_and_save(r.content, dest)
        return dest
    finally:
        if own_client:
            cli.close()


def cache_local_file(dataset_id: int, image_id: str, src_path: str | Path) -> Path:
    """Resize + cache an already-on-disk file (e.g. extracted from xlsx)."""
    dest = cache_path(dataset_id, image_id)
    if dest.exists():
        return dest
    raw = Path(src_path).read_bytes()
    _resize_and_save(raw, dest)
    return dest
