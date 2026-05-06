"""Semantic similarity for ingredient name matching.

Uses sentence-transformers (all-MiniLM-L6-v2, ~80MB) to capture synonym
matches that the token-based matcher misses — lettuce ≡ salad-greens,
beans ≡ legumes, eggplant ≡ aubergine, tomato ≡ tomatoes,
multilingual aliases.

Embeddings are persisted to the existing SQLite db so re-scoring runs is
free after the first computation.

Gracefully degrades when sentence-transformers / torch are not installed:
is_available() returns False and semantic_similarity() returns 0.0, letting
the token-based matcher in scoring.py work on its own.
"""
from __future__ import annotations

import threading
from typing import Optional

import numpy as np

from . import db

_MODEL = None
_LOCK = threading.Lock()
_AVAILABLE: Optional[bool] = None  # None=not yet probed, True/False after first try


def _ensure_table() -> None:
    with db._conn() as c:
        c.execute("""
            CREATE TABLE IF NOT EXISTS embeddings (
                text TEXT PRIMARY KEY,
                vec BLOB NOT NULL
            )
        """)


def _try_load_model():
    """Best-effort load. Sets _AVAILABLE on first call."""
    global _MODEL, _AVAILABLE
    if _MODEL is not None:
        return _MODEL
    if _AVAILABLE is False:
        return None
    with _LOCK:
        if _MODEL is not None:
            return _MODEL
        try:
            from sentence_transformers import SentenceTransformer
            _MODEL = SentenceTransformer("all-MiniLM-L6-v2")
            _AVAILABLE = True
            _ensure_table()
            print("[semantic] all-MiniLM-L6-v2 loaded; semantic matching enabled.")
        except Exception as e:
            _AVAILABLE = False
            _MODEL = None
            print(f"[semantic] sentence-transformers unavailable ({type(e).__name__}: {e})")
            print("[semantic] To enable semantic matching:")
            print("[semantic]   pip install sentence-transformers")
            print("[semantic] Falling back to token-based matching only.")
    return _MODEL


def is_available() -> bool:
    """True iff the semantic model is loaded and usable.

    Probes lazily on first call. Subsequent calls are O(1). The token-only
    matcher in scoring.py uses this flag to decide whether to compute the
    semantic floor at all — keeps the hot path fast when deps are missing.
    """
    if _AVAILABLE is None:
        _try_load_model()
    return _AVAILABLE is True


def _normalize(text: str) -> str:
    return (text or "").strip().lower()


def _get_cached(key: str) -> Optional[np.ndarray]:
    with db._conn() as c:
        r = c.execute("SELECT vec FROM embeddings WHERE text=?", (key,)).fetchone()
        if r is None:
            return None
        return np.frombuffer(r["vec"], dtype=np.float32)


def _cache(key: str, vec: np.ndarray) -> None:
    blob = vec.astype(np.float32).tobytes()
    with db._conn() as c:
        c.execute("INSERT OR REPLACE INTO embeddings (text, vec) VALUES (?, ?)",
                  (key, blob))


def embed(text: str) -> Optional[np.ndarray]:
    """Return a 384-dim L2-normalized embedding, or None if unavailable.

    Cached in SQLite — each unique ingredient string is embedded exactly
    once across all runs and re-scores.
    """
    key = _normalize(text)
    if not key:
        return None
    if not is_available():
        return None
    cached = _get_cached(key)
    if cached is not None:
        return cached
    model = _try_load_model()
    if model is None:
        return None
    vec = model.encode([key], normalize_embeddings=True)[0].astype(np.float32)
    _cache(key, vec)
    return vec


def semantic_similarity(a: str, b: str) -> float:
    """Cosine similarity in [0, 1]. Returns 0.0 if the model is unavailable.

    Embeddings are L2-normalized so dot product == cosine in [-1, 1]. We
    clamp to [0, 1] — for short food names with all-MiniLM, negative values
    are vanishingly rare and would confuse downstream max() logic.
    """
    if not a or not b:
        return 0.0
    va = embed(a)
    vb = embed(b)
    if va is None or vb is None:
        return 0.0
    cos = float(np.dot(va, vb))
    return max(0.0, min(1.0, cos))


def warmup_cache(strings: list[str]) -> int:
    """Pre-embed a batch of strings (called by ingredient_match before the
    pairwise loop so the inner calls are all cache hits). Returns count of
    NEW embeddings computed."""
    if not is_available() or not strings:
        return 0
    model = _try_load_model()
    if model is None:
        return 0
    # Find which keys aren't cached yet
    keys = [_normalize(s) for s in strings if s]
    keys = [k for k in keys if k]
    if not keys:
        return 0
    missing = []
    for k in keys:
        if _get_cached(k) is None:
            missing.append(k)
    missing = list(dict.fromkeys(missing))  # dedupe, preserve order
    if not missing:
        return 0
    vecs = model.encode(missing, normalize_embeddings=True, batch_size=32)
    for k, v in zip(missing, vecs):
        _cache(k, v.astype(np.float32))
    return len(missing)
