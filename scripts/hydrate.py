#!/usr/bin/env python3
"""Standalone parallel image hydrator for the WILLMA dataset.

Why this exists: the `/api/datasets/{id}/hydrate` endpoint runs as a
daemon thread inside uvicorn, which means it dies every time uvicorn
`--reload` triggers (any *.py edit in backend/). This script is a
standalone process — independent of FastAPI — that keeps running until
done regardless of what happens to the dev server.

Idempotent + dedup-safe:
  - Queue is built ONCE from the parsed dataset, deduped by image_id
    (same image_id appears in many WILLMA rows but only needs one fetch).
  - Already-cached images are filtered out before the queue is built.
  - `image_cache.fetch_and_cache` checks `dest.exists()` before downloading
    (so even if two workers raced on the same id, the second is a no-op).

Usage:
    cd /Users/samueltoma/Documents/Reletix/LLMbenchmark
    .venv/bin/python scripts/hydrate.py --dataset-id 6 --workers 12

Arguments:
    --dataset-id   Dataset id in benchmark.db (default: 6 for WILLMA)
    --workers      Concurrent http fetches (default: 12)
    --max          Cap on total fetches this run (default: no cap)
    --dry-run      Build the queue and print sizes; don't fetch.
"""
from __future__ import annotations

import argparse
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

# Make `import backend.*` work when running as `python scripts/hydrate.py`
REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

import httpx

from backend import db, image_cache
from backend.parser import parse_dataset


def _stable_image_id(row: dict) -> str | None:
    """Mirror of backend.main._stable_image_id."""
    iid = row.get("image_id")
    if iid:
        return iid
    if row.get("image_url"):
        import hashlib
        return "url-" + hashlib.sha1(row["image_url"].encode()).hexdigest()[:16]
    if row.get("image_path"):
        return Path(row["image_path"]).stem
    return None


def build_queue(dataset_id: int) -> list[tuple[str, str]]:
    """Return list of (image_id, image_url) tuples for rows that
    (a) have an image_url, (b) are not already cached locally.
    Deduped by image_id."""
    ds = db.get_dataset(dataset_id)
    if not ds:
        raise SystemExit(f"dataset {dataset_id} not found in benchmark.db")
    parsed = parse_dataset(
        ds["file_path"],
        image_url_template=ds.get("image_url_template"),
        dataset_id=dataset_id,
    )
    rows = parsed.get("rows") or []
    seen: set[str] = set()
    out: list[tuple[str, str]] = []
    n_no_id = n_no_url = n_already = 0
    for row in rows:
        iid = _stable_image_id(row)
        if not iid:
            n_no_id += 1
            continue
        if iid in seen:
            continue
        seen.add(iid)
        if image_cache.has_local(dataset_id, iid):
            n_already += 1
            continue
        url = row.get("image_url")
        if not url:
            # Could also be image_path (local file), but for WILLMA we always
            # have an http url. Skip otherwise — the script is fetcher-only.
            if row.get("image_path"):
                # Cache local file directly
                try:
                    image_cache.cache_local_file(dataset_id, iid, row["image_path"])
                    n_already += 1
                except Exception:
                    pass
            else:
                n_no_url += 1
            continue
        out.append((iid, url))
    print(f"[queue] dataset_id={dataset_id} rows={len(rows)} "
          f"unique_ids={len(seen)} already_cached={n_already} "
          f"no_id={n_no_id} no_url={n_no_url} to_fetch={len(out)}")
    return out


def fetch_one(client: httpx.Client, dataset_id: int,
              iid: str, url: str) -> tuple[str, str | None]:
    """Returns (status, error_msg). status ∈ {ok, skipped, error}."""
    try:
        # Re-check inside the worker — another worker may have raced.
        if image_cache.has_local(dataset_id, iid):
            return ("skipped", None)
        image_cache.fetch_and_cache(dataset_id, iid, url, client=client)
        return ("ok", None)
    except httpx.HTTPStatusError as e:
        return ("error", f"http {e.response.status_code}")
    except Exception as e:
        return ("error", f"{type(e).__name__}: {str(e)[:120]}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dataset-id", type=int, default=6)
    ap.add_argument("--workers", type=int, default=12)
    ap.add_argument("--max", type=int, default=None,
                    help="Cap on number of fetches (default: no cap)")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    queue = build_queue(args.dataset_id)
    if args.max is not None:
        queue = queue[: args.max]
        print(f"[queue] capped to first {len(queue)} (--max)")
    if args.dry_run:
        print("[dry-run] not fetching")
        return 0
    if not queue:
        print("[queue] nothing to fetch — all done")
        return 0

    # One shared httpx client per worker thread is cleaner than a
    # global lock-contended client. httpx.Client is thread-safe but a
    # single connection pool gates concurrent requests; we use one
    # client per worker.
    n_ok = n_skip = n_err = 0
    err_samples: list[str] = []
    started = time.time()
    last_log = started

    print(f"[hydrate] starting with {args.workers} workers, "
          f"{len(queue)} images to fetch")

    def worker_factory():
        # Each worker gets its own client so connection pools aren't shared
        return httpx.Client(timeout=image_cache.HTTP_TIMEOUT,
                            follow_redirects=True,
                            limits=httpx.Limits(max_connections=2,
                                                max_keepalive_connections=2))

    # We can't easily give each thread its own client via ThreadPoolExecutor
    # without a thread-local. Use a dict keyed by thread name as a simple
    # thread-local cache.
    import threading
    _local = threading.local()

    def fetch_with_local_client(iid_url):
        if not hasattr(_local, "client"):
            _local.client = worker_factory()
        return fetch_one(_local.client, args.dataset_id, iid_url[0], iid_url[1])

    try:
        with ThreadPoolExecutor(max_workers=args.workers) as ex:
            futures = {ex.submit(fetch_with_local_client, item): item
                       for item in queue}
            for i, fut in enumerate(as_completed(futures), 1):
                status, err = fut.result()
                if status == "ok":
                    n_ok += 1
                elif status == "skipped":
                    n_skip += 1
                else:
                    n_err += 1
                    if len(err_samples) < 5:
                        iid, _url = futures[fut]
                        err_samples.append(f"{iid}: {err}")
                # Throttled progress print: every 50 results or every 5 s.
                now = time.time()
                if i % 50 == 0 or (now - last_log) >= 5.0:
                    elapsed = now - started
                    rate = i / elapsed if elapsed > 0 else 0
                    eta = (len(queue) - i) / rate if rate > 0 else 0
                    print(f"[{i}/{len(queue)}] ok={n_ok} skip={n_skip} err={n_err} "
                          f"rate={rate:.1f}/s eta={eta/60:.1f} min")
                    last_log = now
    except KeyboardInterrupt:
        print("\n[hydrate] interrupted by user — partial progress is on disk")
        return 130

    elapsed = time.time() - started
    print(f"[hydrate] done in {elapsed/60:.1f} min — "
          f"ok={n_ok} skipped_in_worker={n_skip} errors={n_err}")
    if err_samples:
        print("[hydrate] sample errors:")
        for e in err_samples:
            print(f"  - {e}")
    return 0 if n_err == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
