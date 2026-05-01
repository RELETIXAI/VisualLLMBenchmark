"""Export / import bundle for cross-machine data sharing.

Bundle format (zip):
  manifest.json        — schema version, source machine, counts
  prompts.json         — array keyed by content_hash
  datasets.json        — array keyed by file_hash
  runs.json            — array; FKs replaced by prompt_hash + dataset_hash
  row_results.json     — array scoped by (source_machine_id, source_run_id)
  corrections.json     — current corrections state keyed by (dataset_hash, image_id)
  history.json         — full correction audit log
  files/<hash><ext>    — dataset source file, one per unique hash
  images/<fname>       — local images (skipped when image_ref is a URL)
"""
from __future__ import annotations

import json
import shutil
import tempfile
import time
import zipfile
from pathlib import Path
from typing import Optional

from . import db

SCHEMA_VERSION = 1
ROOT = Path(__file__).resolve().parent.parent


# ── helpers ──────────────────────────────────────────────────────────────────

def _jloads(v) -> dict | list:
    if isinstance(v, (dict, list)):
        return v
    try:
        return json.loads(v or "{}")
    except Exception:
        return {}


# ── Export ───────────────────────────────────────────────────────────────────

def pack_export(
    prompt_ids: Optional[list[int]] = None,
    dataset_ids: Optional[list[int]] = None,
    run_ids: Optional[list[int]] = None,
    include_row_results: bool = True,
    include_history: bool = True,
) -> Path:
    """Build a zip bundle and return its path (inside a mkdtemp dir).
    Caller is responsible for deleting the directory after use."""
    machine_id = db.get_machine_id()

    all_prompts = db.list_prompts()
    all_datasets = db.list_datasets()
    all_runs = db.list_runs()

    if run_ids is not None:
        all_runs = [r for r in all_runs if r["id"] in set(run_ids)]
    # Always include prompts/datasets referenced by selected runs
    used_prompt_ids  = {r["prompt_id"]  for r in all_runs}
    used_dataset_ids = {r["dataset_id"] for r in all_runs}
    if prompt_ids is not None:
        selected_pids = set(prompt_ids) | used_prompt_ids
        all_prompts = [p for p in all_prompts if p["id"] in selected_pids]
    else:
        # include all + anything referenced by runs
        pid_set = {p["id"] for p in all_prompts} | used_prompt_ids
        all_prompts = [p for p in db.list_prompts() if p["id"] in pid_set]
    if dataset_ids is not None:
        selected_dids = set(dataset_ids) | used_dataset_ids
        all_datasets = [d for d in all_datasets if d["id"] in selected_dids]
    else:
        did_set = {d["id"] for d in all_datasets} | used_dataset_ids
        all_datasets = [d for d in db.list_datasets() if d["id"] in did_set]

    # Build hash maps
    prompt_hash_map: dict[int, str] = {}
    for p in all_prompts:
        h = p.get("content_hash") or db.hash_prompt(p["name"], p["system_prompt"])
        prompt_hash_map[p["id"]] = h

    dataset_hash_map: dict[int, str] = {}
    for d in all_datasets:
        h = d.get("file_hash")
        if not h:
            try:
                h = db.hash_file(d["file_path"])
            except Exception:
                h = "unknown_" + str(d["id"])
        dataset_hash_map[d["id"]] = h

    # Serialise prompts
    exp_prompts = [
        {
            "content_hash": prompt_hash_map[p["id"]],
            "name": p["name"],
            "system_prompt": p["system_prompt"],
            "description": p.get("description"),
            "created_at": p["created_at"],
        }
        for p in all_prompts
    ]

    # Serialise datasets
    exp_datasets = [
        {
            "file_hash": dataset_hash_map[d["id"]],
            "name": d["name"],
            "n_rows": d["n_rows"],
            "columns_detected": d.get("columns_detected"),
            "image_url_template": d.get("image_url_template"),
            "created_at": d["created_at"],
            "file_name": Path(d["file_path"]).name,
            "file_suffix": Path(d["file_path"]).suffix,
        }
        for d in all_datasets
    ]

    # Serialise runs + row results
    exp_runs = []
    exp_row_results = []
    local_images: set[str] = set()

    for r in all_runs:
        # Preserve original source identity so re-exports are idempotent
        run_machine   = r.get("source_machine_id") or machine_id
        run_source_id = r.get("source_run_id")     or r["id"]
        ph = prompt_hash_map.get(r["prompt_id"], "")
        dh = dataset_hash_map.get(r["dataset_id"], "")

        exp_runs.append({
            "source_machine_id":    run_machine,
            "source_run_id":        run_source_id,
            "prompt_hash":          ph,
            "dataset_hash":         dh,
            "provider":             r["provider"],
            "model_id":             r["model_id"],
            "status":               r["status"],
            "n_rows":               r["n_rows"],
            "n_done":               r["n_done"],
            "accuracy":             r.get("accuracy", 0),
            "avg_latency_ms":       r.get("avg_latency_ms", 0),
            "total_input_tokens":   r.get("total_input_tokens", 0),
            "total_output_tokens":  r.get("total_output_tokens", 0),
            "total_cost_usd":       r.get("total_cost_usd", 0),
            "composite_score":      r.get("composite_score", 0),
            "config":               _jloads(r.get("config") or "{}"),
            "started_at":           r.get("started_at"),
            "finished_at":          r.get("finished_at"),
            "error":                r.get("error"),
            "dataset_version":      r.get("dataset_version", 0),
        })

        if include_row_results:
            full = db.get_run(r["id"])
            for rr in (full.get("rows") or []):
                ref = rr.get("image_ref") or ""
                if ref and not ref.startswith("http"):
                    local_images.add(Path(ref).name)
                exp_row_results.append({
                    "source_machine_id": run_machine,
                    "source_run_id":     run_source_id,
                    "row_idx":           rr["row_idx"],
                    "latency_ms":        rr.get("latency_ms"),
                    "input_tokens":      rr.get("input_tokens"),
                    "output_tokens":     rr.get("output_tokens"),
                    "cost_usd":          rr.get("cost_usd"),
                    "output_text":       rr.get("output_text"),
                    "output_parsed":     rr.get("output_parsed"),
                    "scores":            rr.get("scores"),
                    "error":             rr.get("error"),
                    "image_ref":         rr.get("image_ref"),
                    "truth":             rr.get("truth"),
                })

    # Serialise corrections + history
    exp_corrections = []
    exp_history = []
    for d in all_datasets:
        dh = dataset_hash_map[d["id"]]
        for corr in db.list_corrections(d["id"]):
            exp_corrections.append({
                "dataset_hash": dh,
                "image_id":     corr["image_id"],
                "truth_json":   corr["truth_json"],
                "note":         corr.get("note"),
                "created_at":   corr["created_at"],
            })
        if include_history:
            for h in db.list_dataset_versions(d["id"]):
                exp_history.append({
                    "dataset_hash":    dh,
                    "image_id":        h["image_id"],
                    "truth_json":      h.get("truth_json"),
                    "prev_truth_json": h.get("prev_truth_json"),
                    "action":          h["action"],
                    "note":            h.get("note"),
                    "created_at":      h["created_at"],
                    "source_machine_id": machine_id,
                })

    # Write zip
    tmpdir   = Path(tempfile.mkdtemp())
    ts       = time.strftime("%Y%m%d-%H%M%S", time.gmtime())
    zip_path = tmpdir / f"benchmark-export-{ts}.zip"

    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        manifest = {
            "schema_version":   SCHEMA_VERSION,
            "exported_at":      time.time(),
            "source_machine_id": machine_id,
            "counts": {
                "prompts":      len(exp_prompts),
                "datasets":     len(exp_datasets),
                "runs":         len(exp_runs),
                "row_results":  len(exp_row_results),
                "corrections":  len(exp_corrections),
                "history":      len(exp_history),
            },
        }
        zf.writestr("manifest.json",     json.dumps(manifest,       indent=2))
        zf.writestr("prompts.json",      json.dumps(exp_prompts,    indent=2))
        zf.writestr("datasets.json",     json.dumps(exp_datasets,   indent=2))
        zf.writestr("runs.json",         json.dumps(exp_runs,       indent=2))
        zf.writestr("row_results.json",  json.dumps(exp_row_results,indent=2))
        zf.writestr("corrections.json",  json.dumps(exp_corrections,indent=2))
        zf.writestr("history.json",      json.dumps(exp_history,    indent=2))

        # Dataset source files (named by hash so destination can identify them)
        for d in all_datasets:
            src = Path(d["file_path"])
            if src.exists():
                dh = dataset_hash_map[d["id"]]
                zf.write(src, f"files/{dh}{src.suffix}")

        # Local images
        images_dir = ROOT / "data" / "images"
        for fname in local_images:
            img = images_dir / fname
            if img.exists():
                zf.write(img, f"images/{fname}")

    return zip_path


# ── Import preview ────────────────────────────────────────────────────────────

def inspect_import(zip_path: Path) -> dict:
    """Parse zip and return a merge plan. Writes nothing to the DB."""
    with zipfile.ZipFile(zip_path, "r") as zf:
        names = set(zf.namelist())

        def _read(name, fallback="[]"):
            return json.loads(zf.read(name)) if name in names else json.loads(fallback)

        manifest        = _read("manifest.json", "{}")
        exp_prompts     = _read("prompts.json")
        exp_datasets    = _read("datasets.json")
        exp_runs        = _read("runs.json")
        exp_corrections = _read("corrections.json")

    sv = manifest.get("schema_version", 0)
    if sv > SCHEMA_VERSION:
        return {"error": f"Bundle schema_version {sv} is newer than this app supports ({SCHEMA_VERSION}). Please upgrade."}

    source_machine = manifest.get("source_machine_id", "unknown")

    # --- prompts ---
    prompt_details, new_p, matched_p = [], 0, 0
    for p in exp_prompts:
        existing = db.find_prompt_by_hash(p["content_hash"])
        if existing:
            matched_p += 1
            prompt_details.append({"hash": p["content_hash"][:12], "name": p["name"],
                                   "status": "matched", "local_id": existing["id"]})
        else:
            new_p += 1
            prompt_details.append({"hash": p["content_hash"][:12], "name": p["name"], "status": "new"})

    # --- datasets ---
    dataset_details, new_d, matched_d = [], 0, 0
    for d in exp_datasets:
        existing = db.find_dataset_by_hash(d["file_hash"])
        n_local  = len(db.list_corrections(existing["id"])) if existing else 0
        n_import = sum(1 for c in exp_corrections if c["dataset_hash"] == d["file_hash"])
        if existing:
            matched_d += 1
            dataset_details.append({
                "hash": d["file_hash"][:12], "name": d["name"], "status": "matched",
                "local_id": existing["id"], "local_name": existing["name"],
                "n_local_corrections": n_local, "n_import_corrections": n_import,
            })
        else:
            new_d += 1
            dataset_details.append({
                "hash": d["file_hash"][:12], "name": d["name"], "status": "new",
                "n_import_corrections": n_import,
            })

    # --- runs ---
    new_r = already_r = orphan_r = 0
    by_status: dict[str, int] = {}
    exp_prompt_hashes  = {p["content_hash"] for p in exp_prompts}
    exp_dataset_hashes = {d["file_hash"]    for d in exp_datasets}
    for r in exp_runs:
        if db.find_imported_run(r["source_machine_id"], r["source_run_id"]):
            already_r += 1
            continue
        ph, dh = r.get("prompt_hash",""), r.get("dataset_hash","")
        p_ok = bool(db.find_prompt_by_hash(ph))  or ph in exp_prompt_hashes
        d_ok = bool(db.find_dataset_by_hash(dh)) or dh in exp_dataset_hashes
        if not p_ok or not d_ok:
            orphan_r += 1
            continue
        new_r += 1
        st = r.get("status", "unknown")
        by_status[st] = by_status.get(st, 0) + 1

    # --- corrections ---
    new_corr = merged_corr = conflicts = 0
    export_time = manifest.get("exported_at", 0)
    for c in exp_corrections:
        existing_ds = db.find_dataset_by_hash(c["dataset_hash"])
        if not existing_ds:
            new_corr += 1
            continue
        existing_c = db.get_correction(existing_ds["id"], c["image_id"])
        if existing_c:
            merged_corr += 1
            if existing_c["created_at"] > export_time:
                conflicts += 1
        else:
            new_corr += 1

    warnings = []
    if orphan_r:
        warnings.append(f"{orphan_r} run(s) skipped — their prompt or dataset is missing from this bundle.")
    if conflicts:
        warnings.append(f"{conflicts} image(s) were edited on both machines after export — local version kept, import history appended.")

    return {
        "schema_version":   sv,
        "source_machine_id": source_machine,
        "exported_at":      manifest.get("exported_at"),
        "prompts":   {"new": new_p,  "matched": matched_p, "details": prompt_details},
        "datasets":  {"new": new_d,  "matched": matched_d, "details": dataset_details},
        "runs":      {"new": new_r,  "already_imported": already_r,
                      "skipped_orphan": orphan_r, "by_status": by_status},
        "corrections": {"new": new_corr, "merged": merged_corr, "conflicts": conflicts},
        "warnings":  warnings,
    }


# ── Import apply ──────────────────────────────────────────────────────────────

def apply_import(zip_path: Path) -> dict:
    """Execute the merge. Returns a summary identical in shape to inspect_import."""
    with zipfile.ZipFile(zip_path, "r") as zf:
        names = set(zf.namelist())

        def _read(name, fallback="[]"):
            return json.loads(zf.read(name)) if name in names else json.loads(fallback)

        manifest        = _read("manifest.json", "{}")
        exp_prompts     = _read("prompts.json")
        exp_datasets    = _read("datasets.json")
        exp_runs        = _read("runs.json")
        exp_row_results = _read("row_results.json")
        exp_corrections = _read("corrections.json")
        exp_history     = _read("history.json")

        sv = manifest.get("schema_version", 0)
        if sv > SCHEMA_VERSION:
            raise ValueError(f"Bundle schema_version {sv} not supported.")

        # 1. Extract dataset source files
        uploads_dir = ROOT / "data" / "uploads"
        uploads_dir.mkdir(parents=True, exist_ok=True)
        for name in names:
            if name.startswith("files/") and not name.endswith("/"):
                dest = uploads_dir / Path(name).name
                if not dest.exists():
                    dest.write_bytes(zf.read(name))

        # 2. Extract local images (skip existing)
        images_dir = ROOT / "data" / "images"
        images_dir.mkdir(parents=True, exist_ok=True)
        for name in names:
            if name.startswith("images/") and not name.endswith("/"):
                dest = images_dir / Path(name).name
                if not dest.exists():
                    dest.write_bytes(zf.read(name))

    # 3. Prompts
    prompt_map: dict[str, int] = {}
    new_prompts = matched_prompts = 0
    for p in exp_prompts:
        h = p["content_hash"]
        existing = db.find_prompt_by_hash(h)
        if existing:
            prompt_map[h] = existing["id"]
            matched_prompts += 1
        else:
            row = db.create_prompt(name=p["name"], system_prompt=p["system_prompt"],
                                   description=p.get("description"))
            prompt_map[h] = row["id"]
            new_prompts += 1

    # 4. Datasets
    dataset_map: dict[str, int] = {}
    new_datasets = matched_datasets = 0
    uploads_dir = ROOT / "data" / "uploads"
    for d in exp_datasets:
        fh = d["file_hash"]
        existing = db.find_dataset_by_hash(fh)
        if existing:
            dataset_map[fh] = existing["id"]
            matched_datasets += 1
        else:
            suffix = d.get("file_suffix") or Path(d.get("file_name","dataset.xlsx")).suffix or ".xlsx"
            candidate = uploads_dir / f"{fh}{suffix}"
            if not candidate.exists():
                # fallback: try original filename
                candidate = uploads_dir / d.get("file_name", "")
            if not candidate.exists():
                continue  # can't install without the source file
            cols = d.get("columns_detected") or {}
            if isinstance(cols, str):
                try:
                    cols = json.loads(cols)
                except Exception:
                    cols = {}
            row = db.create_dataset(
                name=d["name"], file_path=str(candidate),
                n_rows=d["n_rows"], columns_detected=cols,
                image_url_template=d.get("image_url_template"),
            )
            dataset_map[fh] = row["id"]
            new_datasets += 1

    # 5. Correction history merge (append imported entries, renumber versions)
    now = time.time()
    new_corr = merged_corr = 0
    for c in exp_corrections:
        ds_id = dataset_map.get(c["dataset_hash"])
        if ds_id is None:
            continue
        existing = db.get_correction(ds_id, c["image_id"])
        if existing:
            merged_corr += 1
            # Keep local state; history will still record the imported edit below
        else:
            new_corr += 1
            note = ((c.get("note") or "") + " [imported]").strip()
            db.upsert_correction(dataset_id=ds_id, image_id=c["image_id"],
                                 truth_json=c["truth_json"], note=note)

    # Append history entries chronologically
    for h in sorted(exp_history, key=lambda x: x.get("created_at", 0)):
        ds_id = dataset_map.get(h["dataset_hash"])
        if ds_id is None:
            continue
        with db._conn() as c:
            v_row = c.execute(
                "SELECT MAX(version_after) AS v FROM correction_history WHERE dataset_id=?",
                (ds_id,)).fetchone()
            new_v = (v_row["v"] or 0) + 1
            src = h.get("source_machine_id", "?")
            note = ((h.get("note") or "") + f" [from {src[:8]}]").strip()
            c.execute("""
                INSERT INTO correction_history
                  (dataset_id, image_id, truth_json, prev_truth_json, action,
                   version_after, note, created_at)
                VALUES (?,?,?,?,?,?,?,?)
            """, (ds_id, h["image_id"], h.get("truth_json"), h.get("prev_truth_json"),
                  h.get("action", "create"), new_v, note, h.get("created_at", now)))

    # 6. Runs + row results
    rr_by_run: dict[tuple, list] = {}
    for rr in exp_row_results:
        key = (rr["source_machine_id"], rr["source_run_id"])
        rr_by_run.setdefault(key, []).append(rr)

    new_runs = skipped_runs = 0
    for r in exp_runs:
        key = (r["source_machine_id"], r["source_run_id"])
        if db.find_imported_run(r["source_machine_id"], r["source_run_id"]):
            skipped_runs += 1
            continue
        local_pid = prompt_map.get(r.get("prompt_hash", ""))
        local_did = dataset_map.get(r.get("dataset_hash", ""))
        if not local_pid or not local_did:
            skipped_runs += 1
            continue

        config = _jloads(r.get("config") or {})
        # Remove stored api_key from imported config for security
        config.pop("api_key", None)

        new_run_id = db.create_run(
            prompt_id=local_pid, dataset_id=local_did,
            provider=r["provider"], model_id=r["model_id"],
            n_rows=r["n_rows"], config=config,
        )
        db.update_run(
            new_run_id,
            status=r["status"],
            n_done=r.get("n_done", 0),
            accuracy=r.get("accuracy", 0),
            avg_latency_ms=r.get("avg_latency_ms", 0),
            total_input_tokens=r.get("total_input_tokens", 0),
            total_output_tokens=r.get("total_output_tokens", 0),
            total_cost_usd=r.get("total_cost_usd", 0),
            composite_score=r.get("composite_score", 0),
            started_at=r.get("started_at") or now,
            finished_at=r.get("finished_at"),
            error=r.get("error"),
            dataset_version=r.get("dataset_version", 0),
            source_machine_id=r["source_machine_id"],
            source_run_id=r["source_run_id"],
        )

        for rr in rr_by_run.get(key, []):
            try:
                db.add_row_result(
                    run_id=new_run_id, row_idx=rr["row_idx"],
                    latency_ms=rr.get("latency_ms") or 0,
                    input_tokens=rr.get("input_tokens") or 0,
                    output_tokens=rr.get("output_tokens") or 0,
                    cost_usd=rr.get("cost_usd") or 0,
                    output_text=rr.get("output_text") or "",
                    output_parsed=_jloads(rr.get("output_parsed") or {}),
                    scores=_jloads(rr.get("scores") or {}),
                    error=rr.get("error"),
                    image_ref=rr.get("image_ref"),
                    truth=_jloads(rr.get("truth") or {}),
                )
            except Exception:
                pass

        new_runs += 1

    return {
        "applied": True,
        "source_machine_id": manifest.get("source_machine_id", "unknown"),
        "prompts":     {"new": new_prompts,  "matched": matched_prompts},
        "datasets":    {"new": new_datasets, "matched": matched_datasets},
        "runs":        {"new": new_runs,     "skipped": skipped_runs},
        "corrections": {"new": new_corr,     "merged":  merged_corr},
    }
