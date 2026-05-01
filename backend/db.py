"""SQLite store for prompts, datasets, runs, results."""
from __future__ import annotations

import hashlib
import json
import sqlite3
import time
import uuid
from pathlib import Path
from typing import Any, Optional

DB_PATH = Path(__file__).resolve().parent.parent / "data" / "benchmark.db"
_MACHINE_ID_FILE = Path(__file__).resolve().parent.parent / "data" / "machine_id.txt"


def get_machine_id() -> str:
    """Stable UUID for this installation, persisted to data/machine_id.txt."""
    if _MACHINE_ID_FILE.exists():
        mid = _MACHINE_ID_FILE.read_text().strip()
        if mid:
            return mid
    mid = str(uuid.uuid4())
    _MACHINE_ID_FILE.parent.mkdir(parents=True, exist_ok=True)
    _MACHINE_ID_FILE.write_text(mid)
    return mid


def hash_file(path: str | Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def hash_prompt(name: str, system_prompt: str) -> str:
    text = name.strip() + "\n" + system_prompt.strip().replace("\r\n", "\n")
    return hashlib.sha256(text.encode()).hexdigest()


def _conn() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    c = sqlite3.connect(DB_PATH)
    c.row_factory = sqlite3.Row
    c.execute("PRAGMA foreign_keys = ON")
    return c


def init_db() -> None:
    with _conn() as c:
        # Sweep orphaned in-flight runs on startup. The runner thread + in-memory
        # controls are gone, so anything left as running/paused/pending is dead.
        try:
            c.execute("""
                UPDATE runs
                SET status = 'cancelled',
                    finished_at = COALESCE(finished_at, ?),
                    error = COALESCE(error, 'server restarted while run was in flight')
                WHERE status IN ('running','paused','pending')
            """, (time.time(),))
        except Exception:
            pass
        # Corrections table: current state — per-(dataset, image_id) overlay
        # applied at parse time. Source Excel is never modified.
        c.execute("""
            CREATE TABLE IF NOT EXISTS corrections (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                dataset_id INTEGER NOT NULL,
                image_id TEXT NOT NULL,
                truth_json TEXT NOT NULL,
                source_run_id INTEGER,
                source_row_idx INTEGER,
                note TEXT,
                created_at REAL NOT NULL,
                UNIQUE(dataset_id, image_id)
            )
        """)
        c.execute("CREATE INDEX IF NOT EXISTS idx_corrections_ds ON corrections(dataset_id)")

        # Append-only audit log of every change to the truth, per dataset.
        # version_after is a per-dataset monotonic counter (v0 = original Excel,
        # each save/delete/restore increments). action is one of:
        #   create / update / delete / restore
        c.execute("""
            CREATE TABLE IF NOT EXISTS correction_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                dataset_id INTEGER NOT NULL,
                image_id TEXT NOT NULL,
                truth_json TEXT,
                prev_truth_json TEXT,
                action TEXT NOT NULL,
                version_after INTEGER NOT NULL,
                source_run_id INTEGER,
                source_row_idx INTEGER,
                note TEXT,
                created_at REAL NOT NULL
            )
        """)
        c.execute("CREATE INDEX IF NOT EXISTS idx_history_ds  ON correction_history(dataset_id)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_history_img ON correction_history(dataset_id, image_id)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_history_v   ON correction_history(dataset_id, version_after)")

        # Backfill: any corrections row without a history entry gets one (version 1+).
        try:
            orphans = c.execute("""
                SELECT c.* FROM corrections c
                LEFT JOIN correction_history h
                  ON h.dataset_id = c.dataset_id AND h.image_id = c.image_id
                WHERE h.id IS NULL
                ORDER BY c.dataset_id, c.created_at, c.id
            """).fetchall()
            cursor_versions: dict[int, int] = {}
            for row in orphans:
                ds = row["dataset_id"]
                if ds not in cursor_versions:
                    r = c.execute(
                        "SELECT MAX(version_after) AS v FROM correction_history WHERE dataset_id=?",
                        (ds,)).fetchone()
                    cursor_versions[ds] = (r["v"] or 0)
                cursor_versions[ds] += 1
                c.execute("""
                    INSERT INTO correction_history
                      (dataset_id, image_id, truth_json, prev_truth_json, action,
                       version_after, source_run_id, source_row_idx, note, created_at)
                    VALUES (?,?,?,NULL,?,?,?,?,?,?)
                """, (ds, row["image_id"], row["truth_json"], "create",
                      cursor_versions[ds], row["source_run_id"],
                      row["source_row_idx"], row["note"] or "(backfilled)",
                      row["created_at"]))
        except Exception:
            pass

        # Migration: runs.dataset_version
        try:
            cols = [r[1] for r in c.execute("PRAGMA table_info(runs)").fetchall()]
            if cols and "dataset_version" not in cols:
                c.execute("ALTER TABLE runs ADD COLUMN dataset_version INTEGER DEFAULT 0")
        except Exception:
            pass
        # cheap migrations
        try:
            cols = [r[1] for r in c.execute("PRAGMA table_info(datasets)").fetchall()]
            if cols and "image_url_template" not in cols:
                c.execute("ALTER TABLE datasets ADD COLUMN image_url_template TEXT")
        except Exception:
            pass
        try:
            cols = [r[1] for r in c.execute("PRAGMA table_info(row_results)").fetchall()]
            if cols and "image_ref" not in cols:
                c.execute("ALTER TABLE row_results ADD COLUMN image_ref TEXT")
            if cols and "truth" not in cols:
                c.execute("ALTER TABLE row_results ADD COLUMN truth TEXT")
        except Exception:
            pass

        # Migration: prompts.content_hash
        try:
            cols = [r[1] for r in c.execute("PRAGMA table_info(prompts)").fetchall()]
            if cols and "content_hash" not in cols:
                c.execute("ALTER TABLE prompts ADD COLUMN content_hash TEXT")
                for row in c.execute("SELECT id, name, system_prompt FROM prompts").fetchall():
                    h = hash_prompt(row["name"], row["system_prompt"])
                    c.execute("UPDATE prompts SET content_hash=? WHERE id=?", (h, row["id"]))
                c.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_prompts_hash ON prompts(content_hash)")
        except Exception:
            pass

        # Migration: datasets.file_hash
        try:
            cols = [r[1] for r in c.execute("PRAGMA table_info(datasets)").fetchall()]
            if cols and "file_hash" not in cols:
                c.execute("ALTER TABLE datasets ADD COLUMN file_hash TEXT")
                for row in c.execute("SELECT id, file_path FROM datasets").fetchall():
                    try:
                        h = hash_file(row["file_path"])
                        c.execute("UPDATE datasets SET file_hash=? WHERE id=?", (h, row["id"]))
                    except Exception:
                        pass
                c.execute("CREATE INDEX IF NOT EXISTS idx_datasets_hash ON datasets(file_hash)")
        except Exception:
            pass

        # Migration: runs.source_machine_id + source_run_id (for import tracking)
        try:
            cols = [r[1] for r in c.execute("PRAGMA table_info(runs)").fetchall()]
            if cols and "source_machine_id" not in cols:
                c.execute("ALTER TABLE runs ADD COLUMN source_machine_id TEXT")
                c.execute("ALTER TABLE runs ADD COLUMN source_run_id INTEGER")
                c.execute("""
                    CREATE UNIQUE INDEX IF NOT EXISTS idx_runs_import_source
                    ON runs(source_machine_id, source_run_id)
                    WHERE source_machine_id IS NOT NULL
                """)
        except Exception:
            pass
        c.executescript("""
        CREATE TABLE IF NOT EXISTS prompts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            system_prompt TEXT NOT NULL,
            description TEXT,
            created_at REAL NOT NULL
        );
        CREATE TABLE IF NOT EXISTS datasets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            file_path TEXT NOT NULL,
            n_rows INTEGER NOT NULL,
            columns_detected TEXT,
            image_url_template TEXT,
            created_at REAL NOT NULL
        );
        CREATE TABLE IF NOT EXISTS runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            prompt_id INTEGER NOT NULL,
            dataset_id INTEGER NOT NULL,
            provider TEXT NOT NULL,
            model_id TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            n_rows INTEGER NOT NULL,
            n_done INTEGER NOT NULL DEFAULT 0,
            accuracy REAL DEFAULT 0,
            avg_latency_ms REAL DEFAULT 0,
            total_input_tokens INTEGER DEFAULT 0,
            total_output_tokens INTEGER DEFAULT 0,
            total_cost_usd REAL DEFAULT 0,
            composite_score REAL DEFAULT 0,
            config TEXT,
            started_at REAL NOT NULL,
            finished_at REAL,
            error TEXT,
            FOREIGN KEY (prompt_id) REFERENCES prompts(id),
            FOREIGN KEY (dataset_id) REFERENCES datasets(id)
        );
        CREATE TABLE IF NOT EXISTS row_results (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id INTEGER NOT NULL,
            row_idx INTEGER NOT NULL,
            latency_ms REAL,
            input_tokens INTEGER,
            output_tokens INTEGER,
            cost_usd REAL,
            output_text TEXT,
            output_parsed TEXT,
            scores TEXT,
            error TEXT,
            image_ref TEXT,
            truth TEXT,
            FOREIGN KEY (run_id) REFERENCES runs(id) ON DELETE CASCADE
        );
        CREATE INDEX IF NOT EXISTS idx_row_results_run ON row_results(run_id);
        CREATE INDEX IF NOT EXISTS idx_runs_prompt ON runs(prompt_id);
        """)


# ----- prompts -----
def list_prompts() -> list[dict]:
    with _conn() as c:
        rows = c.execute("SELECT * FROM prompts ORDER BY id DESC").fetchall()
        return [dict(r) for r in rows]


def create_prompt(name: str, system_prompt: str, description: str | None = None) -> dict:
    ch = hash_prompt(name, system_prompt)
    with _conn() as c:
        cur = c.execute(
            "INSERT INTO prompts (name, system_prompt, description, created_at, content_hash) VALUES (?,?,?,?,?)",
            (name, system_prompt, description, time.time(), ch),
        )
        return dict(c.execute("SELECT * FROM prompts WHERE id=?", (cur.lastrowid,)).fetchone())


def get_prompt(prompt_id: int) -> dict | None:
    with _conn() as c:
        r = c.execute("SELECT * FROM prompts WHERE id=?", (prompt_id,)).fetchone()
        return dict(r) if r else None


def delete_prompt(prompt_id: int) -> None:
    with _conn() as c:
        c.execute("DELETE FROM prompts WHERE id=?", (prompt_id,))


# ----- datasets -----
def create_dataset(name: str, file_path: str, n_rows: int, columns_detected: dict,
                   image_url_template: str | None = None) -> dict:
    try:
        fh = hash_file(file_path)
    except Exception:
        fh = None
    with _conn() as c:
        cur = c.execute(
            "INSERT INTO datasets (name, file_path, n_rows, columns_detected, image_url_template, created_at, file_hash)"
            " VALUES (?,?,?,?,?,?,?)",
            (name, file_path, n_rows, json.dumps(columns_detected), image_url_template, time.time(), fh),
        )
        return dict(c.execute("SELECT * FROM datasets WHERE id=?", (cur.lastrowid,)).fetchone())


def list_datasets() -> list[dict]:
    with _conn() as c:
        rows = c.execute("SELECT * FROM datasets ORDER BY id DESC").fetchall()
        return [dict(r) for r in rows]


def get_dataset(dataset_id: int) -> dict | None:
    with _conn() as c:
        r = c.execute("SELECT * FROM datasets WHERE id=?", (dataset_id,)).fetchone()
        return dict(r) if r else None


def update_dataset(dataset_id: int, **fields) -> dict | None:
    if not fields:
        return get_dataset(dataset_id)
    cols = ", ".join(f"{k}=?" for k in fields.keys())
    vals = list(fields.values()) + [dataset_id]
    with _conn() as c:
        c.execute(f"UPDATE datasets SET {cols} WHERE id=?", vals)
    return get_dataset(dataset_id)


def delete_dataset(dataset_id: int) -> dict:
    """Cascade-delete: dataset + all runs + all row_results, plus the file on disk."""
    from pathlib import Path
    with _conn() as c:
        ds = c.execute("SELECT * FROM datasets WHERE id=?", (dataset_id,)).fetchone()
        if not ds:
            return {"deleted": False, "reason": "not found"}
        runs = c.execute("SELECT id FROM runs WHERE dataset_id=?", (dataset_id,)).fetchall()
        for run in runs:
            c.execute("DELETE FROM row_results WHERE run_id=?", (run["id"],))
        c.execute("DELETE FROM runs WHERE dataset_id=?", (dataset_id,))
        c.execute("DELETE FROM datasets WHERE id=?", (dataset_id,))
        try:
            p = Path(ds["file_path"])
            if p.exists():
                p.unlink()
        except Exception:
            pass
        return {"deleted": True, "runs_removed": len(runs)}


# ----- runs -----
def create_run(prompt_id: int, dataset_id: int, provider: str, model_id: str,
               n_rows: int, config: dict) -> int:
    with _conn() as c:
        cur = c.execute(
            "INSERT INTO runs (prompt_id, dataset_id, provider, model_id, n_rows, config, started_at, status)"
            " VALUES (?,?,?,?,?,?,?,?)",
            (prompt_id, dataset_id, provider, model_id, n_rows, json.dumps(config), time.time(), "running"),
        )
        return int(cur.lastrowid)


def update_run(run_id: int, **fields) -> None:
    if not fields:
        return
    cols = ", ".join(f"{k}=?" for k in fields.keys())
    vals = list(fields.values())
    vals.append(run_id)
    with _conn() as c:
        c.execute(f"UPDATE runs SET {cols} WHERE id=?", vals)


def add_row_result(run_id: int, row_idx: int, latency_ms: float, input_tokens: int,
                   output_tokens: int, cost_usd: float, output_text: str,
                   output_parsed: dict, scores: dict, error: Optional[str],
                   image_ref: Optional[str] = None, truth: Optional[dict] = None) -> None:
    with _conn() as c:
        c.execute(
            "INSERT INTO row_results (run_id,row_idx,latency_ms,input_tokens,output_tokens,cost_usd,"
            "output_text,output_parsed,scores,error,image_ref,truth) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            (run_id, row_idx, latency_ms, input_tokens, output_tokens, cost_usd,
             output_text, json.dumps(output_parsed), json.dumps(scores), error,
             image_ref, json.dumps(truth) if truth is not None else None),
        )


def get_run_status(run_id: int) -> Optional[str]:
    """Cheap status-only read — used by the runner to detect external cancellation."""
    with _conn() as c:
        r = c.execute("SELECT status FROM runs WHERE id=?", (run_id,)).fetchone()
        return r["status"] if r else None


def get_run(run_id: int) -> dict | None:
    with _conn() as c:
        r = c.execute("SELECT * FROM runs WHERE id=?", (run_id,)).fetchone()
        if not r:
            return None
        d = dict(r)
        rows = c.execute("SELECT * FROM row_results WHERE run_id=? ORDER BY row_idx", (run_id,)).fetchall()
        d["rows"] = [dict(x) for x in rows]
        return d


def list_runs(prompt_id: Optional[int] = None,
              model_id: Optional[str] = None,
              provider: Optional[str] = None,
              dataset_id: Optional[int] = None,
              status: Optional[str] = None) -> list[dict]:
    where, args = [], []
    if prompt_id is not None:  where.append("r.prompt_id=?");  args.append(prompt_id)
    if model_id  is not None:  where.append("r.model_id=?");   args.append(model_id)
    if provider  is not None:  where.append("r.provider=?");   args.append(provider)
    if dataset_id is not None: where.append("r.dataset_id=?"); args.append(dataset_id)
    if status    is not None:  where.append("r.status=?");     args.append(status)
    sql = ("SELECT r.*, p.name AS prompt_name, d.name AS dataset_name "
           "FROM runs r JOIN prompts p ON r.prompt_id=p.id "
           "JOIN datasets d ON r.dataset_id=d.id ")
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY r.id DESC"
    with _conn() as c:
        rows = c.execute(sql, args).fetchall()
        return [dict(r) for r in rows]


def list_runs_by_ids(ids: list[int]) -> list[dict]:
    if not ids:
        return []
    placeholders = ",".join("?" for _ in ids)
    with _conn() as c:
        rows = c.execute(
            f"SELECT r.*, p.name AS prompt_name, p.system_prompt AS prompt_text, "
            f"       d.name AS dataset_name "
            f"FROM runs r JOIN prompts p ON r.prompt_id=p.id "
            f"JOIN datasets d ON r.dataset_id=d.id "
            f"WHERE r.id IN ({placeholders}) ORDER BY r.id",
            ids).fetchall()
        return [dict(r) for r in rows]


def find_prompt_by_text(text: str) -> Optional[dict]:
    with _conn() as c:
        r = c.execute("SELECT * FROM prompts WHERE system_prompt=? ORDER BY id DESC LIMIT 1",
                      (text,)).fetchone()
        return dict(r) if r else None


def find_prompt_by_hash(content_hash: str) -> Optional[dict]:
    with _conn() as c:
        r = c.execute("SELECT * FROM prompts WHERE content_hash=? LIMIT 1",
                      (content_hash,)).fetchone()
        return dict(r) if r else None


def find_dataset_by_hash(file_hash: str) -> Optional[dict]:
    with _conn() as c:
        r = c.execute("SELECT * FROM datasets WHERE file_hash=? LIMIT 1",
                      (file_hash,)).fetchone()
        return dict(r) if r else None


def find_imported_run(source_machine_id: str, source_run_id: int) -> Optional[dict]:
    with _conn() as c:
        r = c.execute(
            "SELECT * FROM runs WHERE source_machine_id=? AND source_run_id=?",
            (source_machine_id, source_run_id)).fetchone()
        return dict(r) if r else None


def find_dataset_by_name(name: str) -> Optional[dict]:
    with _conn() as c:
        r = c.execute("SELECT * FROM datasets WHERE name=? ORDER BY id DESC LIMIT 1",
                      (name,)).fetchone()
        return dict(r) if r else None


def delete_run(run_id: int) -> dict:
    """Delete a run and all its row results.  Dataset and prompt are kept."""
    with _conn() as c:
        r = c.execute("SELECT id FROM runs WHERE id=?", (run_id,)).fetchone()
        if not r:
            return {"deleted": False, "reason": "not found"}
        c.execute("DELETE FROM row_results WHERE run_id=?", (run_id,))
        c.execute("DELETE FROM runs WHERE id=?", (run_id,))
        return {"deleted": True, "run_id": run_id}


def current_dataset_version(dataset_id: int) -> int:
    """Latest version number for this dataset (max version_after in history)."""
    with _conn() as c:
        r = c.execute(
            "SELECT MAX(version_after) AS v FROM correction_history WHERE dataset_id=?",
            (dataset_id,)).fetchone()
        return int(r["v"] or 0) if r else 0


def list_dataset_versions(dataset_id: int) -> list[dict]:
    """Every history entry, oldest-first. v0 (the source Excel) is implicit."""
    with _conn() as c:
        rows = c.execute("""
            SELECT * FROM correction_history WHERE dataset_id=?
            ORDER BY version_after, id
        """, (dataset_id,)).fetchall()
        return [dict(r) for r in rows]


def list_image_history(dataset_id: int, image_id: str) -> list[dict]:
    with _conn() as c:
        rows = c.execute("""
            SELECT * FROM correction_history
            WHERE dataset_id=? AND image_id=?
            ORDER BY version_after, id
        """, (dataset_id, image_id)).fetchall()
        return [dict(r) for r in rows]


def replay_history(dataset_id: int, at_version: int) -> dict:
    """Walk history up to and including at_version, return
    {image_id: <truth dict>}. Items deleted by that version are absent."""
    import json as _json
    state: dict = {}
    with _conn() as c:
        rows = c.execute("""
            SELECT image_id, truth_json, action FROM correction_history
            WHERE dataset_id=? AND version_after<=?
            ORDER BY version_after, id
        """, (dataset_id, at_version)).fetchall()
    for r in rows:
        if r["action"] == "delete":
            state.pop(r["image_id"], None)
        else:
            try:
                state[r["image_id"]] = _json.loads(r["truth_json"] or "{}")
            except Exception:
                pass
    return state


def list_corrections(dataset_id: int) -> list[dict]:
    with _conn() as c:
        rows = c.execute(
            "SELECT * FROM corrections WHERE dataset_id=? ORDER BY id DESC",
            (dataset_id,)).fetchall()
        return [dict(r) for r in rows]


def get_correction(dataset_id: int, image_id: str) -> Optional[dict]:
    with _conn() as c:
        r = c.execute(
            "SELECT * FROM corrections WHERE dataset_id=? AND image_id=?",
            (dataset_id, image_id)).fetchone()
        return dict(r) if r else None


def upsert_correction(dataset_id: int, image_id: str, truth_json: str,
                      source_run_id: Optional[int] = None,
                      source_row_idx: Optional[int] = None,
                      note: Optional[str] = None) -> dict:
    """Save a correction AND append to history, bumping dataset_version by 1."""
    now = time.time()
    with _conn() as c:
        # Read previous state (for diff history)
        prev = c.execute(
            "SELECT truth_json FROM corrections WHERE dataset_id=? AND image_id=?",
            (dataset_id, image_id)).fetchone()
        prev_truth = prev["truth_json"] if prev else None
        action = "update" if prev else "create"
        # Compute new version inside the transaction
        v_row = c.execute(
            "SELECT MAX(version_after) AS v FROM correction_history WHERE dataset_id=?",
            (dataset_id,)).fetchone()
        new_version = (v_row["v"] or 0) + 1
        # Append history
        c.execute("""
            INSERT INTO correction_history
              (dataset_id, image_id, truth_json, prev_truth_json, action,
               version_after, source_run_id, source_row_idx, note, created_at)
            VALUES (?,?,?,?,?,?,?,?,?,?)
        """, (dataset_id, image_id, truth_json, prev_truth, action,
              new_version, source_run_id, source_row_idx, note, now))
        # Upsert current state
        c.execute("""
            INSERT INTO corrections (dataset_id, image_id, truth_json,
                                     source_run_id, source_row_idx, note, created_at)
            VALUES (?,?,?,?,?,?,?)
            ON CONFLICT(dataset_id, image_id) DO UPDATE SET
                truth_json = excluded.truth_json,
                source_run_id = excluded.source_run_id,
                source_row_idx = excluded.source_row_idx,
                note = excluded.note,
                created_at = excluded.created_at
        """, (dataset_id, image_id, truth_json, source_run_id, source_row_idx,
              note, now))
        r = c.execute(
            "SELECT * FROM corrections WHERE dataset_id=? AND image_id=?",
            (dataset_id, image_id)).fetchone()
        out = dict(r)
        out["dataset_version"] = new_version
        out["action"] = action
        return out


def delete_correction(correction_id: int) -> dict:
    """Delete a correction AND append to history with action='delete'."""
    now = time.time()
    with _conn() as c:
        r = c.execute("SELECT * FROM corrections WHERE id=?", (correction_id,)).fetchone()
        if not r:
            return {"deleted": False, "reason": "not found"}
        ds = r["dataset_id"]; img = r["image_id"]; prev_truth = r["truth_json"]
        v_row = c.execute(
            "SELECT MAX(version_after) AS v FROM correction_history WHERE dataset_id=?",
            (ds,)).fetchone()
        new_version = (v_row["v"] or 0) + 1
        c.execute("""
            INSERT INTO correction_history
              (dataset_id, image_id, truth_json, prev_truth_json, action,
               version_after, note, created_at)
            VALUES (?,?,NULL,?,?,?,?,?)
        """, (ds, img, prev_truth, "delete", new_version, "deleted", now))
        c.execute("DELETE FROM corrections WHERE id=?", (correction_id,))
        return {"deleted": True, "correction_id": correction_id,
                "dataset_id": ds, "dataset_version": new_version}


def restore_dataset_to_version(dataset_id: int, target_version: int,
                                note: Optional[str] = None) -> dict:
    """Roll the active corrections set forward/backward to match target_version.

    Walks the diff between current state and target state, writes one history
    entry per affected image_id (action='restore'), and rewrites the
    corrections table. Bumps version exactly once (single 'restore' op).
    """
    import json as _json
    now = time.time()
    note = note or f"restore to v{target_version}"
    with _conn() as c:
        # Snapshot of current corrections
        cur_rows = c.execute(
            "SELECT image_id, truth_json FROM corrections WHERE dataset_id=?",
            (dataset_id,)).fetchall()
        cur = {r["image_id"]: r["truth_json"] for r in cur_rows}
    target = replay_history(dataset_id, target_version)
    target_json = {k: __import__("json").dumps(v) for k, v in target.items()}

    affected_images: set = set(cur.keys()) | set(target_json.keys())
    if not affected_images:
        return {"restored": True, "dataset_id": dataset_id,
                "target_version": target_version, "new_version": current_dataset_version(dataset_id),
                "n_changed": 0}

    with _conn() as c:
        v_row = c.execute(
            "SELECT MAX(version_after) AS v FROM correction_history WHERE dataset_id=?",
            (dataset_id,)).fetchone()
        new_version = (v_row["v"] or 0) + 1
        n_changed = 0
        for img in sorted(affected_images):
            cur_t = cur.get(img)
            tgt_t = target_json.get(img)
            if cur_t == tgt_t:
                continue
            n_changed += 1
            # Write history
            c.execute("""
                INSERT INTO correction_history
                  (dataset_id, image_id, truth_json, prev_truth_json, action,
                   version_after, note, created_at)
                VALUES (?,?,?,?,?,?,?,?)
            """, (dataset_id, img, tgt_t, cur_t, "restore",
                  new_version, note, now))
            # Apply to corrections table
            if tgt_t is None:
                c.execute("DELETE FROM corrections WHERE dataset_id=? AND image_id=?",
                          (dataset_id, img))
            else:
                c.execute("""
                    INSERT INTO corrections (dataset_id, image_id, truth_json, created_at)
                    VALUES (?,?,?,?)
                    ON CONFLICT(dataset_id, image_id) DO UPDATE SET
                        truth_json = excluded.truth_json,
                        created_at = excluded.created_at
                """, (dataset_id, img, tgt_t, now))
        if n_changed == 0:
            new_version -= 1   # nothing changed; don't bump version
    return {"restored": True, "dataset_id": dataset_id,
            "target_version": target_version, "new_version": new_version,
            "n_changed": n_changed}


def get_row_result(run_id: int, row_idx: int) -> Optional[dict]:
    with _conn() as c:
        r = c.execute(
            "SELECT * FROM row_results WHERE run_id=? AND row_idx=?",
            (run_id, row_idx)).fetchone()
        return dict(r) if r else None


def upsert_row_result(run_id: int, row_idx: int, latency_ms: float, input_tokens: int,
                      output_tokens: int, cost_usd: float, output_text: str,
                      output_parsed: dict, scores: dict, error: Optional[str],
                      image_ref: Optional[str] = None, truth: Optional[dict] = None) -> None:
    existing = get_row_result(run_id, row_idx)
    if existing:
        with _conn() as c:
            c.execute("""
                UPDATE row_results SET latency_ms=?, input_tokens=?, output_tokens=?,
                    cost_usd=?, output_text=?, output_parsed=?, scores=?, error=?,
                    image_ref=?, truth=?
                WHERE run_id=? AND row_idx=?
            """, (latency_ms, input_tokens, output_tokens, cost_usd,
                  output_text, json.dumps(output_parsed), json.dumps(scores), error,
                  image_ref, json.dumps(truth) if truth is not None else None,
                  run_id, row_idx))
    else:
        add_row_result(run_id, row_idx, latency_ms=latency_ms, input_tokens=input_tokens,
                       output_tokens=output_tokens, cost_usd=cost_usd, output_text=output_text,
                       output_parsed=output_parsed, scores=scores, error=error,
                       image_ref=image_ref, truth=truth)


def recalc_run_stats(run_id: int) -> None:
    """Recompute run-level aggregates from all row_results (used after single-row retry)."""
    with _conn() as c:
        rows = c.execute(
            "SELECT latency_ms, scores, input_tokens, output_tokens, cost_usd, error "
            "FROM row_results WHERE run_id=?", (run_id,)).fetchall()
    if not rows:
        return
    latencies, accs = [], []
    total_in = total_out = 0
    total_cost = 0.0
    for row in rows:
        sc = json.loads(row["scores"] or "{}")
        if not row["error"]:
            latencies.append(row["latency_ms"] or 0)
            accs.append(sc.get("overall", 0))
        total_in += row["input_tokens"] or 0
        total_out += row["output_tokens"] or 0
        total_cost += row["cost_usd"] or 0.0
    avg_lat = sum(latencies) / len(latencies) if latencies else 0
    acc = sum(accs) / len(accs) if accs else 0
    from .scoring import composite_score
    comp = composite_score(acc, avg_lat, total_cost, None)
    update_run(run_id, accuracy=acc, avg_latency_ms=avg_lat, n_done=len(rows),
               total_input_tokens=total_in, total_output_tokens=total_out,
               total_cost_usd=total_cost, composite_score=comp)


def leaderboard(prompt_id: int) -> list[dict]:
    """Best run per (provider, model) for this prompt, sorted by composite_score."""
    with _conn() as c:
        rows = c.execute("""
            SELECT r.*, p.name AS prompt_name, d.name AS dataset_name FROM runs r
            JOIN prompts p ON r.prompt_id=p.id
            JOIN datasets d ON r.dataset_id=d.id
            WHERE r.prompt_id=? AND r.status='completed'
            ORDER BY r.composite_score DESC, r.accuracy DESC
        """, (prompt_id,)).fetchall()
        seen = {}
        for r in rows:
            key = (r["provider"], r["model_id"])
            if key not in seen:
                seen[key] = dict(r)
        return list(seen.values())
