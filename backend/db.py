"""SQLite store for prompts, datasets, runs, results."""
from __future__ import annotations

import json
import sqlite3
import time
from pathlib import Path
from typing import Any, Optional

DB_PATH = Path(__file__).resolve().parent.parent / "data" / "benchmark.db"


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
    with _conn() as c:
        cur = c.execute(
            "INSERT INTO prompts (name, system_prompt, description, created_at) VALUES (?,?,?,?)",
            (name, system_prompt, description, time.time()),
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
    with _conn() as c:
        cur = c.execute(
            "INSERT INTO datasets (name, file_path, n_rows, columns_detected, image_url_template, created_at)"
            " VALUES (?,?,?,?,?,?)",
            (name, file_path, n_rows, json.dumps(columns_detected), image_url_template, time.time()),
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
