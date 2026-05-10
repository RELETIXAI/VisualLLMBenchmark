"""Training job manager — subprocess-based for memory isolation.

Why subprocess: mlx-vlm allocates large MLX arrays during training and
the only reliable way to reclaim that memory after a job is to let the
process exit. Running the trainer in-process inside the FastAPI app
would either lock the GIL for hours or leak ~20 GB of unified RAM after
each run. Subprocess gives us isolation, a clean kill path, and a
trivial way to stream progress (parse the log file).

Lifecycle:
    queued  → DB row created, no process yet
    running → subprocess started, pid stored
    completed → process exited 0, at least one .safetensors written
    failed   → process exited non-zero (or no adapter file produced)
    cancelled → user clicked Cancel; we sent SIGTERM

The reaper (`reap_finished`) is called on every poll endpoint to
transition running→completed/failed by checking pid liveness. There is
no background thread — the FastAPI request loop drives state changes.
"""
from __future__ import annotations

import json
import os
import signal
import sqlite3
import subprocess
import sys
import time
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
ADAPTERS_DIR = REPO / "data" / "adapters"
ADAPTERS_DIR.mkdir(parents=True, exist_ok=True)

# Use the same data dir the rest of the app uses (LLMBENCH_DATA_DIR
# override supported, just like backend/db.py).
_DATA_DIR = Path(os.environ.get("LLMBENCH_DATA_DIR")
                 or (REPO / "data"))
DB_PATH = _DATA_DIR / "benchmark.db"


# Defaults tuned for a 48 GB M5 Max with 45 GB wired-RAM ceiling.
# Override per-job via the `config` payload to /api/training/jobs.
DEFAULTS = {
    "lora_rank": 16,
    "lora_alpha": 16,
    "lora_dropout": 0.05,
    "lr": 2e-4,
    "iters": 4000,
    "batch_size": 1,
    "grad_checkpoint": True,
    "seq_len": 1024,
    "save_every": 200,
    "eval_every": 500,
    "freeze_vision": True,
    "freeze_routed_experts": True,
}


# Registry of locally-known training bases. Endpoint
# /api/training/models surfaces these to the frontend Train tab. Adding
# a new entry here is the only step needed to make a new model
# selectable in the UI.
KNOWN_MODELS: list[dict] = [
    {
        "path": "/Users/samueltoma/AI/models/source/google--gemma-4-E4B",
        "label": "Gemma 4 E4B (bf16) — smoke",
        "kind": "smoke",
    },
    {
        "path": "/Users/samueltoma/AI/models/mlx/google--gemma-4-26B-A4B-it",
        "label": "Gemma 4 26B-A4B (4-bit MoE) — production",
        "kind": "production",
    },
    {
        "path": "/Users/samueltoma/AI/models/mlx/gemma-4-31b-it",
        "label": "Gemma 4 31B (4-bit) — alternative",
        "kind": "alternative",
    },
    {
        "path": "/Users/samueltoma/AI/models/mlx/gemma4-e2b",
        "label": "Gemma 4 E2B (4-bit) — micro-smoke",
        "kind": "smoke",
    },
]


def list_models() -> list[dict]:
    """Return KNOWN_MODELS annotated with availability (has weights on disk)."""
    out = []
    for m in KNOWN_MODELS:
        p = Path(m["path"])
        has_weights = p.exists() and (
            any(p.glob("*.safetensors"))
            or (p / "model.safetensors.index.json").exists()
        )
        out.append({**m, "available": has_weights})
    return out


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def _con() -> sqlite3.Connection:
    c = sqlite3.connect(DB_PATH, timeout=30.0)
    c.row_factory = sqlite3.Row
    c.execute("PRAGMA busy_timeout = 30000")
    return c


def list_jobs(limit: int = 50) -> list[dict]:
    with _con() as c:
        rows = c.execute(
            "SELECT * FROM training_jobs ORDER BY id DESC LIMIT ?", (limit,)
        ).fetchall()
        return [dict(r) for r in rows]


def get_job(job_id: int) -> dict | None:
    with _con() as c:
        r = c.execute(
            "SELECT * FROM training_jobs WHERE id=?", (job_id,)
        ).fetchone()
        return dict(r) if r else None


# ---------------------------------------------------------------------------
# Job lifecycle
# ---------------------------------------------------------------------------

def _resolve_dataset_dir(dataset_dir: str) -> Path:
    """Allow callers to pass either an absolute path or a path relative
    to the repo (e.g. 'data/sft')."""
    p = Path(dataset_dir)
    if not p.is_absolute():
        p = REPO / p
    return p.resolve()


def create_job(base_model: str, dataset_dir: str,
               config: dict | None = None) -> int:
    """Insert a new training_jobs row, allocate output paths, return id.

    Validates that the base_model directory exists and the dataset_dir
    contains a train.jsonl. Raises ValueError on either failure so the
    UI can surface the error before a process is spawned.
    """
    if not Path(base_model).exists():
        raise ValueError(f"base_model path does not exist: {base_model}")
    ds_path = _resolve_dataset_dir(dataset_dir)
    if not (ds_path / "train.jsonl").exists():
        raise ValueError(
            f"dataset_dir {ds_path} has no train.jsonl. "
            f"Run scripts/export_sft_dataset.py first."
        )

    cfg = {**DEFAULTS, **(config or {})}
    with _con() as c:
        cur = c.execute(
            "INSERT INTO training_jobs "
            "(status, base_model, config_json, dataset_dir) VALUES (?,?,?,?)",
            ("queued", base_model, json.dumps(cfg), str(ds_path))
        )
        job_id = cur.lastrowid

    # Allocate output paths under data/adapters/<id>-<modelname>/
    job_dir = ADAPTERS_DIR / f"{job_id:04d}-{Path(base_model).name}"
    job_dir.mkdir(parents=True, exist_ok=True)
    log_path = job_dir / "train.log"
    metrics_path = job_dir / "metrics.jsonl"

    with _con() as c:
        c.execute(
            "UPDATE training_jobs "
            "SET adapter_path=?, log_path=?, metrics_path=? WHERE id=?",
            (str(job_dir), str(log_path), str(metrics_path), job_id)
        )
    return int(job_id)


def _prepare_clean_dataset_dir(dataset_dir: str) -> str:
    """Create a /tmp symlink directory containing ONLY train.jsonl.

    mlx_vlm.lora calls load_dataset(path, split="train"). When the SFT
    directory contains other JSON/JSONL files (manifest, review_queue, …)
    HuggingFace's dataset builder reads ALL of them, hits a schema mismatch,
    and crashes. A clean symlink directory with just train.jsonl avoids that.
    """
    import tempfile
    src = Path(dataset_dir) / "train.jsonl"
    # Stable name so we don't litter /tmp with a new dir per job.
    clean_dir = Path(tempfile.gettempdir()) / "llmbench_train_only"
    clean_dir.mkdir(exist_ok=True)
    dst = clean_dir / "train.jsonl"
    if dst.exists() or dst.is_symlink():
        dst.unlink()
    dst.symlink_to(src.resolve())
    return str(clean_dir)


def _build_command(job: dict, cfg: dict) -> list[str]:
    """Construct the mlx_vlm.lora command line for mlx-vlm 0.5+.

    Flag mapping (mlx-vlm 0.5.0):
      --model-path  (not --model)
      --dataset     (not --data / --train)
      --split       (default: train)
      --lora-rank   (not --lora-layers)
      --lora-alpha
      --output-path (not --adapter-path, which means *resume from*)
      --train-on-completions  (mask prompt tokens from loss)

    Less-stable knobs (freeze_vision, freeze_routed_experts, seq_len,
    lora_dropout, eval_every) are written into the sidecar train_config.json
    only; they are not forwarded until the CLI surface stabilises.
    """
    clean_dir = _prepare_clean_dataset_dir(job["dataset_dir"])

    cmd = [
        sys.executable, "-m", "mlx_vlm.lora",
        "--model-path", job["base_model"],
        "--dataset", clean_dir,
        "--split", "train",
        "--iters", str(cfg["iters"]),
        "--batch-size", str(cfg["batch_size"]),
        "--lora-rank", str(cfg["lora_rank"]),
        "--lora-alpha", str(cfg.get("lora_alpha", cfg["lora_rank"])),
        "--learning-rate", str(cfg["lr"]),
        "--output-path", job["adapter_path"],
        "--train-on-completions",
        "--steps-per-report", "20",
    ]
    if cfg.get("save_every"):
        cmd += ["--steps-per-save", str(cfg["save_every"])]
    if cfg.get("grad_checkpoint"):
        cmd.append("--grad-checkpoint")
    return cmd


def start_job(job_id: int) -> int:
    """Spawn the trainer subprocess. Returns the pid.

    The subprocess inherits the current environment plus PYTHONUNBUFFERED=1
    so log lines flush in real time (otherwise mlx-vlm batches them and
    the UI sees nothing for minutes). Stdout + stderr are merged into the
    job's log file so a reader can `tail -f` it.
    """
    job = get_job(job_id)
    if not job:
        raise ValueError(f"job {job_id} not found")
    if job["status"] not in ("queued",):
        raise ValueError(f"job {job_id} status is {job['status']!r}; "
                         f"only 'queued' jobs can be started")
    cfg = json.loads(job["config_json"])

    # Sidecar reproducibility config — captures every knob, even the
    # ones we couldn't pass via CLI.
    sidecar = Path(job["adapter_path"]) / "train_config.json"
    sidecar.write_text(json.dumps({
        "base_model": job["base_model"],
        "dataset_dir": job["dataset_dir"],
        "config": cfg,
        "started_at": time.time(),
    }, indent=2))

    cmd = _build_command(job, cfg)
    log_f = open(job["log_path"], "w", buffering=1)  # line-buffered
    # Best-effort: dump the command itself as the first line of the log
    log_f.write("# " + " ".join(cmd) + "\n")
    log_f.flush()

    proc = subprocess.Popen(
        cmd,
        stdout=log_f,
        stderr=subprocess.STDOUT,
        cwd=str(REPO),
        env={**os.environ, "PYTHONUNBUFFERED": "1"},
        # Detach into its own process group so SIGTERM-on-cancel
        # reaches mlx-vlm and any of its child workers.
        start_new_session=True,
    )

    with _con() as c:
        c.execute(
            "UPDATE training_jobs "
            "SET status='running', pid=?, started_at=? WHERE id=?",
            (proc.pid, time.time(), job_id)
        )
    return int(proc.pid)


def cancel_job(job_id: int) -> bool:
    """Send SIGTERM to a running job's process group. Idempotent."""
    job = get_job(job_id)
    if not job:
        return False
    if job["status"] not in ("running", "paused"):
        return False
    pid = job["pid"]
    if pid is None:
        return False
    try:
        # Kill the whole process group started in start_job
        os.killpg(os.getpgid(pid), signal.SIGTERM)
    except (ProcessLookupError, PermissionError):
        # Process already gone or we don't own it — fall through and
        # mark cancelled anyway.
        pass
    with _con() as c:
        c.execute(
            "UPDATE training_jobs SET status='cancelled', ended_at=? WHERE id=?",
            (time.time(), job_id)
        )
    return True


def _last_saved_step(adapter_path: str) -> int:
    """Scan the adapter directory for numbered checkpoint files and return
    the highest step number found (e.g. 0001500_adapters.safetensors → 1500).
    Falls back to 0 if none found."""
    p = Path(adapter_path)
    if not p.exists():
        return 0
    best = 0
    for f in p.glob("[0-9]*_adapters.safetensors"):
        try:
            step = int(f.stem.split("_")[0])
            best = max(best, step)
        except ValueError:
            pass
    return best


def pause_job(job_id: int) -> int:
    """Send SIGTERM to a running job's process group and mark it 'paused'.

    Returns the step number of the last saved checkpoint so the caller
    can report how far along training was when it was paused.
    """
    job = get_job(job_id)
    if not job:
        raise ValueError(f"job {job_id} not found")
    if job["status"] != "running":
        raise ValueError(f"job {job_id} is not running (status={job['status']!r})")
    pid = job["pid"]
    if pid is None:
        raise ValueError(f"job {job_id} has no pid")

    try:
        os.killpg(os.getpgid(pid), signal.SIGTERM)
    except (ProcessLookupError, PermissionError):
        pass  # Already gone — still mark paused

    step = _last_saved_step(job["adapter_path"] or "")
    with _con() as c:
        c.execute(
            "UPDATE training_jobs "
            "SET status='paused', ended_at=?, paused_at_step=? WHERE id=?",
            (time.time(), step, job_id)
        )
    return step


def resume_job(job_id: int) -> int:
    """Re-spawn a paused training job from its latest checkpoint.

    Calculates remaining iters as (original iters − paused_at_step), then
    launches a new subprocess with --adapter-path pointing at the adapter
    directory (so mlx_vlm loads adapters.safetensors). Updates the existing
    DB row in-place (keeps the same id, same adapter_path).

    Returns the new pid.
    """
    job = get_job(job_id)
    if not job:
        raise ValueError(f"job {job_id} not found")
    if job["status"] != "paused":
        raise ValueError(f"job {job_id} is not paused (status={job['status']!r})")

    cfg = json.loads(job["config_json"])
    total_iters = int(cfg.get("iters", 0))
    paused_step = int(job.get("paused_at_step") or 0)
    remaining   = max(1, total_iters - paused_step)

    # Build the command with remaining iters and adapter-path for resume.
    clean_dir = _prepare_clean_dataset_dir(job["dataset_dir"])
    cmd = [
        sys.executable, "-m", "mlx_vlm.lora",
        "--model-path",    job["base_model"],
        "--dataset",       clean_dir,
        "--split",         "train",
        "--adapter-path",  job["adapter_path"],   # directory — mlx_vlm loads adapters.safetensors
        "--iters",         str(remaining),
        "--batch-size",    str(cfg.get("batch_size", 1)),
        "--lora-rank",     str(cfg.get("lora_rank", 16)),
        "--lora-alpha",    str(cfg.get("lora_alpha", cfg.get("lora_rank", 16))),
        "--learning-rate", str(cfg.get("lr", 2e-4)),
        "--output-path",   job["adapter_path"],
        "--train-on-completions",
        "--steps-per-report", "50",
    ]
    if cfg.get("save_every"):
        cmd += ["--steps-per-save", str(cfg["save_every"])]
    if cfg.get("grad_checkpoint"):
        cmd.append("--grad-checkpoint")

    # Archive the old log so the loss chart starts fresh.
    log_path = _resolve_path(job.get("log_path"))
    if log_path and log_path.exists():
        ts = time.strftime("%Y%m%d-%H%M%S")
        log_path.rename(log_path.with_name(
            log_path.stem + f"-paused-{ts}" + log_path.suffix
        ))
    if log_path:
        log_f = open(log_path, "w", buffering=1)
        log_f.write(f"# resumed from step {paused_step}, remaining={remaining}\n")
        log_f.write("# " + " ".join(cmd) + "\n")
        log_f.flush()
    else:
        log_f = subprocess.DEVNULL  # type: ignore[assignment]

    proc = subprocess.Popen(
        cmd,
        stdout=log_f,
        stderr=subprocess.STDOUT,
        cwd=str(REPO),
        env={**os.environ, "PYTHONUNBUFFERED": "1"},
        start_new_session=True,
    )

    with _con() as c:
        c.execute(
            "UPDATE training_jobs "
            "SET status='running', pid=?, started_at=?, ended_at=NULL WHERE id=?",
            (proc.pid, time.time(), job_id)
        )
    return int(proc.pid)


def reap_finished() -> None:
    """Transition `running` jobs whose pid has exited to `completed`
    (if at least one .safetensors was written) or `failed` (otherwise).

    Called from every poll endpoint so the UI sees state without a
    background reaper thread.

    Defensive cleanup: when the trainer leader pid is gone, we also send
    SIGTERM to its old process group. mlx-vlm's HuggingFace dataset
    loader uses `multiprocessing.spawn` for DataLoader workers, and on
    natural exit those workers can survive the leader and burn CPU in a
    closed-pipe loop, holding GPU/Metal state. Since we put the trainer
    in its own session via start_new_session=True, killpg(pid, TERM)
    here only targets that one trainer's group — never our own.
    """
    with _con() as c:
        rows = c.execute(
            "SELECT id, pid, adapter_path FROM training_jobs WHERE status='running'"
        ).fetchall()
    for r in rows:
        pid = r["pid"]
        if pid is None:
            continue
        try:
            os.kill(pid, 0)
            # Still alive — leave it.
            continue
        except ProcessLookupError:
            pass
        except PermissionError:
            # Some other process owns this pid now; treat as gone.
            pass
        # Leader is gone. Mop up any surviving group members. Group
        # still exists as long as it has any member, even after the
        # leader's death. Best-effort: silently ignore if the group is
        # already empty or owned by someone else.
        try:
            os.killpg(pid, signal.SIGTERM)
        except (ProcessLookupError, PermissionError, OSError):
            pass
        adapter_files = []
        try:
            adapter_files = list(Path(r["adapter_path"]).glob("**/*.safetensors"))
        except Exception:
            pass
        status = "completed" if adapter_files else "failed"
        with _con() as c:
            c.execute(
                "UPDATE training_jobs SET status=?, ended_at=? WHERE id=?",
                (status, time.time(), r["id"])
            )


# ---------------------------------------------------------------------------
# Logs / metrics
# ---------------------------------------------------------------------------

_ANSI_RE  = __import__("re").compile(r"\x1b\[[0-9;]*m")
_ITER_RE  = __import__("re").compile(
    r"Iter\s+(\d+):\s+Train loss\s+([\d.]+)"
    r".*?Learning Rate\s+([\d.e+\-]+)"   # stop before the comma
    r".*?It/sec\s+([\d.]+)"
    r".*?Tokens/sec\s+([\d.]+)"
    r".*?Peak mem\s+([\d.]+)\s*GB"
)


def _resolve_path(raw: str | None) -> Path | None:
    """Return an absolute Path, resolving relative paths against REPO."""
    if not raw:
        return None
    p = Path(raw)
    return p if p.is_absolute() else (REPO / p)


def read_metrics(job_id: int, since: int = 0) -> list[dict]:
    """Parse per-iter metrics from the training log.

    mlx-vlm 0.5+ writes lines like:
      Iter 50: Train loss 7.35, Learning Rate 2.000e-04, It/sec 1.13, ...

    We parse these instead of relying on a metrics.jsonl (which 0.5 doesn't
    produce). Falls back to [] if the log is missing or unparseable.
    """
    job = get_job(job_id)
    if not job or not job.get("log_path"):
        return []
    p = _resolve_path(job["log_path"])
    if not p.exists():
        return []
    out = []
    try:
        for raw in p.read_text(errors="replace").splitlines():
            line = _ANSI_RE.sub("", raw)   # strip colour codes before parsing
            m = _ITER_RE.search(line)
            if not m:
                continue
            step = int(m.group(1))
            if step <= since:
                continue
            out.append({
                "step":       step,
                "loss":       float(m.group(2)),
                "lr":         float(m.group(3)),
                "it_per_sec": float(m.group(4)),
                "tok_per_sec": float(m.group(5)),
                "peak_mem_gb": float(m.group(6)),
            })
    except Exception:
        pass
    return out


def tail_log(job_id: int, n: int = 200) -> str:
    job = get_job(job_id)
    if not job or not job.get("log_path"):
        return ""
    p = _resolve_path(job["log_path"])
    if not p.exists():
        return ""
    try:
        raw = p.read_text(errors="replace")
        clean = _ANSI_RE.sub("", raw)
        lines = clean.splitlines()
    except Exception:
        return ""
    return "\n".join(lines[-n:])


def adapter_size_mb(adapter_path: str | None) -> float:
    """Return total MB of .safetensors files in an adapter directory."""
    if not adapter_path:
        return 0.0
    p = _resolve_path(adapter_path)
    if not p or not p.exists():
        return 0.0
    total = sum(f.stat().st_size for f in p.glob("*.safetensors") if f.is_file())
    return round(total / 1_048_576, 1)


# ---------------------------------------------------------------------------
# Adapter discovery (for the benchmark "use this adapter" picker)
# ---------------------------------------------------------------------------

def list_adapters() -> list[dict]:
    """Return every directory under data/adapters/ that contains
    .safetensors weights. Used by the frontend to populate the
    benchmark form's adapter dropdown.
    """
    out = []
    if not ADAPTERS_DIR.exists():
        return out
    for child in sorted(ADAPTERS_DIR.iterdir()):
        if not child.is_dir():
            continue
        weights = list(child.glob("**/*.safetensors"))
        if not weights:
            continue
        cfg_path = child / "train_config.json"
        cfg = {}
        if cfg_path.exists():
            try:
                cfg = json.loads(cfg_path.read_text())
            except Exception:
                pass
        out.append({
            "path": str(child),
            "name": child.name,
            "n_weights": len(weights),
            "base_model": cfg.get("base_model"),
            "iters": (cfg.get("config") or {}).get("iters"),
        })
    return out
