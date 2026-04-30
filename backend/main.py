"""FastAPI server: REST API + static frontend."""
from __future__ import annotations

import json as _json
import os
import shutil
from collections import Counter
from pathlib import Path
from typing import Any, Optional

from dotenv import load_dotenv
from fastapi import FastAPI, File, Form, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, PlainTextResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from . import db, runner
from .parser import parse_dataset
from .pricing import PRICING
from .providers import PROVIDERS
from .models_registry import PROVIDER_MODELS, models_for

load_dotenv()

ROOT = Path(__file__).resolve().parent.parent
UPLOAD_DIR = ROOT / "data" / "uploads"
IMAGES_DIR = ROOT / "data" / "images"
FRONTEND_DIR = ROOT / "frontend"

UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
IMAGES_DIR.mkdir(parents=True, exist_ok=True)

app = FastAPI(title="Reletix LLM Benchmark", version="0.1.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


@app.on_event("startup")
def _startup_init_db() -> None:
    """Run init_db (incl. orphan-sweep) AFTER the port has been bound.

    Earlier behaviour was to call db.init_db() at module import. That ran
    on every uvicorn that imported the module — including a second uvicorn
    that briefly started, ran the orphan-sweep, then failed to bind the
    port. The result: a stale uvicorn's runner threads kept hammering the
    provider while the DB said the runs were cancelled.

    Running init_db here means a uvicorn that fails to bind never gets
    to corrupt DB state.
    """
    db.init_db()


# ---------- schemas ----------
class PromptIn(BaseModel):
    name: str
    system_prompt: str
    description: Optional[str] = None


class RunIn(BaseModel):
    prompt_id: int
    dataset_id: int
    provider: str
    model_id: str
    api_key: Optional[str] = None
    base_url: Optional[str] = None
    user_prompt: Optional[str] = None
    pricing_override: Optional[dict] = None
    weights: Optional[dict] = None
    max_rows: Optional[int] = None
    random_sample: bool = True


# ---------- meta ----------
@app.get("/api/meta")
def meta():
    return {
        "providers": PROVIDERS,
        "pricing": PRICING,
        "env_keys_present": {
            "OPENAI_API_KEY": bool(os.getenv("OPENAI_API_KEY")),
            "ANTHROPIC_API_KEY": bool(os.getenv("ANTHROPIC_API_KEY")),
            "GEMINI_API_KEY": bool(os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY")),
        },
    }


@app.get("/api/env_key")
def env_key(provider: str):
    """Return API key from env if present (used by frontend so user doesn't have to paste)."""
    p = provider.lower()
    key = None
    if p == "openai":
        key = os.getenv("OPENAI_API_KEY")
    elif p == "anthropic":
        key = os.getenv("ANTHROPIC_API_KEY")
    elif p in ("gemini", "google"):
        key = os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY")
    return {"present": bool(key)}


@app.get("/api/models")
def models(provider: str, base_url: Optional[str] = None):
    """List vision-capable models for the given provider.

    For cloud providers (openai/anthropic/gemini) returns curated registry.
    For local providers, hits the local endpoint.
    """
    p = provider.lower()
    try:
        if p == "ollama":
            import httpx
            base = (base_url or "http://localhost:11434").rstrip("/")
            r = httpx.get(f"{base}/api/tags", timeout=5)
            local = [{"id": m["name"], "label": m["name"], "input": 0, "output": 0,
                      "group": "current"} for m in r.json().get("models", [])]
            return {"models": [m["id"] for m in local], "details": local}
        if p in ("lmstudio", "lm_studio", "lm-studio"):
            import httpx
            base = (base_url or "http://localhost:1234/v1").rstrip("/")
            r = httpx.get(f"{base}/models", timeout=5)
            local = [{"id": m["id"], "label": m["id"], "input": 0, "output": 0,
                      "group": "current"} for m in r.json().get("data", [])]
            return {"models": [m["id"] for m in local], "details": local}
    except Exception as e:
        return {"models": [], "details": [], "error": str(e)}

    # Cloud providers: curated vision-capable registry
    if p in ("gemini", "google"):
        p = "gemini"
    details = models_for(p)
    return {"models": [m["id"] for m in details], "details": details}


# ---------- prompts ----------
@app.get("/api/prompts")
def get_prompts():
    return db.list_prompts()


@app.post("/api/prompts")
def post_prompt(p: PromptIn):
    return db.create_prompt(p.name, p.system_prompt, p.description)


@app.delete("/api/prompts/{prompt_id}")
def del_prompt(prompt_id: int):
    db.delete_prompt(prompt_id)
    return {"ok": True}


# ---------- datasets ----------
@app.post("/api/datasets")
async def upload_dataset(file: UploadFile = File(...),
                         name: Optional[str] = Form(None),
                         image_url_template: Optional[str] = Form(None)):
    suffix = Path(file.filename).suffix.lower()
    if suffix not in (".xlsx", ".xlsm", ".csv"):
        raise HTTPException(400, "Only .xlsx, .xlsm, .csv supported")
    target = UPLOAD_DIR / file.filename
    with target.open("wb") as f:
        shutil.copyfileobj(file.file, f)
    parsed = parse_dataset(target, images_dir=IMAGES_DIR,
                                  image_url_template=image_url_template)
    ds = db.create_dataset(
        name=name or file.filename,
        file_path=str(target),
        n_rows=parsed["n"],
        columns_detected=parsed["columns_detected"],
        image_url_template=image_url_template,
    )
    ds["preview"] = parsed["rows"][:3]
    return ds


@app.get("/api/datasets")
def get_datasets():
    return db.list_datasets()


@app.get("/api/datasets/{dataset_id}/preview")
def preview_dataset(dataset_id: int, n: int = 5):
    ds = db.get_dataset(dataset_id)
    if not ds:
        raise HTTPException(404, "Not found")
    parsed = parse_dataset(ds["file_path"], images_dir=IMAGES_DIR,
                           image_url_template=ds.get("image_url_template"),
                           dataset_id=ds["id"])
    return {"dataset": ds, "rows": parsed["rows"][:n], "n_total": parsed["n"]}


@app.get("/api/datasets/{dataset_id}/rows")
def get_dataset_rows(dataset_id: int, offset: int = 0, limit: int = 20, q: Optional[str] = None):
    ds = db.get_dataset(dataset_id)
    if not ds:
        raise HTTPException(404, "Not found")
    parsed = parse_dataset(ds["file_path"], images_dir=IMAGES_DIR,
                           image_url_template=ds.get("image_url_template"),
                           dataset_id=ds["id"])
    rows = parsed["rows"]
    if q:
        ql = q.lower().strip()
        rows = [r for r in rows if r.get("food") and ql in r["food"].lower()]
    total = len(rows)
    page = rows[offset:offset + limit]
    return {"dataset": ds, "rows": page, "offset": offset, "limit": limit, "total": total,
            "n_total": parsed["n"]}


@app.delete("/api/datasets/{dataset_id}")
def delete_dataset(dataset_id: int):
    return db.delete_dataset(dataset_id)


# ---------- Corrections (truth overlay) ----------
class CorrectionIn(BaseModel):
    dataset_id: int
    image_id: str
    truth: dict     # canonical {food, description, nutrition, ingredients, health_score}
    source_run_id: Optional[int] = None
    source_row_idx: Optional[int] = None
    note: Optional[str] = None


@app.get("/api/corrections")
def list_corrections(dataset_id: int):
    items = db.list_corrections(dataset_id)
    # decode truth_json for client convenience
    for c in items:
        try:
            c["truth"] = _json.loads(c.get("truth_json") or "{}")
        except Exception:
            c["truth"] = {}
    return items


@app.post("/api/corrections")
def post_correction(c: CorrectionIn):
    return db.upsert_correction(
        dataset_id=c.dataset_id,
        image_id=c.image_id,
        truth_json=_json.dumps(c.truth),
        source_run_id=c.source_run_id,
        source_row_idx=c.source_row_idx,
        note=c.note,
    )


@app.delete("/api/corrections/{correction_id}")
def del_correction(correction_id: int):
    return db.delete_correction(correction_id)


class DatasetPatchIn(BaseModel):
    image_url_template: Optional[str] = None
    name: Optional[str] = None


@app.patch("/api/datasets/{dataset_id}")
def patch_dataset(dataset_id: int, p: DatasetPatchIn):
    fields = {k: v for k, v in p.dict(exclude_unset=True).items() if v is not None or k == "image_url_template"}
    return db.update_dataset(dataset_id, **fields)


# ---------- runs ----------
@app.post("/api/runs")
def post_run(r: RunIn):
    api_key = r.api_key
    if not api_key:
        if r.provider.lower() == "openai":
            api_key = os.getenv("OPENAI_API_KEY")
        elif r.provider.lower() == "anthropic":
            api_key = os.getenv("ANTHROPIC_API_KEY")
        elif r.provider.lower() in ("gemini", "google"):
            api_key = os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY")
    run_id = runner.start_run(
        prompt_id=r.prompt_id, dataset_id=r.dataset_id,
        provider_name=r.provider, model_id=r.model_id,
        api_key=api_key, base_url=r.base_url,
        user_prompt=r.user_prompt, pricing_override=r.pricing_override,
        weights=r.weights, max_rows=r.max_rows, random_sample=r.random_sample,
    )
    return {"run_id": run_id}


@app.get("/api/runs")
def get_runs(prompt_id: Optional[int] = None,
             model_id: Optional[str] = None,
             provider: Optional[str] = None,
             dataset_id: Optional[int] = None,
             status: Optional[str] = None):
    return db.list_runs(prompt_id=prompt_id, model_id=model_id,
                        provider=provider, dataset_id=dataset_id, status=status)


@app.get("/api/health")
def health():
    return {"status": "ok"}


@app.get("/api/runs/{run_id}")
def get_run(run_id: int):
    r = db.get_run(run_id)
    if not r:
        raise HTTPException(404, "Not found")
    return r


@app.post("/api/runs/{run_id}/pause")
def pause_run(run_id: int):
    return {"ok": runner.pause_run(run_id)}


@app.post("/api/runs/{run_id}/resume")
def resume_run(run_id: int):
    return {"ok": runner.resume_run(run_id)}


@app.post("/api/runs/{run_id}/cancel")
def cancel_run(run_id: int):
    return {"ok": runner.cancel_run(run_id)}


@app.delete("/api/runs/{run_id}")
def delete_run(run_id: int):
    """Delete a single run (and its row results).  If the run is still
    running/paused, cancel it first so the worker thread exits cleanly."""
    full = db.get_run(run_id)
    if not full:
        raise HTTPException(404, "Run not found")
    if full["status"] in ("running", "paused", "pending"):
        runner.cancel_run(run_id)
    return db.delete_run(run_id)


@app.get("/api/leaderboard/{prompt_id}")
def get_leaderboard(prompt_id: int):
    return db.leaderboard(prompt_id)


# ---------- run analysis (LLM-friendly export) ----------
def _build_run_analysis(run_id: int, max_rows_detail: int = 30) -> dict:
    """Build a compact analysis payload for a run, suitable for pasting into a chat.

    Includes: metadata, full system prompt, aggregate metrics, weakness patterns,
    and the worst-/best-scoring rows with full detail.
    """
    r = db.get_run(run_id)
    if not r:
        raise HTTPException(404, "Run not found")
    prompt = db.get_prompt(r["prompt_id"]) or {}
    dataset = db.get_dataset(r["dataset_id"]) or {}
    rows = r.get("rows", []) or []

    parsed_rows = []
    nutrient_acc: dict[str, list] = {}
    f1_scores: list[float] = []
    weight_scores: list[float] = []
    health_scores: list[float] = []
    name_scores: list[float] = []
    overall_scores: list[float] = []
    missed_truth_ings: Counter = Counter()
    extra_pred_ings: Counter = Counter()
    health_confusion: Counter = Counter()  # (truth, pred) pairs

    for rr in rows:
        sc = _safe_json(rr.get("scores"))
        op = _safe_json(rr.get("output_parsed"))
        truth = _safe_json(rr.get("truth"))
        if rr.get("error"):
            parsed_rows.append({
                "row_idx": rr["row_idx"],
                "error": rr["error"][:200],
                "latency_ms": rr.get("latency_ms"),
            })
            continue

        # Per-nutrient accuracy aggregation
        for k, d in (sc.get("nutrition_detail") or {}).items():
            nutrient_acc.setdefault(k, []).append(d.get("score", 0))
        if "ingredient_f1" in sc:
            f1_scores.append(sc["ingredient_f1"])
        if "weight_acc" in sc:
            weight_scores.append(sc["weight_acc"])
        if isinstance(sc.get("health"), dict) and sc["health"].get("score") is not None:
            health_scores.append(sc["health"]["score"])
            tg = sc["health"].get("truth"); pg = sc["health"].get("pred")
            if tg or pg:
                health_confusion[f"{tg or '—'}→{pg or '—'}"] += 1
        if "name_sim" in sc:
            name_scores.append(sc["name_sim"])
        if "overall" in sc:
            overall_scores.append(sc["overall"])

        ing = sc.get("ingredients") or {}
        for it in ing.get("unmatched_truth") or []:
            if it.get("name"):
                missed_truth_ings[it["name"].strip().lower()] += 1
        for it in ing.get("unmatched_pred") or []:
            if it.get("name"):
                extra_pred_ings[it["name"].strip().lower()] += 1

        # Condensed per-row summary
        nut_truth = (truth.get("nutrition") or {})
        nut_pred = (op.get("nutrition") or {})
        parsed_rows.append({
            "row_idx": rr["row_idx"],
            "image_ref": rr.get("image_ref"),
            "latency_ms": rr.get("latency_ms"),
            "tokens_in": rr.get("input_tokens"),
            "tokens_out": rr.get("output_tokens"),
            "cost_usd": rr.get("cost_usd"),
            "truth": {
                "food": truth.get("food"),
                "nutrition": nut_truth,
                "health_score": truth.get("health_score"),
                "ingredients": [
                    {"name": i.get("name"), "qty": i.get("quantity"), "unit": i.get("unit")}
                    for i in (truth.get("ingredients") or [])
                ],
            },
            "pred": {
                "food": op.get("food"),
                "nutrition": nut_pred,
                "health_score": op.get("health_score"),
                "ingredients": [
                    {"name": i.get("name"), "qty": i.get("quantity"), "unit": i.get("unit")}
                    for i in (op.get("ingredients") or [])
                ],
            },
            "scores": {
                "overall": sc.get("overall"),
                "macros_avg": sc.get("macros_avg"),
                "ingredient_f1": sc.get("ingredient_f1"),
                "weight_acc": sc.get("weight_acc"),
                "health": (sc.get("health") or {}).get("score"),
                "name_sim": sc.get("name_sim"),
                "per_nutrient": {k: round(d.get("score", 0), 3)
                                 for k, d in (sc.get("nutrition_detail") or {}).items()},
                "ingredient_match": {
                    "matched": ing.get("matched"),
                    "n_truth": ing.get("n_truth"),
                    "n_pred": ing.get("n_pred"),
                    "precision": ing.get("precision"),
                    "recall": ing.get("recall"),
                },
            },
        })

    parsed_rows.sort(key=lambda r: (r.get("scores") or {}).get("overall", 0))
    worst = parsed_rows[: min(max_rows_detail, len(parsed_rows))]
    best = list(reversed(parsed_rows[-min(5, len(parsed_rows)):]))

    def _avg(xs):
        return round(sum(xs) / len(xs), 4) if xs else None

    return {
        "run": {
            "id": r["id"],
            "status": r["status"],
            "provider": r["provider"],
            "model_id": r["model_id"],
            "n_rows": r["n_rows"],
            "n_done": r["n_done"],
            "started_at": r["started_at"],
            "finished_at": r.get("finished_at"),
            "accuracy": r["accuracy"],
            "composite_score": r["composite_score"],
            "avg_latency_ms": r["avg_latency_ms"],
            "total_input_tokens": r["total_input_tokens"],
            "total_output_tokens": r["total_output_tokens"],
            "total_cost_usd": r["total_cost_usd"],
            "config": _safe_json(r.get("config")),
            "error": r.get("error"),
        },
        "prompt": {
            "id": prompt.get("id"),
            "name": prompt.get("name"),
            "description": prompt.get("description"),
            "system_prompt": prompt.get("system_prompt"),
        },
        "dataset": {
            "id": dataset.get("id"),
            "name": dataset.get("name"),
            "n_rows_total": dataset.get("n_rows"),
        },
        "aggregates": {
            "name_sim_avg": _avg(name_scores),
            "macros_per_nutrient_avg": {k: _avg(v) for k, v in nutrient_acc.items()},
            "ingredient_f1_avg": _avg(f1_scores),
            "weight_acc_avg": _avg(weight_scores),
            "health_acc_avg": _avg(health_scores),
            "health_confusion": dict(health_confusion.most_common(20)),
            "top_missed_truth_ingredients": missed_truth_ings.most_common(20),
            "top_extra_pred_ingredients": extra_pred_ings.most_common(20),
        },
        "worst_rows": worst,
        "best_rows": best,
    }


def _safe_json(s):
    if s is None:
        return {}
    if isinstance(s, dict) or isinstance(s, list):
        return s
    try:
        return _json.loads(s)
    except Exception:
        return {}


def _render_analysis_markdown(payload: dict) -> str:
    r = payload["run"]; p = payload["prompt"]; ds = payload["dataset"]
    a = payload["aggregates"]

    def _pct(v):
        return "—" if v is None else f"{v*100:.1f}%"
    def _money(v):
        return "—" if v is None else f"${v:.4f}"

    lines = []
    lines.append(f"# Run #{r['id']} — {r['provider']} · {r['model_id']}")
    lines.append("")
    lines.append(f"- **Status**: {r['status']}  ·  **rows**: {r['n_done']}/{r['n_rows']}")
    lines.append(f"- **Prompt**: {p.get('name')}  (id {p.get('id')})")
    lines.append(f"- **Dataset**: {ds.get('name')}  (id {ds.get('id')}, {ds.get('n_rows_total')} total rows)")
    lines.append(f"- **Accuracy**: {_pct(r['accuracy'])}  ·  **Composite**: {r['composite_score']:.1f}")
    lines.append(f"- **Avg latency**: {(r['avg_latency_ms'] or 0)/1000:.2f}s  ·  "
                 f"**Tokens**: {r['total_input_tokens']:,} in / {r['total_output_tokens']:,} out  ·  "
                 f"**Cost**: {_money(r['total_cost_usd'])}")
    if r.get("error"):
        lines.append(f"- **Error**: {r['error'][:300]}")
    lines.append("")

    # System prompt
    lines.append("## System prompt")
    lines.append("```")
    lines.append((p.get("system_prompt") or "").rstrip())
    lines.append("```")
    lines.append("")

    # Aggregates
    lines.append("## Aggregate sub-scores (averaged across all scored rows)")
    lines.append("")
    lines.append(f"- **Name sim**: {_pct(a['name_sim_avg'])}")
    lines.append(f"- **Ingredient F1**: {_pct(a['ingredient_f1_avg'])}")
    lines.append(f"- **Weight accuracy**: {_pct(a['weight_acc_avg'])}")
    lines.append(f"- **Health grade accuracy**: {_pct(a['health_acc_avg'])}")
    lines.append("")
    lines.append("### Per-nutrient accuracy")
    lines.append("| Nutrient | Avg |")
    lines.append("|----------|----:|")
    for k, v in (a["macros_per_nutrient_avg"] or {}).items():
        lines.append(f"| {k} | {_pct(v)} |")
    lines.append("")

    # Health confusion
    if a.get("health_confusion"):
        lines.append("### Health-grade confusion (truth → pred, count)")
        for k, v in a["health_confusion"].items():
            lines.append(f"- `{k}` — {v}")
        lines.append("")

    # Ingredient mismatches
    if a.get("top_missed_truth_ingredients"):
        lines.append("### Most-missed truth ingredients (model failed to detect)")
        for name, count in a["top_missed_truth_ingredients"]:
            lines.append(f"- `{name}` × {count}")
        lines.append("")
    if a.get("top_extra_pred_ingredients"):
        lines.append("### Most over-predicted ingredients (in model output, not in truth)")
        for name, count in a["top_extra_pred_ingredients"]:
            lines.append(f"- `{name}` × {count}")
        lines.append("")

    # Per-row detail (worst first)
    def _row_block(rows, title):
        if not rows:
            return
        lines.append(f"## {title}")
        for row in rows:
            lines.append("")
            if row.get("error"):
                lines.append(f"### Row {row['row_idx']} — ERROR")
                lines.append(f"`{row['error']}`")
                continue
            sc = row["scores"]; t = row["truth"]; pp = row["pred"]
            lines.append(f"### Row {row['row_idx']} — overall {_pct(sc.get('overall'))}")
            lines.append(f"- **Truth food**: {t.get('food')}")
            lines.append(f"- **Pred food**:  {pp.get('food')}")
            lines.append(f"- **Sub-scores**: macros {_pct(sc.get('macros_avg'))} · "
                         f"ing F1 {_pct(sc.get('ingredient_f1'))} · "
                         f"weight {_pct(sc.get('weight_acc'))} · "
                         f"health {_pct(sc.get('health'))} · "
                         f"name {_pct(sc.get('name_sim'))}")
            lines.append(f"- **Health**: truth `{t.get('health_score') or '—'}` vs pred `{pp.get('health_score') or '—'}`")
            # macros table
            lines.append("")
            lines.append("| Macro | Truth | Pred | Acc |")
            lines.append("|---|---:|---:|---:|")
            for k in ("calories","protein_g","carbs_g","fat_g","fiber_g","sugar_g","sodium_mg"):
                tv = (t.get("nutrition") or {}).get(k); pv = (pp.get("nutrition") or {}).get(k)
                acc = (sc.get("per_nutrient") or {}).get(k)
                lines.append(f"| {k} | {tv if tv is not None else '—'} | "
                             f"{pv if pv is not None else '—'} | {_pct(acc)} |")
            # ingredient match summary
            im = sc.get("ingredient_match") or {}
            lines.append("")
            lines.append(f"- **Ingredients matched**: {im.get('matched')}/{im.get('n_truth')} truth · "
                         f"model returned {im.get('n_pred')} · "
                         f"P={_pct(im.get('precision'))} R={_pct(im.get('recall'))}")
            # truth vs pred ingredient names
            t_ings = ", ".join(f"{i['name']}({i['qty']}{i.get('unit') or 'g'})" for i in (t.get('ingredients') or []) if i.get('name'))
            p_ings = ", ".join(f"{i['name']}({i['qty']}{i.get('unit') or 'g'})" for i in (pp.get('ingredients') or []) if i.get('name'))
            lines.append(f"- **Truth ings**: {t_ings or '—'}")
            lines.append(f"- **Pred ings**:  {p_ings or '—'}")
            lines.append(f"- **Latency**: {(row.get('latency_ms') or 0)/1000:.2f}s · "
                         f"tokens {row.get('tokens_in')}/{row.get('tokens_out')} · "
                         f"cost {_money(row.get('cost_usd'))}")
        lines.append("")

    _row_block(payload["worst_rows"], f"Worst {len(payload['worst_rows'])} rows (lowest overall score first)")
    _row_block(payload["best_rows"], f"Best {len(payload['best_rows'])} rows (highest overall score first)")

    return "\n".join(lines)


@app.get("/api/runs/{run_id}/analysis")
def get_run_analysis(run_id: int, format: str = "json", max_rows: int = 30):
    """LLM-friendly export of a single run.

    GET /api/runs/16/analysis            -> JSON
    GET /api/runs/16/analysis?format=md  -> Markdown ready to paste into a chat
    """
    payload = _build_run_analysis(run_id, max_rows_detail=max_rows)
    fmt = format.lower().strip()
    if fmt in ("md", "markdown", "text"):
        return PlainTextResponse(_render_analysis_markdown(payload),
                                 media_type="text/markdown; charset=utf-8")
    return payload


@app.get("/api/runs/{run_id}/analysis.md")
def get_run_analysis_md(run_id: int, max_rows: int = 30):
    payload = _build_run_analysis(run_id, max_rows_detail=max_rows)
    return PlainTextResponse(_render_analysis_markdown(payload),
                             media_type="text/markdown; charset=utf-8")


# ---------- multi-run comparison ----------
def _parse_id_list(s: str) -> list[int]:
    out = []
    for tok in (s or "").split(","):
        tok = tok.strip()
        if tok.isdigit():
            out.append(int(tok))
    return sorted(set(out))


def _build_compare(ids: list[int]) -> dict:
    runs = db.list_runs_by_ids(ids)
    if len(runs) < 2:
        raise HTTPException(400, "Need at least 2 valid run ids")

    # Per-run summary + map of row_idx -> row record
    per_run_summary = []
    per_run_rows: dict[int, dict[int, dict]] = {}
    for r in runs:
        full = db.get_run(r["id"]) or {}
        rows = full.get("rows", []) or []
        per_run_rows[r["id"]] = {rr["row_idx"]: rr for rr in rows}
        per_run_summary.append({
            "id": r["id"],
            "status": r["status"],
            "provider": r["provider"],
            "model_id": r["model_id"],
            "prompt_id": r["prompt_id"],
            "prompt_name": r["prompt_name"],
            "prompt_text": r["prompt_text"],
            "dataset_id": r["dataset_id"],
            "dataset_name": r["dataset_name"],
            "n_done": r["n_done"],
            "n_rows": r["n_rows"],
            "accuracy": r["accuracy"] or 0,
            "composite_score": r["composite_score"] or 0,
            "avg_latency_ms": r["avg_latency_ms"] or 0,
            "total_input_tokens": r["total_input_tokens"] or 0,
            "total_output_tokens": r["total_output_tokens"] or 0,
            "total_cost_usd": r["total_cost_usd"] or 0,
            "started_at": r["started_at"],
            "finished_at": r.get("finished_at"),
        })

    # Aggregate sub-scores per run (averaged across that run's rows)
    def _avg(xs):
        return round(sum(xs) / len(xs), 4) if xs else 0.0

    def _aggs_for(run_id: int) -> dict:
        rows = per_run_rows[run_id].values()
        macro_avg, name_sim, ing_f1, weight, health = [], [], [], [], []
        per_nut: dict[str, list[float]] = {}
        for rr in rows:
            sc = _safe_json(rr.get("scores"))
            if rr.get("error"):
                continue
            if "macros_avg" in sc: macro_avg.append(sc["macros_avg"])
            if "name_sim" in sc: name_sim.append(sc["name_sim"])
            if "ingredient_f1" in sc: ing_f1.append(sc["ingredient_f1"])
            if "weight_acc" in sc: weight.append(sc["weight_acc"])
            if isinstance(sc.get("health"), dict) and sc["health"].get("score") is not None:
                health.append(sc["health"]["score"])
            for k, d in (sc.get("nutrition_detail") or {}).items():
                per_nut.setdefault(k, []).append(d.get("score", 0))
        return {
            "macros_avg":     _avg(macro_avg),
            "name_sim":       _avg(name_sim),
            "ingredient_f1":  _avg(ing_f1),
            "weight_acc":     _avg(weight),
            "health_acc":     _avg(health),
            "per_nutrient":   {k: _avg(v) for k, v in per_nut.items()},
        }

    for s in per_run_summary:
        s["aggregates"] = _aggs_for(s["id"])

    # Shared rows pivot — only rows that appear in EVERY selected run
    common_idxs = set.intersection(*[set(per_run_rows[r["id"]].keys()) for r in runs])
    shared = []
    for idx in sorted(common_idxs):
        row_record = next(per_run_rows[r["id"]][idx] for r in runs)
        truth = _safe_json(row_record.get("truth"))
        image_ref = row_record.get("image_ref")
        per_run_for_row: dict[str, dict] = {}
        scores_for_idx = []
        for r in runs:
            rr = per_run_rows[r["id"]][idx]
            sc = _safe_json(rr.get("scores"))
            op = _safe_json(rr.get("output_parsed"))
            ov = sc.get("overall", 0) if not rr.get("error") else 0
            scores_for_idx.append((r["id"], ov))
            per_run_for_row[str(r["id"])] = {
                "pred_food": op.get("food"),
                "pred_health": op.get("health_score"),
                "pred_nutrition": op.get("nutrition"),
                "scores": {
                    "overall": ov,
                    "macros_avg": sc.get("macros_avg"),
                    "ingredient_f1": sc.get("ingredient_f1"),
                    "weight_acc": sc.get("weight_acc"),
                    "health": (sc.get("health") or {}).get("score"),
                    "name_sim": sc.get("name_sim"),
                },
                "latency_ms": rr.get("latency_ms"),
                "input_tokens": rr.get("input_tokens"),
                "output_tokens": rr.get("output_tokens"),
                "cost_usd": rr.get("cost_usd"),
                "error": rr.get("error"),
            }
        scores_only = [s for _, s in scores_for_idx]
        spread = (max(scores_only) - min(scores_only)) if scores_only else 0
        shared.append({
            "row_idx": idx,
            "image_ref": image_ref,
            "truth_food": truth.get("food"),
            "truth_health": truth.get("health_score"),
            "truth_nutrition": truth.get("nutrition"),
            "by_run": per_run_for_row,
            "score_spread": round(spread, 4),
        })

    shared.sort(key=lambda r: -r["score_spread"])

    # Diff summary
    biggest_swings = shared[:10]

    # Rows only in some runs (informational)
    all_idxs: set = set()
    for r in runs:
        all_idxs.update(per_run_rows[r["id"]].keys())
    rows_only_in_some = []
    for idx in sorted(all_idxs - common_idxs):
        in_runs = [r["id"] for r in runs if idx in per_run_rows[r["id"]]]
        rows_only_in_some.append({"row_idx": idx, "present_in": in_runs})

    return {
        "runs": per_run_summary,
        "shared_rows": shared,
        "rows_partial": rows_only_in_some,
        "biggest_swings": biggest_swings,
        "n_shared": len(shared),
        "n_partial": len(rows_only_in_some),
    }


def _render_compare_markdown(payload: dict) -> str:
    runs = payload["runs"]
    lines = []
    lines.append(f"# Comparison: " + " · ".join(f"#{r['id']}" for r in runs))
    lines.append("")
    # Run summary table
    lines.append("## Run summary")
    lines.append("")
    headers = ["Field"] + [f"#{r['id']}" for r in runs]
    lines.append("| " + " | ".join(headers) + " |")
    lines.append("|" + "|".join("---" for _ in headers) + "|")
    def _row(label, vals):
        lines.append("| " + label + " | " + " | ".join(str(v) for v in vals) + " |")

    _row("Provider",   [r["provider"] for r in runs])
    _row("Model",      [r["model_id"] for r in runs])
    _row("Prompt",     [f'{r["prompt_name"]} (id {r["prompt_id"]})' for r in runs])
    _row("Dataset",    [f'{r["dataset_name"]} (id {r["dataset_id"]})' for r in runs])
    _row("Rows",       [f'{r["n_done"]}/{r["n_rows"]}' for r in runs])
    _row("Accuracy",   [f'{(r["accuracy"] or 0)*100:.1f}%' for r in runs])
    _row("Composite",  [f'{(r["composite_score"] or 0):.1f}' for r in runs])
    _row("Macros avg", [f'{(r["aggregates"]["macros_avg"])*100:.1f}%' for r in runs])
    _row("Ingredient F1", [f'{(r["aggregates"]["ingredient_f1"])*100:.1f}%' for r in runs])
    _row("Weight acc", [f'{(r["aggregates"]["weight_acc"])*100:.1f}%' for r in runs])
    _row("Health acc", [f'{(r["aggregates"]["health_acc"])*100:.1f}%' for r in runs])
    _row("Avg latency",[f'{(r["avg_latency_ms"] or 0)/1000:.2f}s' for r in runs])
    _row("Total cost", [f'${(r["total_cost_usd"] or 0):.4f}' for r in runs])
    _row("Tokens in",  [f'{r["total_input_tokens"]:,}' for r in runs])
    _row("Tokens out", [f'{r["total_output_tokens"]:,}' for r in runs])
    lines.append("")

    # Per-nutrient comparison
    lines.append("## Per-nutrient accuracy")
    lines.append("")
    nuts = ["calories","protein_g","carbs_g","fat_g","fiber_g","sugar_g","sodium_mg"]
    headers = ["Nutrient"] + [f"#{r['id']} ({r['model_id']})" for r in runs]
    lines.append("| " + " | ".join(headers) + " |")
    lines.append("|" + "|".join("---:" if i>0 else "---" for i in range(len(headers))) + "|")
    for n in nuts:
        vals = [n]
        for r in runs:
            v = r["aggregates"]["per_nutrient"].get(n)
            vals.append(f'{(v or 0)*100:.1f}%' if v is not None else "—")
        lines.append("| " + " | ".join(vals) + " |")
    lines.append("")

    # Prompt diffs
    lines.append("## Prompts (verbatim)")
    for r in runs:
        lines.append("")
        lines.append(f"### Run #{r['id']} — {r['prompt_name']}")
        lines.append("```")
        lines.append((r["prompt_text"] or "").rstrip())
        lines.append("```")
    lines.append("")

    # Biggest swings
    swings = payload["biggest_swings"]
    if swings:
        lines.append(f"## Biggest score swings (top {len(swings)} of {payload['n_shared']} shared rows)")
        lines.append("")
        head = ["Row", "Truth food", "Spread"] + [f"#{r['id']}" for r in runs]
        lines.append("| " + " | ".join(head) + " |")
        lines.append("|" + "|".join(["---"] + ["---"]*(len(head)-1)) + "|")
        for s in swings:
            cells = [str(s["row_idx"]),
                     (s["truth_food"] or "—")[:40],
                     f"{s['score_spread']*100:.0f}%"]
            for r in runs:
                bd = s["by_run"].get(str(r["id"])) or {}
                ov = (bd.get("scores") or {}).get("overall") or 0
                pred = bd.get("pred_food") or ("ERR" if bd.get("error") else "—")
                cells.append(f"{ov*100:.0f}% · {pred[:30]}")
            lines.append("| " + " | ".join(cells) + " |")
        lines.append("")

    lines.append(f"_Shared rows: {payload['n_shared']} · partial overlap: {payload['n_partial']}_")
    return "\n".join(lines)


@app.get("/api/compare")
def get_compare(ids: str):
    return _build_compare(_parse_id_list(ids))


@app.get("/api/compare.md")
def get_compare_md(ids: str):
    payload = _build_compare(_parse_id_list(ids))
    return PlainTextResponse(_render_compare_markdown(payload),
                             media_type="text/markdown; charset=utf-8")


# ---------- automation: simpler /api/v1/runs ----------
class V1RunIn(BaseModel):
    prompt_text: Optional[str] = None
    prompt_id: Optional[int] = None
    prompt_name: Optional[str] = None
    dataset_id: Optional[int] = None
    dataset_name: Optional[str] = None
    provider: str
    model_id: str
    api_key: Optional[str] = None
    base_url: Optional[str] = None
    user_prompt: Optional[str] = None
    pricing_override: Optional[dict] = None
    weights: Optional[dict] = None
    max_rows: Optional[int] = None
    random_sample: bool = True


@app.post("/api/v1/runs")
def post_v1_run(r: V1RunIn):
    """Automation-friendly run launcher.

    Accepts prompt as TEXT (creates or finds the prompt automatically) and
    dataset by id OR name. Returns urls to poll status and pull analysis.
    """
    # Resolve prompt
    prompt = None
    if r.prompt_id:
        prompt = db.get_prompt(r.prompt_id)
    elif r.prompt_text:
        existing = db.find_prompt_by_text(r.prompt_text)
        if existing:
            prompt = existing
        else:
            prompt = db.create_prompt(
                name=r.prompt_name or f"auto-{int(__import__('time').time())}",
                system_prompt=r.prompt_text,
                description="Created via /api/v1/runs",
            )
    if not prompt:
        raise HTTPException(400, "Provide prompt_id OR prompt_text")

    # Resolve dataset
    dataset = None
    if r.dataset_id:
        dataset = db.get_dataset(r.dataset_id)
    elif r.dataset_name:
        dataset = db.find_dataset_by_name(r.dataset_name)
    if not dataset:
        raise HTTPException(400, "Provide dataset_id or dataset_name (and ensure it exists)")

    # API key fallback to env
    api_key = r.api_key
    if not api_key:
        if r.provider.lower() == "openai":
            api_key = os.getenv("OPENAI_API_KEY")
        elif r.provider.lower() == "anthropic":
            api_key = os.getenv("ANTHROPIC_API_KEY")
        elif r.provider.lower() in ("gemini", "google"):
            api_key = os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY")

    run_id = runner.start_run(
        prompt_id=prompt["id"], dataset_id=dataset["id"],
        provider_name=r.provider, model_id=r.model_id,
        api_key=api_key, base_url=r.base_url,
        user_prompt=r.user_prompt, pricing_override=r.pricing_override,
        weights=r.weights, max_rows=r.max_rows,
        random_sample=r.random_sample,
    )
    return {
        "run_id": run_id,
        "prompt_id": prompt["id"],
        "dataset_id": dataset["id"],
        "status_url": f"/api/runs/{run_id}",
        "analysis_url": f"/api/runs/{run_id}/analysis.md",
    }


# ---------- static images ----------
app.mount("/images", StaticFiles(directory=IMAGES_DIR), name="images")


# ---------- frontend ----------
@app.get("/")
def index():
    return FileResponse(FRONTEND_DIR / "index.html")


app.mount("/", StaticFiles(directory=FRONTEND_DIR), name="frontend")
