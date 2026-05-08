"""Diagnostic report for scoring fairness.

Surfaces the rows where a benchmark score is suspiciously low for a
visually-good answer, and exposes the *reasoning chain* (token sets,
synonyms, semantic cosine, which gate fired) that produced the number.
Use this to decide *what* to change about scoring, not just *that* it
needs changing.

Usage:
  .venv/bin/python -m scripts.diagnose_scoring --runs 12,17,21
  .venv/bin/python -m scripts.diagnose_scoring --runs 12 --top 30

Output: data/diag/<timestamp>-runs-<ids>.md  (markdown, no DB writes)
"""
from __future__ import annotations

import argparse
import json
import os
import statistics
import sys
import time
from pathlib import Path

# Make the script runnable both as `python -m scripts.diagnose_scoring`
# and `python scripts/diagnose_scoring.py`.
ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend import db, scoring  # noqa: E402


# ---------- helpers ----------

def _safe_json(s):
    if s is None:
        return {}
    if isinstance(s, (dict, list)):
        return s
    try:
        return json.loads(s)
    except Exception:
        return {}


def _judge_provenance(run: dict, rows: list[dict]) -> tuple[str, str | None]:
    """Mirror of backend.main._judge_provenance, kept local so the
    script doesn't import a FastAPI module just for one helper."""
    cfg = _safe_json(run.get("config"))
    seen_llm = seen_rules = False
    for rr in rows:
        if rr.get("error"):
            continue
        sc = _safe_json(rr.get("scores"))
        ing = sc.get("ingredients") if isinstance(sc, dict) else None
        if not isinstance(ing, dict):
            continue
        jb = ing.get("judged_by")
        if jb == "llm":
            seen_llm = True
        elif jb == "rules":
            seen_rules = True
    if seen_llm and seen_rules:
        mode = "mixed"
    elif seen_llm:
        mode = "llm"
    elif seen_rules:
        mode = "rules"
    else:
        mode = "none"
    if mode in ("rules", "none"):
        return mode, None
    if cfg.get("judge_with_run_model"):
        return mode, f"self ({run.get('model_id')})"
    return mode, os.environ.get("LLM_JUDGE_MODEL") or "gpt-5-mini"


def _semantic_cosine(a: str | None, b: str | None) -> float | None:
    """Return the raw cosine if the semantic backend is available, else None."""
    if not a or not b:
        return None
    try:
        from backend import semantic
        if not semantic.is_available():
            return None
        return float(semantic.semantic_similarity(a, b))
    except Exception:
        return None


def _name_trace(pred: str | None, truth: str | None) -> dict:
    """Recompute the reasoning chain that text_similarity() walked.
    Returns the inputs after synonym rewrite, the token sets, the F1,
    semantic cosine, and which (if any) gates would have fired in
    the semantic-floor branch."""
    if not pred or not truth:
        return {"pred": pred, "truth": truth, "name_sim": 0.0, "note": "empty input"}

    pred_n = scoring._apply_synonyms(str(pred).lower())
    truth_n = scoring._apply_synonyms(str(truth).lower())
    p_outer, p_inner = scoring._split_paren(pred_n)
    t_outer, t_inner = scoring._split_paren(truth_n)
    sets = {
        "p_outer": scoring._tokens(p_outer),
        "p_inner": scoring._tokens(p_inner),
        "t_outer": scoring._tokens(t_outer),
        "t_inner": scoring._tokens(t_inner),
    }
    best_pair = ("", "", 0.0)
    for pk in ("p_outer", "p_inner"):
        for tk in ("t_outer", "t_inner"):
            s = scoring._sim_pair(sets[pk], sets[tk])
            if s > best_pair[2]:
                best_pair = (pk, tk, s)

    # Gates the semantic-floor branch checks
    p_all = sets["p_outer"] | sets["p_inner"]
    t_all = sets["t_outer"] | sets["t_inner"]
    shared = p_all & t_all
    p_id = p_all & scoring._IDENTITY_MODS
    t_id = t_all & scoring._IDENTITY_MODS
    identity_conflict = bool(p_id and t_id and not (p_id & t_id))
    category_rejection = bool(shared & scoring._GENERIC_CATEGORY_TOKENS)
    false_friend = any(
        (ff & p_all) and (ff & t_all) and not (ff & p_all & t_all)
        for ff in scoring._SEMANTIC_FALSE_FRIENDS
    )

    # Authoritative score from the function under test
    actual = scoring.text_similarity(pred, truth)
    cosine = _semantic_cosine(pred, truth)

    return {
        "pred": pred,
        "truth": truth,
        "pred_after_synonyms": pred_n,
        "truth_after_synonyms": truth_n,
        "tokens_pred": sorted(p_all),
        "tokens_truth": sorted(t_all),
        "shared_tokens": sorted(shared),
        "best_pair_F1": round(best_pair[2], 3),
        "best_pair_keys": (best_pair[0], best_pair[1]),
        "semantic_cosine": (None if cosine is None else round(cosine, 3)),
        "name_sim": round(actual, 3),
        "gate_identity_conflict": identity_conflict,
        "gate_category_rejection": category_rejection,
        "gate_false_friend": false_friend,
    }


def _looks_unfair(pred: str, truth: str, name_sim: float, ing_f1: float) -> bool:
    """Heuristic: name_sim < 0.5 *but* the row probably should have scored higher."""
    if name_sim >= 0.5:
        return False
    if not pred or not truth:
        return False
    p, t = pred.lower(), truth.lower()
    # 1. one is fully contained in the other (after lowercase)
    if p in t or t in p:
        return True
    # 2. ingredient_f1 says the model understood the dish even if it named it differently
    if ing_f1 >= 0.5:
        return True
    # 3. shared non-stopword token (common dish-name pattern: "chicken biryani" vs "biryani")
    p_toks = scoring._tokens(p)
    t_toks = scoring._tokens(t)
    if p_toks & t_toks:
        return True
    return False


# ---------- per-run sections ----------

def _section_run_header(run: dict, rows: list[dict]) -> str:
    mode, jmodel = _judge_provenance(run, rows)
    cfg = _safe_json(run.get("config"))
    judge_inline = cfg.get("judge_with_run_model")
    n_rows = len(rows)
    n_err = sum(1 for r in rows if r.get("error"))
    return (
        f"## Run #{run['id']} — `{run.get('model_id')}` ({run.get('provider')})\n\n"
        f"- Dataset: `{run.get('dataset_name') or run.get('dataset_id')}`\n"
        f"- Status: `{run.get('status')}` · rows: {n_rows} (errored: {n_err})\n"
        f"- Accuracy: `{(run.get('accuracy') or 0):.3f}` · "
        f"Composite: `{(run.get('composite_score') or 0):.2f}` · "
        f"Avg latency: `{(run.get('avg_latency_ms') or 0)/1000:.2f}s`\n"
        f"- **Judge mode:** `{mode}`"
        f"{' (inline self-judge)' if judge_inline else ''}"
        f"{f' · **judge model:** `{jmodel}`' if jmodel else ''}\n\n"
    )


def _section_name_outliers(run: dict, rows: list[dict], top: int) -> str:
    candidates = []
    for rr in rows:
        if rr.get("error"):
            continue
        sc = _safe_json(rr.get("scores"))
        truth = _safe_json(rr.get("truth"))
        op = _safe_json(rr.get("output_parsed"))
        pred_food = (op.get("food") or "").strip()
        truth_food = (truth.get("food") or "").strip()
        ns = float(sc.get("name_sim") or 0)
        f1 = float(sc.get("ingredient_f1") or 0)
        if _looks_unfair(pred_food, truth_food, ns, f1):
            candidates.append((rr, sc, truth, op, pred_food, truth_food, ns, f1))

    candidates.sort(key=lambda x: x[6])  # lowest name_sim first
    candidates = candidates[:top]

    if not candidates:
        return "### name_sim outliers\n\n_None._\n\n"

    out = [f"### name_sim outliers ({len(candidates)} rows)\n\n"]
    out.append(
        "Rows where `name_sim < 0.5` *but* the prediction shares structure with truth "
        "(substring, shared non-stop token, or `ingredient_f1 >= 0.5`).\n\n"
    )
    for rr, sc, truth, op, pred_food, truth_food, ns, f1 in candidates:
        trace = _name_trace(pred_food, truth_food)
        gates = []
        if trace["gate_identity_conflict"]: gates.append("identity_conflict")
        if trace["gate_category_rejection"]: gates.append("category_rejection")
        if trace["gate_false_friend"]: gates.append("false_friend")
        gates_s = (", ".join(gates) or "none fired")

        ing = sc.get("ingredients") or {}
        jb = ing.get("judged_by") or "?"
        out.append(
            f"#### row {rr.get('row_idx')}  · overall `{(sc.get('overall') or 0):.3f}`\n\n"
            f"| field | value |\n|---|---|\n"
            f"| pred food | `{pred_food}` |\n"
            f"| truth food | `{truth_food}` |\n"
            f"| name_sim | **{ns:.3f}** |\n"
            f"| ingredient_f1 | {f1:.3f} (judged_by: `{jb}`) |\n"
            f"| pred tokens | `{trace['tokens_pred']}` |\n"
            f"| truth tokens | `{trace['tokens_truth']}` |\n"
            f"| shared tokens | `{trace['shared_tokens']}` |\n"
            f"| best F1 (token pipeline) | {trace['best_pair_F1']:.3f} (pair `{trace['best_pair_keys']}`) |\n"
            f"| semantic cosine | `{trace['semantic_cosine']}` |\n"
            f"| gates fired | `{gates_s}` |\n"
            f"| pred after synonyms | `{trace['pred_after_synonyms']}` |\n"
            f"| truth after synonyms | `{trace['truth_after_synonyms']}` |\n\n"
        )
    return "".join(out)


def _section_judge_reasons(rows: list[dict], top: int = 15) -> str:
    """When the LLM judge ran, what *reason* labels did it use? Surfacing
    the distribution often reveals systemic bias (e.g. way too many
    `different-item` calls)."""
    reasons: dict[str, int] = {}
    sample: list[tuple[str, str, str, float]] = []
    for rr in rows:
        if rr.get("error"):
            continue
        sc = _safe_json(rr.get("scores"))
        ing = sc.get("ingredients") or {}
        if ing.get("judged_by") != "llm":
            continue
        for m in (ing.get("matches") or []):
            label = (m.get("reason") or "").split("—", 1)[0].strip().lower() or "(blank)"
            reasons[label] = reasons.get(label, 0) + 1
            if len(sample) < top:
                sample.append((m.get("truth_name") or "?",
                               m.get("pred_name") or "?",
                               m.get("reason") or "?",
                               float(m.get("name_sim") or 0)))
    if not reasons:
        return ""
    out = ["### LLM judge reason distribution\n\n"]
    for k, v in sorted(reasons.items(), key=lambda kv: -kv[1]):
        out.append(f"- `{k}` × {v}\n")
    out.append("\n#### sample reasons\n\n| truth | pred | score | reason |\n|---|---|---|---|\n")
    for t, p, r, s in sample:
        out.append(f"| `{t}` | `{p}` | {s:.2f} | {r} |\n")
    out.append("\n")
    return "".join(out)


# ---------- multi-run sections ----------

def _section_disagreements(runs: list[dict], rows_per_run: dict[int, dict[int, dict]],
                            top: int) -> str:
    if len(runs) < 2:
        return ""
    common = set.intersection(*[set(rows_per_run[r["id"]].keys()) for r in runs])
    if not common:
        return "### Disagreement rows\n\n_No rows shared by all selected runs._\n\n"
    swings = []
    for idx in sorted(common):
        per_run_overall = []
        per_run_name = []
        truth = None
        for r in runs:
            rr = rows_per_run[r["id"]][idx]
            sc = _safe_json(rr.get("scores"))
            if rr.get("error"):
                per_run_overall.append((r["id"], None, None))
                continue
            ov = float(sc.get("overall") or 0)
            ns = float(sc.get("name_sim") or 0)
            per_run_overall.append((r["id"], ov, ns))
            per_run_name.append(ns)
            if truth is None:
                truth = _safe_json(rr.get("truth"))
        valid = [v for _, v, _ in per_run_overall if v is not None]
        if len(valid) < 2:
            continue
        spread = max(valid) - min(valid)
        stdev = statistics.stdev(valid) if len(valid) >= 2 else 0
        swings.append((idx, spread, stdev, per_run_overall, truth))
    swings.sort(key=lambda x: -x[1])
    swings = swings[:top]
    if not swings:
        return ""
    out = [f"### Disagreement rows (top {len(swings)} by overall-score spread)\n\n"]
    out.append("| row | truth food | " + " | ".join(f"#{r['id']} ov / name" for r in runs)
               + " | spread | stdev |\n")
    out.append("|---|---|" + "|".join("---" for _ in runs) + "|---|---|\n")
    for idx, spread, stdev, per_run_overall, truth in swings:
        cells = []
        for rid, ov, ns in per_run_overall:
            if ov is None:
                cells.append("—")
            else:
                cells.append(f"{ov:.2f} / {ns:.2f}")
        truth_food = (truth or {}).get("food", "") if truth else ""
        out.append(f"| {idx} | `{truth_food}` | " + " | ".join(cells)
                   + f" | {spread:.2f} | {stdev:.2f} |\n")
    out.append("\n")
    return "".join(out)


# ---------- main ----------

def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--runs", required=True,
                    help="Comma-separated run ids, e.g. '12,17,21'")
    ap.add_argument("--top", type=int, default=20,
                    help="Cap on outlier / disagreement rows shown per section (default 20)")
    ap.add_argument("--out", default=None,
                    help="Output path (default: data/diag/<timestamp>-runs-<ids>.md)")
    args = ap.parse_args()

    ids = sorted({int(x) for x in args.runs.split(",") if x.strip().isdigit()})
    if not ids:
        print("error: no valid run ids in --runs", file=sys.stderr)
        return 2

    runs_full: list[dict] = []
    rows_per_run: dict[int, dict[int, dict]] = {}
    for rid in ids:
        r = db.get_run(rid)
        if not r:
            print(f"warn: run #{rid} not found, skipping", file=sys.stderr)
            continue
        rows = r.get("rows") or []
        runs_full.append(r)
        rows_per_run[rid] = {rr["row_idx"]: rr for rr in rows}

    if not runs_full:
        print("error: no runs loaded", file=sys.stderr)
        return 2

    out: list[str] = []
    out.append(f"# Scoring diagnostic — runs {','.join(str(r['id']) for r in runs_full)}\n\n")
    out.append(f"_generated {time.strftime('%Y-%m-%d %H:%M:%S')}_\n\n")
    out.append(f"This report surfaces (a) the active judge configuration per run, "
               f"(b) rows where `name_sim` looks unfairly low, and (c) the largest "
               f"score disagreements across runs. The reasoning chain (synonyms, "
               f"tokens, semantic cosine, gates) is included so you can decide "
               f"*why* a row scored where it did.\n\n")

    # Per-run sections
    for r in runs_full:
        rows = list(rows_per_run[r["id"]].values())
        out.append(_section_run_header(r, rows))
        out.append(_section_name_outliers(r, rows, args.top))
        out.append(_section_judge_reasons(rows))

    # Cross-run section
    if len(runs_full) >= 2:
        out.append(_section_disagreements(runs_full, rows_per_run, args.top))

    # Write
    if args.out:
        out_path = Path(args.out)
    else:
        diag_dir = ROOT / "data" / "diag"
        diag_dir.mkdir(parents=True, exist_ok=True)
        ts = time.strftime("%Y%m%d-%H%M%S")
        out_path = diag_dir / f"{ts}-runs-{'-'.join(str(r['id']) for r in runs_full)}.md"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text("".join(out), encoding="utf-8")
    print(str(out_path))
    return 0


if __name__ == "__main__":
    sys.exit(main())
