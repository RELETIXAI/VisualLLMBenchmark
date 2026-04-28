"""Benchmark runner with pause/resume/cancel controls."""
from __future__ import annotations

import threading
import time
import traceback
from typing import Any

from . import db
from .parser import parse_dataset
from .pricing import cost_usd
from .providers import get_provider
from .scoring import score_row, composite_score


# In-memory control state per run.  Lives only as long as the process —
# if the server restarts, paused/running runs become orphaned (status stays
# 'running' in DB but no thread).  Acceptable for a local prototype.
RUN_CONTROLS: dict[int, dict] = {}
_LOCK = threading.Lock()


def _get_controls(run_id: int) -> dict:
    with _LOCK:
        c = RUN_CONTROLS.get(run_id)
        if not c:
            ev = threading.Event(); ev.set()
            c = {"pause_event": ev, "cancelled": False, "started": time.time()}
            RUN_CONTROLS[run_id] = c
        return c


def pause_run(run_id: int) -> bool:
    c = RUN_CONTROLS.get(run_id)
    if not c: return False
    c["pause_event"].clear()
    db.update_run(run_id, status="paused")
    return True


def resume_run(run_id: int) -> bool:
    c = RUN_CONTROLS.get(run_id)
    if not c: return False
    c["pause_event"].set()
    db.update_run(run_id, status="running")
    return True


def cancel_run(run_id: int) -> bool:
    c = RUN_CONTROLS.get(run_id)
    if not c: return False
    c["cancelled"] = True
    c["pause_event"].set()  # unblock any wait
    return True


def _run_blocking(run_id: int, dataset_path: str, system_prompt: str,
                  provider_name: str, model_id: str, api_key: str | None,
                  base_url: str | None, user_prompt: str | None,
                  pricing_override: dict | None,
                  weights: dict | None,
                  max_rows: int | None,
                  image_url_template: str | None,
                  random_sample: bool) -> None:
    controls = _get_controls(run_id)
    try:
        import random
        ds = parse_dataset(dataset_path, image_url_template=image_url_template)
        rows = ds["rows"]
        if max_rows and max_rows < len(rows):
            if random_sample:
                rng = random.Random(42)
                rows = rng.sample(rows, max_rows)
            else:
                rows = rows[:max_rows]
        provider = get_provider(provider_name, api_key=api_key, base_url=base_url)

        latencies, accs = [], []
        total_in = total_out = 0
        total_cost = 0.0

        for i, row in enumerate(rows):
            # Pause gate: blocks here while paused
            controls["pause_event"].wait()
            if controls["cancelled"]:
                db.update_run(run_id, status="cancelled", finished_at=time.time())
                return
            # Defensive: another process may have flipped our status (e.g. orphan
            # sweep from a second uvicorn that briefly imported backend.main).
            # If the DB says we're not live anymore, exit cleanly without hammering
            # the provider further.
            db_status = db.get_run_status(run_id)
            if db_status not in ("running", "paused", "pending"):
                return

            res = provider.run(
                system_prompt=system_prompt,
                image_path=row.get("image_path"),
                image_url=row.get("image_url"),
                model_id=model_id,
                user_prompt=user_prompt,
            )
            scores = score_row(res.parsed, row) if not res.error else {
                "food_sim": 0, "desc_sim": 0, "nutrition_per": {}, "macros_avg": 0,
                "ingredient_f1": 0, "weight_acc": 0, "overall": 0,
                "ingredients": {"matches": [], "unmatched_truth": [], "unmatched_pred": [],
                                "n_pred": 0, "n_truth": 0, "matched": 0,
                                "precision": 0, "recall": 0, "f1": 0, "weight_acc": 0},
                "health": {"score": None, "pred": None, "truth": None, "delta": None}
            }
            row_cost = cost_usd(res.input_tokens, res.output_tokens, model_id, provider_name, pricing_override)

            image_ref = row.get("image_url") or row.get("image_path") or row.get("image_id")
            truth_payload = {
                "food": row.get("food"),
                "description": row.get("description"),
                "nutrition": row.get("nutrition_truth"),
                "ingredients": row.get("ingredients_truth") or [],
                "health_score": row.get("health_score_truth"),
            }

            db.add_row_result(
                run_id=run_id, row_idx=row["row_idx"],
                latency_ms=res.latency_ms, input_tokens=res.input_tokens,
                output_tokens=res.output_tokens, cost_usd=row_cost,
                output_text=res.text, output_parsed=res.parsed, scores=scores,
                error=res.error, image_ref=image_ref, truth=truth_payload,
            )

            if not res.error:
                latencies.append(res.latency_ms)
                accs.append(scores["overall"])
            total_in += res.input_tokens
            total_out += res.output_tokens
            total_cost += row_cost

            avg_lat = sum(latencies) / len(latencies) if latencies else 0
            acc = sum(accs) / len(accs) if accs else 0
            comp = composite_score(acc, avg_lat, total_cost, weights)
            db.update_run(run_id,
                          n_done=i + 1, accuracy=acc, avg_latency_ms=avg_lat,
                          total_input_tokens=total_in, total_output_tokens=total_out,
                          total_cost_usd=total_cost, composite_score=comp)

        db.update_run(run_id, status="completed", finished_at=time.time())
    except Exception as e:
        db.update_run(run_id, status="failed", finished_at=time.time(),
                      error=f"{type(e).__name__}: {e}\n{traceback.format_exc()}")
    finally:
        with _LOCK:
            RUN_CONTROLS.pop(run_id, None)


def start_run(prompt_id: int, dataset_id: int, provider_name: str, model_id: str,
              api_key: str | None = None, base_url: str | None = None,
              user_prompt: str | None = None,
              pricing_override: dict | None = None,
              weights: dict | None = None,
              max_rows: int | None = None,
              random_sample: bool = True) -> int:
    prompt = db.get_prompt(prompt_id)
    dataset = db.get_dataset(dataset_id)
    if not prompt or not dataset:
        raise ValueError("Prompt or dataset not found")

    n = dataset["n_rows"] if not max_rows else min(max_rows, dataset["n_rows"])
    config = {"user_prompt": user_prompt, "pricing_override": pricing_override,
              "weights": weights, "max_rows": max_rows, "base_url": base_url,
              "random_sample": random_sample}
    run_id = db.create_run(prompt_id=prompt_id, dataset_id=dataset_id,
                           provider=provider_name, model_id=model_id, n_rows=n, config=config)
    _get_controls(run_id)  # pre-create controls so pause/cancel work immediately

    th = threading.Thread(
        target=_run_blocking, daemon=True,
        kwargs=dict(run_id=run_id, dataset_path=dataset["file_path"],
                    system_prompt=prompt["system_prompt"], provider_name=provider_name,
                    model_id=model_id, api_key=api_key, base_url=base_url,
                    user_prompt=user_prompt, pricing_override=pricing_override,
                    weights=weights, max_rows=max_rows,
                    image_url_template=dataset.get("image_url_template"),
                    random_sample=random_sample),
    )
    th.start()
    return run_id
