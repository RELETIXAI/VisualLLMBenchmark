# Reletix · LLM Food Vision Benchmark

A self-hosted arena to benchmark vision-capable LLMs on food/nutrition images.
Drop an Excel file (with embedded images + ground-truth nutrition columns), pick a
system prompt, and race **OpenAI · Anthropic · Gemini · Ollama · LM Studio** on
**accuracy · latency · tokens · cost**.

## Quickstart

```bash
cp .env.example .env          # paste your API keys
.venv/bin/python scripts/make_sample.py    # optional: generate a tiny demo dataset
./run.sh                       # http://localhost:8765
```

## How scoring works

Each row of your dataset is sent to the model. The output (JSON) is compared to ground truth:

- **Food name similarity** — F1 token overlap, more forgiving than Jaccard for short names matched against long ones (e.g. "bread" vs "whole wheat bread roll")
- **Description similarity** — same F1 token overlap
- **Nutrition accuracy** — tolerance-band scoring with graceful decay:

  ```
  delta   = |pred − truth|
  allowed = max(abs_tol, rel_tol · |truth|)
  score   = 1.0                                    if delta ≤ allowed
            1 − (delta − allowed) / (3 · allowed)  if delta ≤ 4 · allowed
            0                                      otherwise
  ```

- **Per-nutrient tolerance + weight** (in `backend/scoring.py`):

  | Nutrient | Abs tol | Rel tol | Weight |
  |----------|---------|---------|--------|
  | calories | 50 kcal | 12 %    | 3.0    |
  | protein  | 5 g     | 20 %    | 1.8    |
  | carbs    | 8 g     | 20 %    | 1.8    |
  | fat      | 4 g     | 25 %    | 1.5    |
  | sodium   | 200 mg  | 35 %    | 1.0    |
  | sugar    | 4 g     | 40 %    | 0.8    |
  | fiber    | 3 g     | 40 %    | 0.6    |

- **Nutrition aggregate** = weighted average (calories carry the most signal, fiber/sugar the least)
- **Overall row score** = weighted avg (food 30 %, desc 10 %, nutrition 60 %)
- **Composite leaderboard score** = `100 × (0.70·accuracy + 0.15·speed + 0.15·cost_efficiency)`

Why a band? "Sugar 2 g vs 1 g" used to penalise 33 %; now it's within tolerance → 100 %. The band protects small-magnitude nutrients from looking catastrophically wrong on what is really a 1-gram disagreement.

## Excel format

Headers are auto-detected (case-insensitive). Recognized columns:

| Canonical    | Aliases                                  |
|--------------|------------------------------------------|
| image        | image, picture, photo, img               |
| food         | food, dish, name, item, meal             |
| description  | description, explanation, details, desc  |
| calories     | calories, kcal, energy, cal              |
| protein_g    | protein, proteins, prot                  |
| carbs_g      | carbs, carbohydrates                     |
| fat_g        | fat, fats, lipids                        |
| fiber_g      | fiber, fibre                             |
| sugar_g      | sugar, sugars                            |
| sodium_mg    | sodium, salt                             |
| nutrition    | freeform fallback — parsed via regex     |

Embedded images in `.xlsx` are extracted from cell anchors and saved to `data/images/`.
You can also supply image URLs or relative paths in the `image` column.

## Architecture

```
backend/
  main.py           FastAPI server + static
  parser.py         Excel/CSV + image extraction
  scoring.py        Per-row + composite score
  pricing.py        $/Mtok table (override per run)
  db.py             SQLite store
  runner.py         Threaded benchmark executor
  providers/
    base.py         Adapter interface + JSON parsing
    openai_provider.py
    anthropic_provider.py
    gemini_provider.py
    ollama_provider.py
    lmstudio_provider.py
frontend/
  index.html, style.css, app.js   Single-page UI
data/
  benchmark.db      SQLite
  uploads/          Original Excel files
  images/           Extracted images
```

## API for automation

The same backend is consumable via HTTP. Auto-generated Swagger docs live at
[`/docs`](http://localhost:8765/docs).

### Trigger a run

```bash
curl -X POST http://localhost:8765/api/v1/runs \
  -H "Content-Type: application/json" \
  -d '{
    "prompt_text": "You are a precise food vision and nutrition expert. ...",
    "prompt_name": "v5",
    "dataset_id": 6,
    "provider": "openai",
    "model_id": "gpt-5-mini",
    "max_rows": 50,
    "random_sample": true
  }'
```

Response:

```json
{
  "run_id": 22,
  "prompt_id": 9,
  "dataset_id": 6,
  "status_url": "/api/runs/22",
  "analysis_url": "/api/runs/22/analysis.md"
}
```

`prompt_text` will create a new prompt row OR re-use an exact-match existing
one. You can also pass `prompt_id` to reuse a saved prompt explicitly, or
`dataset_name` to look up by name.

API key falls back to `OPENAI_API_KEY` / `ANTHROPIC_API_KEY` /
`GEMINI_API_KEY` from `.env` when not provided.

### Poll until done & pull analysis

```bash
RUN_ID=22
until [ "$(curl -s http://localhost:8765/api/runs/$RUN_ID | jq -r .status)" = "completed" ]; do
  sleep 5
done
curl -s "http://localhost:8765/api/runs/$RUN_ID/analysis.md" > run_$RUN_ID.md
```

### Compare runs programmatically

```bash
curl -s "http://localhost:8765/api/compare.md?ids=15,16,17" > compare.md
```

### Filter the leaderboard

```bash
curl "http://localhost:8765/api/runs?provider=openai&model_id=gpt-5-mini&status=completed"
```

### Python automation snippet

```python
import os, time, httpx

API = "http://localhost:8765"
PROMPT = open("prompts/v5.txt").read()

# Sweep multiple models
for model in ["gpt-5-mini", "gpt-4.1-mini", "claude-haiku-4-5"]:
    provider = "openai" if model.startswith("gpt") else "anthropic"
    r = httpx.post(f"{API}/api/v1/runs", json={
        "prompt_text": PROMPT,
        "prompt_name": "v5",
        "dataset_id": 6,
        "provider": provider,
        "model_id": model,
        "max_rows": 100,
    }, timeout=30).json()
    run_id = r["run_id"]
    print(f"{model} → run #{run_id}")
    # Poll
    while True:
        s = httpx.get(f"{API}/api/runs/{run_id}").json()
        if s["status"] in ("completed", "failed", "cancelled"):
            break
        time.sleep(5)
    md = httpx.get(f"{API}/api/runs/{run_id}/analysis.md").text
    open(f"runs/{model}_{run_id}.md", "w").write(md)
```

## Adding a provider

Implement `BaseProvider.run()` returning a `ProviderResult` and register it in
`backend/providers/__init__.py`. The adapter must return `text`, `parsed` (normalized
to `{food, description, nutrition: {...}}`), `input_tokens`, `output_tokens`.
