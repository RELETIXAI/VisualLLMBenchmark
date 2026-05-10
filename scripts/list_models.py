#!/usr/bin/env python3
"""List local Gemma / Qwen / etc. models and report what's tunable on this Mac.

Scans /Users/samueltoma/AI/models/{source,mlx} for HF/MLX-style model
directories (each must contain a config.json) and classifies each as a
training base, smoke base, or conversion source based on size and quant.
"""
import json
from pathlib import Path

ROOT = Path("/Users/samueltoma/AI/models")
WIRED_GB = 45  # change if your iogpu.wired_limit_mb is different


def estimate(cfg):
    q = cfg.get("quantization")
    txt = cfg.get("text_config", {}) or {}
    n_experts = txt.get("num_experts") or cfg.get("num_experts") or 0
    moe = bool(n_experts)
    bits = (q or {}).get("bits") if isinstance(q, dict) else None
    return {"quant_bits": bits, "moe": moe, "n_experts": n_experts}


def directory_size_gb(p: Path) -> float:
    return sum(f.stat().st_size for f in p.rglob("*") if f.is_file()) / 1e9


def model_status(p: Path):
    cfg_path = p / "config.json"
    if not cfg_path.exists():
        return None
    try:
        cfg = json.loads(cfg_path.read_text())
    except Exception as e:
        return {"path": str(p), "error": f"config.json unreadable: {e}"}
    info = estimate(cfg)
    info["path"] = str(p)
    info["size_gb"] = round(directory_size_gb(p), 1)
    info["model_type"] = cfg.get("model_type")
    weights_gb = info["size_gb"]
    fits_train = (weights_gb + 14) <= WIRED_GB  # +14 GB headroom for activations
    info["tunable_on_this_mac"] = fits_train
    info["use_for"] = (
        "TRAINING base" if fits_train and info["quant_bits"] in (4, 8) else
        "smoke training" if fits_train and info["quant_bits"] is None else
        "conversion source / adapter-merge target" if not fits_train else
        "?"
    )
    return info


def main():
    rows = []
    for sub in ("source", "mlx"):
        d = ROOT / sub
        if not d.exists():
            continue
        for child in sorted(d.iterdir()):
            if not child.is_dir():
                continue
            r = model_status(child)
            if r:
                rows.append(r)
    if not rows:
        print(f"No models found under {ROOT}")
        return
    print(f"{'Path':<70} {'Size GB':>8} {'Quant':<6} {'MoE':<8} {'Tunable':<8} Use for")
    print("-" * 140)
    for r in rows:
        if "error" in r:
            print(f"{r['path']:<70} ERROR: {r['error']}")
            continue
        quant = (str(r['quant_bits']) + "-bit") if r['quant_bits'] else "bf16"
        moe = ("Y(" + str(r['n_experts']) + ")") if r['moe'] else "N"
        print(f"{r['path']:<70} {r['size_gb']:>8} {quant:<6} {moe:<8} "
              f"{'YES' if r['tunable_on_this_mac'] else 'no':<8} "
              f"{r['use_for']}")


if __name__ == "__main__":
    main()
