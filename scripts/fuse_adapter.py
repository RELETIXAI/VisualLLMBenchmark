#!/usr/bin/env python3
"""
Fuse a LoRA adapter into a VLM base model.

mlx-vlm 0.5.x doesn't ship a `fuse` CLI (only mlx-lm does, and it skips
vision weights). This script does the same thing the mlx_lm.fuse main
function does, but using mlx_vlm.load / save_weights / save_config so
the vision tower and processor files survive.

Output is a self-contained model directory you can pass directly to
mlx_vlm.convert (Phase 8 step 2) or to MLXProvider as a model_id with
no adapter_path needed.

Usage:
    python scripts/fuse_adapter.py \\
        --model /Users/samueltoma/AI/models/source/google--gemma-4-E4B \\
        --adapter-path data/adapters/0006-google--gemma-4-E4B \\
        --save-path /Users/samueltoma/AI/models/source/google--gemma-4-E4B-merged
"""
from __future__ import annotations

import argparse
import json
import shutil
import sys
from pathlib import Path

import mlx.core as mx
import mlx.nn as nn
from mlx.utils import tree_unflatten

from mlx_vlm.utils import load, save_config, save_weights
from mlx_vlm.trainer.utils import apply_lora_layers, set_module_by_name
from mlx_vlm.trainer.lora import LoRaLayer


def _merge_lora_layer(layer: LoRaLayer) -> nn.Linear:
    """Bake a LoRaLayer back into a plain nn.Linear.

    mlx_vlm's LoRaLayer forward pass is:
        y = original_layer(x) + scale * (x @ A @ B)
    For nn.Linear, original_layer(x) = x @ W.T + b, so:
        y = x @ W.T + b + scale * (x @ A @ B)
          = x @ (W.T + scale * A @ B) + b
          = x @ W_merged.T + b   where  W_merged = W + scale * (A @ B).T

    A has shape (input_dims, rank); B has shape (rank, output_dims);
    so A @ B is (input_dims, output_dims) and (A @ B).T is
    (output_dims, input_dims) — exactly the shape of W.

    NOTE: mlx_vlm.trainer.lora.replace_lora_with_linear is BROKEN — it
    only iterates model.layers (top level) and computes the merge with
    a shape-mismatched `A @ B` (no transpose). We do it correctly here.
    """
    orig = layer.original_layer
    if not isinstance(orig, nn.Linear):
        # mlx_vlm also wraps nn.QuantizedLinear; for our bf16 base this
        # branch isn't hit. If you ever fuse on a quantized base, this
        # function needs to dequantize first.
        raise NotImplementedError(
            f"merge for {type(orig).__name__} not implemented; base must be nn.Linear"
        )
    W = orig.weight                       # (out, in)
    AB = layer.A @ layer.B                # (in, out)
    # Cast the LoRA delta to the base weight's dtype before adding so we
    # don't promote bf16 base weights to float32 — that would 2× the
    # saved file size for no quality benefit. Trainer keeps A/B in fp32
    # for stable gradients; at fuse time we collapse back.
    W_merged = W + (layer.scale * AB.T).astype(W.dtype)  # (out, in)

    # Build a fresh Linear with the merged weight. Bias is preserved.
    out_dim, in_dim = W_merged.shape
    has_bias = (orig.bias is not None) if hasattr(orig, "bias") else False
    new_lin = nn.Linear(in_dim, out_dim, bias=has_bias)
    new_lin.weight = W_merged
    if has_bias:
        new_lin.bias = orig.bias
    return new_lin


def _copy_processor_files(src: Path, dst: Path) -> list[str]:
    """Copy tokenizer + image-processor files from src to dst.

    These files are NOT serialized by save_weights / save_config and the
    runtime needs them to actually run inference. Pattern matches what
    HuggingFace `from_pretrained` looks for.
    """
    copied = []
    patterns = [
        # Tokenizer
        "tokenizer.json", "tokenizer_config.json",
        "tokenizer.model", "spiece.model",
        "special_tokens_map.json", "added_tokens.json",
        "vocab.json", "merges.txt",
        # Image processor
        "preprocessor_config.json", "processor_config.json",
        # Chat template
        "chat_template.jinja", "chat_template.json",
        # Generation
        "generation_config.json",
    ]
    for pat in patterns:
        for f in src.glob(pat):
            shutil.copy2(f, dst / f.name)
            copied.append(f.name)
    return copied


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--model", required=True,
                    help="Base model directory (e.g. .../source/google--gemma-4-E4B)")
    ap.add_argument("--adapter-path", required=True,
                    help="Adapter directory containing adapters.safetensors + adapter_config.json")
    ap.add_argument("--save-path", required=True,
                    help="Output directory for the fused model (will be created)")
    ap.add_argument("--dequantize", action="store_true",
                    help="Dequantize the model after fusing (only relevant if base was quantized)")
    args = ap.parse_args()

    model_path = Path(args.model).resolve()
    adapter_path = Path(args.adapter_path).resolve()
    save_path = Path(args.save_path).resolve()

    if not model_path.exists():
        print(f"ERROR: model path not found: {model_path}", file=sys.stderr)
        return 2
    if not adapter_path.exists():
        print(f"ERROR: adapter path not found: {adapter_path}", file=sys.stderr)
        return 2
    if not (adapter_path / "adapter_config.json").exists():
        print(f"ERROR: {adapter_path}/adapter_config.json missing — "
              f"is this really an adapter directory?", file=sys.stderr)
        return 2

    print(f"[1/5] Loading base model from {model_path}")
    # NOTE: mlx_vlm.load returns (model, processor). We don't need the
    # processor for the fuse itself but we'll copy its files manually.
    model, _processor = load(str(model_path))

    print(f"[2/5] Applying LoRA adapter from {adapter_path}")
    model = apply_lora_layers(model, str(adapter_path))

    print(f"[3/5] Fusing LoRA layers into base linear weights")
    # mlx_vlm wraps target linears with LoRaLayer (not the same class as
    # mlx_lm's LoRALinear, and without a .fuse() method). We walk every
    # named_module in the language_model, find LoRaLayer instances,
    # compute W_merged = W + scale·(A·B).T, and swap each one back to a
    # plain nn.Linear in-place via set_module_by_name.
    n_fused = 0
    # Collect first so we don't mutate the iterator we're walking
    targets = [
        (name, m) for name, m in model.language_model.named_modules()
        if isinstance(m, LoRaLayer)
    ]
    for name, layer in targets:
        merged = _merge_lora_layer(layer)
        set_module_by_name(model.language_model, name, merged)
        n_fused += 1
    print(f"      → {n_fused} LoRaLayer modules fused into nn.Linear")
    if n_fused == 0:
        print("      ⚠  no LoRaLayer instances found — adapter wasn't applied "
              "to the language_model, or model structure differs. Aborting.")
        return 3

    save_path.mkdir(parents=True, exist_ok=True)

    print(f"[4/5] Writing weights to {save_path}")
    save_weights(save_path, model, donate_weights=True)

    # Read the original config and pop quantization hints if dequantizing
    cfg_path = model_path / "config.json"
    config = json.loads(cfg_path.read_text())
    if args.dequantize:
        config.pop("quantization", None)
        config.pop("quantization_config", None)
    save_config(config, save_path / "config.json")

    print(f"[5/5] Copying tokenizer + processor files")
    copied = _copy_processor_files(model_path, save_path)
    if not copied:
        print("      ⚠  no tokenizer/processor files copied — the saved "
              "model will not load. Check the source path.")
    else:
        print(f"      → {len(copied)} files: {', '.join(copied)}")

    print(f"\nDone. Fused model at: {save_path}")
    print(f"Next step (optional): quantize for fast inference:")
    print(f"  python -m mlx_vlm convert --hf-path {save_path} \\\n"
          f"    --mlx-path {save_path}-q4 --quantize -q-bits 4")
    return 0


if __name__ == "__main__":
    sys.exit(main())
