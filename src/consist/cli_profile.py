"""Profiled entry point for the Consist CLI."""

from __future__ import annotations

import cProfile
import os
import pstats
import sys
from pathlib import Path


def _bool_env(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() not in {"0", "false", "no", "off"}


def main() -> None:
    """
    Run the Consist CLI under cProfile.

    Environment variables:
      CONSIST_PROFILE_OUT: path for the .prof file (default: ./consist.prof)
      CONSIST_PROFILE_SORT: pstats sort key (default: tottime)
      CONSIST_PROFILE_TOP: number of lines to print (default: 50)
      CONSIST_PROFILE_PRINT: print summary to stderr (default: true)
    """
    out_path = Path(os.getenv("CONSIST_PROFILE_OUT", "consist.prof"))
    sort_key = os.getenv("CONSIST_PROFILE_SORT", "tottime")
    top_n = int(os.getenv("CONSIST_PROFILE_TOP", "50"))
    print_summary = _bool_env("CONSIST_PROFILE_PRINT", True)

    profiler = cProfile.Profile()
    exit_code: int | None = 0
    profiler.enable()
    try:
        from . import cli

        cli.app()
    except SystemExit as exc:
        exit_code = int(exc.code) if exc.code is not None else 0
    finally:
        profiler.disable()
        out_path.parent.mkdir(parents=True, exist_ok=True)
        profiler.dump_stats(out_path)
        if print_summary:
            stream = sys.stderr
            stream.write(f"[consist-profile] wrote {out_path}\n")
            stats = pstats.Stats(profiler, stream=stream).sort_stats(sort_key)
            stats.print_stats(top_n)

    if exit_code:
        raise SystemExit(exit_code)
