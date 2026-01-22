# AGENTS notes

## Project overview
- `main.py` is the entry point that groups input dataframes by key columns and calls `process_dataframes` to build output frames.
- `processing.py` contains the core logic for parsing metric columns (`*_new`/`*_prom`), choosing the preferred version, and merging samples on key columns.
- `scenario_tester.py` is a lightweight script used to sanity-check processing logic via sample dataframes and assertions.

## Column naming rules
- Metric columns end with `_new` or `_prom` (optionally with a `status_` prefix) and are normalized in `process_dataframes`.
- Output metric columns always end with `_auto_driver_score` to simplify downstream column recognition.

## Development tips
- Use `scenario_tester.py` to validate column selection/merging changes quickly.
- Keep key columns intact; only non-key metric columns are renamed.
