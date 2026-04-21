# Agent Rules

## Scope
- Work in `israel-rent-data-collector` unless a task explicitly requires `../israel-rent-data`.
- Do not modify or delete the detailed planning and handoff docs under `docs/` when creating short agent-facing context files.

## Required Commands
- Install: `pip install -e ".[dev]"`
- Local CI parity: `pre-commit run --all-files`
- CLI sanity check: `python scripts/collect.py --help`
- Probe sources: `python scripts/collect.py --probe`
- Dry run: `python scripts/collect.py --source nadlan --source cbs-table49 --source cbs-api --source boi-hedonic --dry-run`
- Full validated run: `python scripts/collect.py --validate --expected-total-2022 131000000`
- Unit tests: `pytest -m "not integration"`
- Integration tests: `pytest -m integration`
- Lint: `ruff check .`
- Format check: `ruff format --check .`
- Type check: `mypy src`

## Output Rules
- Main output must be written to `data/output/rent_benchmarks.csv`.
- Crosswalk output must be written to `data/output/locality_crosswalk.csv`.
- Do not publish to `../israel-rent-data/` unless validation passes.
- After a validated run, copy:
  - `data/output/rent_benchmarks.csv` -> `../israel-rent-data/rent_benchmarks.csv`
  - `data/output/locality_crosswalk.csv` -> `../israel-rent-data/locality_crosswalk.csv`

## Git And PR Rules
- Feature branches should use the `codex/` prefix by default.
- Use the repo-specific GitHub MCPs first when they support the action.
- Fall back to `gh` only for unsupported GitHub operations such as PR creation or metadata updates the MCP cannot perform.
- Do not treat feature work as complete until the implementation branch is pushed and a non-draft PR exists with a detailed body.
- If the repo has a relevant milestone, assign it before handoff.
- Do not stage unrelated local changes. Leave pre-existing out-of-scope edits alone.

## Code Boundaries
- Python package root: `src/rent_collector/`.
- CLI entrypoint: `scripts/collect.py` and `src/rent_collector/cli.py`.
- Pipeline orchestration belongs in `src/rent_collector/pipeline.py`.
- Source-specific fetch and parse logic belongs only in `src/rent_collector/collectors/`.
- Shared schemas belong in `src/rent_collector/models.py`.
- Runtime configuration belongs in `src/rent_collector/config.py`.
- Shared HTTP and locality utilities belong in `src/rent_collector/utils/`.
- Generated CSVs belong under `data/output/`; do not hardcode output paths outside the repo.

## Data Source Constraints
- Use only official government or Bank of Israel sources already documented in this repo.
- Do not add Madlan, Yad2, or other commercial real-estate sources.
- Prefer nadlan data over CBS Table 4.9, CBS Table 4.9 over CBS API, and model fallback last.

## Validation Constraints
- Validation target: annualized total rent must be at least `131000000` NIS.
- Sanity bounds for published output:
  - no rows with `rent_nis < 500`
  - no rows with `rent_nis > 20000`

## Context Files
- Keep `AGENTS.md` static and short.
- Keep `.agent-plan.md` limited to current branch state, immediate next steps, and links to deeper docs.
- Use `llms.txt` as a high-density architecture index only.
