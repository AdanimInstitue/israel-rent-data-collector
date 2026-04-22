# AGENTS

## Commands

- Install: `python -m pip install -e ".[dev]"`
- Lint: `ruff check src tests`
- Type check: `mypy src`
- Unit tests: `pytest -m "not integration"`
- Integration tests: `pytest -m integration`
- Full test suite: `pytest`
- Build public bundle: `indc build-public-bundle`
- Validate public bundle: `indc validate-public-bundle`

## Branching

- Use `codex/<topic>` or `refactor/<topic>` for agent branches.
- Keep changes scoped to this repository only.

## Hard Constraints

- Treat this repository as a complete public collector project.
- Keep docs, tests, fixtures, configs, and manifests public-safe.
- Do not reference private repositories, private workflows, sibling paths, or hidden enrichment in tracked public files.
- Do not add acquisition guidance for sources with unclear reuse posture.
- Keep source-rights wording conservative and source-specific.

## Architecture Boundaries

- Collector code lives under `src/rent_collector/`.
- Public source configs live under `configs/sources/`.
- Public release docs live under `docs/`.
- Release artifacts must stay under `data/public_bundle/` and use relative paths in manifests.
