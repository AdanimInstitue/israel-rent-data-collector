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
- Keep `pr-agent-context` on floating `v4` everywhere it is referenced in workflows, including both the reusable workflow `uses:` line and the `tool_ref` input.
- Do not pin `pr-agent-context` to a commit SHA or point release such as `v4.0.19`; floating `v4` is intentional and must stay that way.

## Workflow Guardrails

- `pr-agent-context` must remain floating major `v4` in both CI and refresh workflows, including `tool_ref: v4`.
- Treat any exact-version or SHA pin for `pr-agent-context` as a regression unless a tracked decision explicitly overrides this.

## Architecture Boundaries

- Collector code lives under `src/rent_collector/`.
- Public source configs live under `configs/sources/`.
- Public release docs live under `docs/`.
- Release artifacts must stay under `data/public_bundle/` and use relative paths in manifests.
