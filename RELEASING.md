# Releasing Consist

This project uses semantic versioning. Until `1.0.0`, breaking changes may occur in `0.y` minor releases.

## Pre-release checklist

- License is set (`LICENSE` exists) and `README.md` reflects it.
- CI is green: `pytest`, `ruff`, and `zensical serve`.
- Docs site metadata is updated in `zensical.yaml` (`site_url`, `repo_url`, `repo_name`).
- Changelog is updated for the release.
- Public API changes are reviewed:
  - Python exports in `src/consist/__init__.py`
  - CLI flags/output in `src/consist/cli.py`
  - DuckDB schema/provenance semantics (Runs/Artifacts, signatures, scenario linkage)

## Tagging a release

- Bump `pyproject.toml` `project.version` if needed.
- Create a signed tag: `git tag -s vX.Y.Z -m "vX.Y.Z"`.
- Push tag: `git push origin vX.Y.Z`.

## PyPI release checklist

- Build: `python -m build`.
- Smoke-test install from the built artifacts in a clean env.
- Publish (once configured): `python -m twine upload dist/*`.

