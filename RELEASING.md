# Releasing Consist

This is the maintainer runbook for shipping a Consist release.

Consist uses semantic versioning. Because the project is still pre-`1.0`,
minor releases may include breaking changes.

## Before You Start

- Cut the release from a branch based on the latest `main`.
- Keep the release scope explicit. For `0.1.0`, that means curating everything
  since `v0.1.0-beta.3`.
- Make sure `CHANGELOG.md`, `README.md`, install docs, and `pyproject.toml`
  agree on the release story.
- Remember that docs deploy automatically from pushes to `main`. Do not switch
  the public install instructions to `pip install consist` until the package is
  actually live on PyPI.

## Release Sequence

### 1. Prepare the release commit

- Update `CHANGELOG.md` with the user-facing changes for the release.
- Rewrite or refresh release-facing docs if the process has changed.
- Update install guidance in `README.md` and `docs/` if this release changes
  the recommended installation path.
- Confirm the package version in `pyproject.toml`.

### 2. Run local validation

Use the project virtualenv for release checks:

```bash
.venv/bin/python -m ruff check src tests
.venv/bin/python -m ruff format --check src tests
.venv/bin/python -m pytest -m "not heavy" tests/unit tests/integration tests/e2e
```

If release changes touch packaging, docs, or CLI behavior, also do the
relevant smoke checks before publishing.

### 3. Build release artifacts

Build the wheel and source distribution from the exact commit you plan to tag:

```bash
uv build
```

You should end up with fresh artifacts in `dist/`.

### 4. Smoke-test the built artifacts

Use a clean environment so you are testing the actual release artifacts rather
than the editable checkout.

```bash
python -m venv /tmp/consist-release-smoke
/tmp/consist-release-smoke/bin/python -m pip install --upgrade pip
/tmp/consist-release-smoke/bin/python -m pip install dist/*.whl
/tmp/consist-release-smoke/bin/python -c "import consist; print(consist.__file__)"
/tmp/consist-release-smoke/bin/consist --help
```

If the release changes optional dependency guidance, smoke-test at least one
extra as well.

### 5. Publish to PyPI

PyPI publishing is currently manual. Publish the exact artifacts you just
tested:

```bash
.venv/bin/python -m twine upload dist/*
```

After upload completes, verify the package is live on PyPI before merging or
publishing docs that tell users to install from PyPI.

### 6. Tag the release

Create the release tag from the same commit whose artifacts were uploaded:

```bash
git tag -s vX.Y.Z -m "vX.Y.Z"
git push origin vX.Y.Z
```

If you do not want a signed tag for a given release, make that decision
explicitly rather than quietly changing the process.

### 7. Publish the GitHub release

- Create the GitHub release for the new tag.
- Use the curated changelog summary as the release notes body.
- Include install guidance and any upgrade caveats.

### 8. Post-release checks

- Verify `pip install consist` works from a fresh environment.
- Verify the docs site reflects the intended install guidance after deploy.
- Confirm the changelog compare links are correct for the new tag.
- If the release exposed friction in the process, update this file while the
  details are still fresh.

## Practical Notes for v0.1.0

- `pyproject.toml` is already set to `0.1.0`.
- `0.1.0` is the first non-beta release, so docs and README should stop
  presenting GitHub install as the default path once PyPI is live.
- There is currently no dedicated GitHub Actions workflow for PyPI publishing,
  so do not assume release automation exists.
