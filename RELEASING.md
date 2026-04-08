# Releasing Consist

This is the maintainer runbook for shipping a Consist release.

Consist uses semantic versioning. Because the project is still pre-`1.0`,
minor releases may include breaking changes.

## Before You Start

- Cut the release from a branch based on the latest `main`.
- Keep ordinary feature/fix work flowing through normal PRs to `main`.
- When you are ready to ship, open a dedicated release-prep PR containing only
  release metadata and release-facing docs changes.
- Keep the release scope explicit. For `0.1.0`, that means curating everything
  since `v0.1.0-beta.3`.
- Make sure `CHANGELOG.md`, `README.md`, install docs, and `pyproject.toml`
  agree on the release story.
- Versioned docs now publish from GitHub Releases, not from ordinary pushes to
  `main`.
- Use a `vX.Y.Z` tag that matches `pyproject.toml` exactly. The docs workflow
  strips the leading `v` and publishes version `X.Y.Z`.
- Do not switch the public install instructions to `pip install consist` until
  the package is actually live on PyPI.

## Release Sequence

### 1. Prepare the release PR

- Branch from the current `main`.
- Update `CHANGELOG.md` with the user-facing changes for the release.
- Bump `pyproject.toml` to the release version.
- Rewrite or refresh release-facing docs if the process has changed.
- Update install guidance in `README.md` and `docs/` if this release changes
  the recommended installation path.
- Keep the PR narrowly scoped to release-prep changes only.
- Merge the release PR to `main` only when you are ready to publish from that
  exact resulting commit.

### 2. Identify the exact release commit

- After the release PR merges, use the resulting commit on `main` as the sole
  source of truth for the release.
- If you use squash merge, release from the squash commit on `main`.
- If you use merge commits, release from the merge commit on `main`.
- Avoid merging unrelated PRs between selecting the release commit and
  publishing/tagging it.

### 3. Run local validation

Use the project virtualenv for release checks:

```bash
uv sync --group dev
.venv/bin/python -m ruff check src tests
.venv/bin/python -m ruff format --check src tests
.venv/bin/python -m pytest -m "not heavy" tests/unit tests/integration tests/e2e
```

The release-check environment should include the non-heavy optional test
dependencies used by the BEAM and OpenMatrix suites. `uv sync --group dev`
refreshes `.venv` from the repo's development dependency group so those tests
run instead of being skipped or failing on missing imports.

If release changes touch packaging, docs, or CLI behavior, also do the
relevant smoke checks before publishing.

### 4. Build release artifacts

Build the wheel and source distribution from the exact commit you plan to tag:

```bash
uv build
```

You should end up with fresh artifacts in `dist/`.

### 5. Smoke-test the built artifacts

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

### 6. Publish to PyPI

PyPI publishing is currently manual. Publish the exact artifacts you just
tested:

```bash
.venv/bin/python -m pip install --upgrade twine
.venv/bin/python -m twine check dist/*
```

Optional dry run against TestPyPI:

```bash
.venv/bin/python -m twine upload \
  --repository testpypi \
  dist/*
```

Production upload:

```bash
.venv/bin/python -m twine upload dist/*
```

After upload completes, verify the package is live on PyPI before merging or
publishing docs that tell users to install from PyPI.

If you prefer an explicit repository URL instead of relying on local Twine
configuration, use:

```bash
.venv/bin/python -m twine upload \
  --repository-url https://upload.pypi.org/legacy/ \
  dist/*
```

### 7. Tag the release

Create the release tag from the same commit whose artifacts were uploaded:

```bash
git tag -s vX.Y.Z -m "vX.Y.Z"
git push origin vX.Y.Z
```

If you do not want a signed tag for a given release, make that decision
explicitly rather than quietly changing the process.

### 8. Publish the GitHub release

- Create the GitHub release for the new tag.
- Use the curated changelog summary as the release notes body.
- Include install guidance and any upgrade caveats.
- Publishing a non-prerelease GitHub Release now triggers the docs deploy.
- The docs job checks out the tagged commit, verifies the tag matches
  `pyproject.toml`, publishes `/X.Y.Z/`, updates the `latest` alias, and sets
  the site root redirect to `latest`.

### 9. Post-release checks

- Verify `pip install consist` works from a fresh environment.
- Verify the docs site reflects the intended install guidance after deploy.
- Verify the new version exists at `/X.Y.Z/` and that the root redirects to the
  same version through `latest`.
- Confirm the changelog compare links are correct for the new tag.
- If the release exposed friction in the process, update this file while the
  details are still fresh.

## Practical Notes for v0.1.0

- `pyproject.toml` is already set to `0.1.0`.
- `0.1.0` is the first non-beta release, so docs and README should stop
  presenting GitHub install as the default path once PyPI is live.
- There is currently no dedicated GitHub Actions workflow for PyPI publishing,
  so do not assume release automation exists.
- The docs workflow can also be backfilled manually with `workflow_dispatch`
  by providing a `release_tag`, which is useful for older tags or reruns.
