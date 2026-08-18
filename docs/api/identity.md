# Identity Manager

## Code identity

Run code identity is resolved in this order:

1. `repo_git` uses the repository commit and tracked Python working-tree state
   when the configured project root is inside a readable Git repository.
2. If repository identity is unavailable and the lifecycle has a Python
   callable, Consist hashes that callable's defining module and records the
   resolved mode as `callable_module`.
3. If neither identity is safe, Consist raises
   `CodeIdentityUnavailableError` before reusable cache lookup.

Explicit `callable_module` and `callable_source` selections are strict: they
must resolve their callable and are never silently retried as `repo_git`.
`get_code_version()` and `resolve_code_version()` retain their successful
string-returning compatibility behavior, but unavailable identities now raise
instead of returning `unknown_code_version`, `no_git_module_found`, or a
time-based fallback.

```python
from consist.core.identity import CodeIdentityUnavailableError

try:
    tracker.run(fn=step)
except CodeIdentityUnavailableError:
    # Run inside a repository or select/provide a supported callable identity.
    raise
```

The resolved mode and digest are the same values persisted on the run and used
for its final signature and cache tuple.

## Admission identity

`Artifact.hash` remains Consist's cache and provenance fingerprint. It can
represent fast metadata, a directory aggregate, or other valid cache identity,
so it is not automatically a proof that a supplied file has the same bytes.

`check_artifact_identity()` is the separate prior-run admission API. It accepts
only a full regular-file SHA-256 whose forward metadata explicitly records raw
file-byte semantics. For older or otherwise ambiguous stored hashes, callers
may supply a distinct immutable expected file; Consist hashes that file directly
instead of resolving the stored artifact URI or recovery roots, then requires it
to corroborate the stored 64-character historical fingerprint. The result is a
versioned, policy-neutral `AdmissionReport` with a deterministic JSON form. See
[Artifact Admission](admission.md) for the complete API and developer helper
reference.

::: consist.core.identity.IdentityManager
    options:
      show_source: false
      show_root_heading: false
      show_root_toc_entry: false
      members:
        - canonical_json_str
        - canonical_json_sha256
        - normalize_json
        - calculate_run_signature
        - get_code_version
        - resolve_code_version
        - resolve_code_identity
        - compute_callable_hash
        - compute_config_hash
        - compute_run_config_hash
        - compute_input_hash
        - compute_file_checksum
        - label_for_hash_input
        - digest_path
        - compute_hash_inputs_digests
