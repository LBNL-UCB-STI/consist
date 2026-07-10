# Identity Manager

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
versioned, policy-neutral `AdmissionReport` with a deterministic JSON form.

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
        - compute_callable_hash
        - compute_config_hash
        - compute_run_config_hash
        - compute_input_hash
        - compute_file_checksum
        - label_for_hash_input
        - digest_path
        - compute_hash_inputs_digests
