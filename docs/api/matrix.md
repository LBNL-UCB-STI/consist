# Matrix Views

Consist provides high-performance loading and virtualization of multidimensional
matrices (such as OMX, HDF5, and Zarr datasets) through `MatrixViewFactory`.

## Format-Neutral Logical Array Identity

When tracking or admitting matrix datasets across formats (e.g., verifying an OMX file
against an upstream Zarr store), use [`consist.digest`](digest.md):

```python
from consist.digest import (
    compute_omx_logical_matrices_sha256,
    compute_zarr_logical_matrices_sha256,
)

omx_digest = compute_omx_logical_matrices_sha256("matrices.omx")
zarr_digest = compute_zarr_logical_matrices_sha256("matrices.zarr")
```

See [Content & Tabular Digests](digest.md) and [Artifact Admission](admission.md)
for comprehensive guides on array canonicalization and boundary verification.

## View Factory

::: consist.core.matrix.MatrixViewFactory
    options:
      show_source: false
      show_root_heading: false
      show_root_toc_entry: false
      members:
        - load_matrix_view
