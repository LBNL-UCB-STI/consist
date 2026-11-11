"""
Immutable, hashable, JSON-safe metadata containers (`FrozenMeta` and `FrozenList`).

This is a stdlib-only leaf module re-exported by `consist.location`.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
import math
import sys
from typing import Any

_RO_MSG = (
    "Artifact.meta is read-only; use tracker.update_artifact_meta(art, **changes) "
    "-> Artifact"
)


def _unfreeze_meta_val(val: Any) -> Any:
    if isinstance(val, FrozenMeta):
        return val.to_dict()
    if isinstance(val, FrozenList):
        return val.to_list()
    return val


def _freeze_meta_val(val: object, path: str = "meta") -> Any:
    if (
        isinstance(val, (FrozenMeta, FrozenList))
        or val is None
        or isinstance(val, (str, bool, int))
    ):
        return val

    np_mod = sys.modules.get("numpy")
    if np_mod is not None:
        generic_cls = np_mod.__dict__.get("generic")
        if isinstance(generic_cls, type) and isinstance(val, generic_cls):
            return _freeze_meta_val(val.item(), path)

    if isinstance(val, float):
        if not math.isfinite(val):
            raise TypeError(f"{path}: non-finite float is not valid JSON metadata")
        return val
    if isinstance(val, Mapping):
        return FrozenMeta(val)
    if isinstance(val, (list, tuple)):
        return FrozenList(
            _freeze_meta_val(x, f"{path}[{i}]") for i, x in enumerate(val)
        )
    raise TypeError(
        f"{path}: {type(val).__name__} is not JSON-representable "
        "(convert sets to sorted lists and Path/UUID to str)"
    )


class FrozenList(tuple[Any, ...], Sequence[Any]):
    """Immutable, hashable sequence for list values inside `FrozenMeta`."""

    __slots__ = ()

    def __new__(cls, iterable: Iterable[Any] = ()) -> FrozenList:
        if type(iterable) is cls:
            return iterable
        return super().__new__(
            cls,
            (_freeze_meta_val(x, f"list[{i}]") for i, x in enumerate(iterable)),
        )

    def __eq__(self, other: object) -> bool:
        if isinstance(other, (list, tuple)):
            return len(self) == len(other) and all(
                a == b for a, b in zip(self, other, strict=True)
            )
        return super().__eq__(other)

    def __ne__(self, other: object) -> bool:
        res = self.__eq__(other)
        return res if res is NotImplemented else not res

    __hash__ = tuple.__hash__

    def to_list(self) -> list[Any]:
        """Recursively convert this `FrozenList` into plain Python `list`/`dict` objects."""
        return [_unfreeze_meta_val(x) for x in self]

    def __reduce__(self) -> tuple[type[FrozenList], tuple[tuple[Any, ...]]]:
        return (FrozenList, (tuple(self),))

    def __copy__(self) -> FrozenList:
        return self

    def __deepcopy__(self, memo: object) -> FrozenList:
        return self

    def __repr__(self) -> str:
        return f"FrozenList({list(self)!r})"


_EMPTY_META: FrozenMeta | None = None


class FrozenMeta(dict[str, Any], Mapping[str, Any]):
    """Immutable, hashable, pickle-safe, JSON-domain mapping for `Artifact.meta`."""

    __slots__ = ()

    def __new__(
        cls,
        data: Mapping[str, Any] | Iterable[tuple[str, Any]] = (),
        /,
        **kwargs: Any,
    ) -> FrozenMeta:
        if type(data) is cls and not kwargs:
            return data
        src: dict[Any, Any] = dict(data, **kwargs)
        if not src and cls is FrozenMeta and _EMPTY_META is not None:
            return _EMPTY_META
        if bad := [k for k in src if not isinstance(k, str)]:
            raise TypeError(f"Artifact.meta keys must be str, got: {bad!r}")
        self = super().__new__(cls)
        dict.update(
            self,
            {k: _freeze_meta_val(v, f"meta[{k!r}]") for k, v in src.items()},
        )
        return self

    def __init__(self, *args: object, **kwargs: object) -> None:
        # Populated in __new__; re-calling __init__ (e.g. on EMPTY_META) cannot mutate.
        pass

    def _readonly(self, *args: object, **kwargs: object) -> Any:
        raise TypeError(_RO_MSG)

    __setitem__ = __delitem__ = __ior__ = _readonly  # type: ignore[assignment]
    clear = pop = popitem = setdefault = update = _readonly  # type: ignore[assignment]

    def __eq__(self, other: object) -> bool:
        if isinstance(other, dict):
            return super().__eq__(other)
        if isinstance(other, Mapping):
            if len(self) != len(other):
                return False
            return all(k in other and self[k] == other[k] for k in self)
        return NotImplemented

    def __ne__(self, other: object) -> bool:
        res = self.__eq__(other)
        return res if res is NotImplemented else not res

    def __hash__(self) -> int:
        return hash((FrozenMeta, frozenset(self.items())))

    def to_dict(self) -> dict[str, Any]:
        """Recursively convert this `FrozenMeta` into plain Python `dict`/`list` objects."""
        return {k: _unfreeze_meta_val(v) for k, v in self.items()}

    def __reduce__(self) -> tuple[type[FrozenMeta], tuple[dict[str, Any]]]:
        return (FrozenMeta, (dict(self),))

    def __copy__(self) -> FrozenMeta:
        return self

    def __deepcopy__(self, memo: object) -> FrozenMeta:
        return self

    def __repr__(self) -> str:
        return f"FrozenMeta({dict(self)!r})"


EMPTY_META: FrozenMeta = FrozenMeta()
_EMPTY_META = EMPTY_META

SYSTEM_META_KEYS: frozenset[str] = frozenset(
    {
        "hash_semantics",
        "hash_algorithm",
        "hashing_strategy",
        "directory_manifest",
        "directory_artifact",
        "is_dir",
        "is_ingested",
        "canonical_table_digest",
        "admission_file_identity",
        "admission_identity",
        "logical_identity",
        "logical_profile",
        "schema_id",
        "_dlt_load_id",
        "_dlt_id",
        "_dlt_table_name",
        "_dlt_schema_name",
    }
)


_PRESERVED_META_PREFIXES: tuple[str, ...] = (
    "output_set_",
    "file_bundle_",
)

PRESERVED_SYSTEM_META_KEYS: frozenset[str] = frozenset(
    {
        "schema_profile",
        "schema_summary",
        "file_schema",
        "facet",
        "facet_namespace",
        "facet_schema_version",
        "facet_index",
        "output_set_key",
        "output_set_role",
        "output_set_index",
        "output_set_member_keys",
        "manifest_artifact_id",
        "recovery_roots",
        "year",
        "iteration",
        "tags",
    }
)


def strip_system_meta(
    raw: Mapping[str, Any] | None,
    *,
    extra_keys: Iterable[str] = (),
) -> FrozenMeta:
    """Return a `FrozenMeta` with internal v1 system keys removed."""
    if not raw:
        return EMPTY_META
    blocked = SYSTEM_META_KEYS | frozenset(extra_keys)
    cleaned = {
        str(k): v
        for k, v in raw.items()
        if (ks := str(k)) not in blocked and not ks.startswith("_dlt_")
    }
    return FrozenMeta(cleaned) if cleaned else EMPTY_META


def _unfreeze_mapping(raw: Mapping[str, Any] | None) -> dict[str, Any]:
    if not raw:
        return {}
    return {str(k): _unfreeze_meta_val(v) for k, v in raw.items()}


def preserve_system_artifact_meta(
    existing_meta: Mapping[str, Any] | None,
    incoming_meta: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Preserve `PRESERVED_SYSTEM_META_KEYS` from `existing_meta` under `incoming_meta`."""
    merged: dict[str, Any] = {}
    if existing_meta:
        for k, v in existing_meta.items():
            ks = str(k)
            if ks in PRESERVED_SYSTEM_META_KEYS or ks.startswith(
                _PRESERVED_META_PREFIXES
            ):
                merged[ks] = _unfreeze_meta_val(v)
    if incoming_meta:
        merged.update(_unfreeze_mapping(incoming_meta))
    return merged


def merge_artifact_output_meta(
    existing_meta: Mapping[str, Any] | None,
    updates: Mapping[str, Any] | None,
    *,
    merge: bool = True,
) -> dict[str, Any]:
    """Merge or replace user metadata while retaining `PRESERVED_SYSTEM_META_KEYS` on replace."""
    if not merge:
        return preserve_system_artifact_meta(existing_meta, updates)
    merged = _unfreeze_mapping(existing_meta)
    if updates:
        merged.update(_unfreeze_mapping(updates))
    return merged


__all__ = [
    "EMPTY_META",
    "FrozenList",
    "FrozenMeta",
    "PRESERVED_SYSTEM_META_KEYS",
    "SYSTEM_META_KEYS",
    "merge_artifact_output_meta",
    "preserve_system_artifact_meta",
    "strip_system_meta",
]
