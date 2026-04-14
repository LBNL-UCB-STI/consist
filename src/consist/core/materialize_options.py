from __future__ import annotations

from typing import Sequence

VALID_MATERIALIZE_ON_MISSING = frozenset({"warn", "raise"})
VALID_MATERIALIZE_DB_FALLBACK = frozenset({"never", "if_ingested"})


def normalize_materialize_output_keys(
    keys: Sequence[str] | None,
    *,
    caller: str,
) -> tuple[str, ...] | None:
    """Normalize key selection for historical output recovery APIs.

    Parameters
    ----------
    keys : Sequence[str] | None
        Requested output keys. ``None`` means "all outputs for the run".
    caller : str
        Friendly caller name used in validation error messages.

    Returns
    -------
    tuple[str, ...] | None
        Tuple form of ``keys`` preserving caller order, or ``None`` when all
        outputs should be selected.

    Raises
    ------
    TypeError
        If ``keys`` is a scalar string/bytes value or contains non-string
        entries.
    """
    if keys is None:
        return None
    if isinstance(keys, (str, bytes)):
        raise TypeError(
            "keys must be a sequence of output-key strings, not a single str/bytes value."
        )

    normalized: list[str] = []
    for key in keys:
        if not isinstance(key, str):
            raise TypeError(
                "keys must contain only strings "
                f"(got {type(key).__name__!s} in {caller})."
            )
        normalized.append(key)
    return tuple(normalized)


def validate_materialize_option(
    *,
    name: str,
    value: str,
    allowed: set[str] | frozenset[str],
) -> None:
    """Validate a runtime option shared by hydration/materialization helpers.

    Parameters
    ----------
    name : str
        Option name shown in any validation error.
    value : str
        User-provided option value.
    allowed : set[str] | frozenset[str]
        Accepted values for the option.

    Raises
    ------
    ValueError
        If ``value`` is not present in ``allowed``.
    """
    if value not in allowed:
        allowed_display = ", ".join(repr(item) for item in sorted(allowed))
        raise ValueError(f"{name} must be one of: {allowed_display}")
