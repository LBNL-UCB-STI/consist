from __future__ import annotations

from typing import Iterable, List


class ArtifactKeyRegistry:
    """
    Base class for defining artifact key constants.

    Example:
        class Keys(ArtifactKeyRegistry):
            RAW = "raw"
            CLEAN = "clean"
    """

    @classmethod
    def all_keys(cls) -> List[str]:
        """Return all registered keys in declaration order."""
        keys: List[str] = []
        seen: set[str] = set()
        for base in reversed(cls.mro()):
            if base in {object, ArtifactKeyRegistry}:
                continue
            for name, value in vars(base).items():
                if name.startswith("_"):
                    continue
                if isinstance(value, str) and value not in seen:
                    keys.append(value)
                    seen.add(value)
        return keys

    @classmethod
    def validate(cls, keys: Iterable[str], *, require_all: bool = False) -> None:
        """
        Validate that provided keys exist in the registry.

        Args:
            keys: Iterable of keys to validate.
            require_all: If True, require every registry key to be present.
        """
        provided = list(keys)
        known = set(cls.all_keys())
        unknown = [key for key in provided if key not in known]
        if unknown:
            raise ValueError(
                f"Unknown artifact keys: {unknown}. Known keys: {sorted(known)}"
            )
        if require_all:
            missing = [key for key in known if key not in provided]
            if missing:
                raise ValueError(f"Missing artifact keys: {missing}.")
