from __future__ import annotations

from typing import Any, Dict, Hashable, Iterable, List, Optional, Union, TYPE_CHECKING

from consist.core.indexing import FacetIndex, IndexBySpec, RunFieldIndex
from consist.models.run import Run
from consist.models.run_config_kv import RunConfigKV

if TYPE_CHECKING:
    from consist.core.tracker import Tracker


class RunQueryService:
    """
    Query and config-diff helpers for Tracker-backed run metadata.
    """

    def __init__(self, tracker: "Tracker") -> None:
        self._tracker = tracker

    def find_runs(
        self,
        tags: Optional[List[str]] = None,
        year: Optional[int] = None,
        iteration: Optional[int] = None,
        model: Optional[str] = None,
        status: Optional[str] = None,
        parent_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        limit: int = 100,
        index_by: Optional[Union[str, IndexBySpec]] = None,
        name: Optional[str] = None,
    ) -> Union[List[Run], Dict[Hashable, Run]]:
        runs: List[Run] = []
        if self._tracker.db:
            runs = self._tracker.db.find_runs(
                tags, year, iteration, model, status, parent_id, metadata, limit, name
            )

        if index_by:
            if isinstance(index_by, FacetIndex):
                facet_key = index_by.key
                if not self._tracker.db:
                    return {}
                values_by_run = self._tracker.db.get_facet_values_for_runs(
                    [r.id for r in runs],
                    key=facet_key,
                    namespace=model,
                )
                return {values_by_run[r.id]: r for r in runs if r.id in values_by_run}

            if isinstance(index_by, RunFieldIndex):
                return {getattr(r, index_by.field): r for r in runs}

            if isinstance(index_by, str) and (
                index_by.startswith("facet.") or index_by.startswith("facet:")
            ):
                if not self._tracker.db:
                    return {}
                facet_key = (
                    index_by.split(".", 1)[1]
                    if index_by.startswith("facet.")
                    else index_by.split(":", 1)[1]
                )
                values_by_run = self._tracker.db.get_facet_values_for_runs(
                    [r.id for r in runs],
                    key=facet_key,
                    namespace=model,
                )
                return {values_by_run[r.id]: r for r in runs if r.id in values_by_run}

            if not isinstance(index_by, str):
                raise TypeError(f"Unsupported index_by type: {type(index_by)}")
            return {getattr(r, index_by): r for r in runs}

        return runs

    def find_run(self, **kwargs: Any) -> Run:
        run_id = kwargs.pop("id", None) or kwargs.pop("run_id", None)

        if run_id:
            run = self._tracker.get_run(run_id)
            if not run:
                raise ValueError(f"No run found with ID: {run_id}")
            return run

        kwargs["limit"] = 2
        results = self.find_runs(**kwargs)

        if not results:
            raise ValueError(f"No run found matching criteria: {kwargs}")

        if len(results) > 1:
            raise ValueError(
                f"Multiple runs ({len(results)}+) found matching criteria: {kwargs}. Narrow your search."
            )

        return results[0]

    def find_latest_run(
        self,
        *,
        parent_id: Optional[str] = None,
        model: Optional[str] = None,
        status: Optional[str] = None,
        year: Optional[int] = None,
        tags: Optional[List[str]] = None,
        metadata: Optional[Dict[str, Any]] = None,
        limit: int = 10_000,
    ) -> Run:
        runs_result = self.find_runs(
            parent_id=parent_id,
            model=model,
            status=status,
            year=year,
            tags=tags,
            metadata=metadata,
            limit=limit,
        )
        runs = (
            list(runs_result.values()) if isinstance(runs_result, dict) else runs_result
        )
        if not runs:
            raise ValueError("No runs found matching criteria for find_latest_run().")

        def _latest_key(run: Run) -> tuple[int, int, float, str]:
            has_iteration = 1 if run.iteration is not None else 0
            iteration_value = run.iteration if run.iteration is not None else -1
            created_at_ts = run.created_at.timestamp()
            return (has_iteration, iteration_value, created_at_ts, run.id)

        return max(runs, key=_latest_key)

    def get_latest_run_id(self, **kwargs: Any) -> str:
        return self.find_latest_run(**kwargs).id

    def get_config_facet(self, facet_id: str):
        if not self._tracker.db:
            return None
        return self._tracker.db.get_config_facet(facet_id)

    def get_config_facets(
        self,
        *,
        namespace: Optional[str] = None,
        schema_name: Optional[str] = None,
        limit: int = 100,
    ):
        if not self._tracker.db:
            return []
        return self._tracker.db.get_config_facets(
            namespace=namespace, schema_name=schema_name, limit=limit
        )

    def get_run_config_kv(
        self,
        run_id: str,
        *,
        namespace: Optional[str] = None,
        prefix: Optional[str] = None,
        limit: int = 10_000,
    ):
        if not self._tracker.db:
            return []
        return self._tracker.db.get_run_config_kv(
            run_id, namespace=namespace, prefix=prefix, limit=limit
        )

    def _resolve_config_namespace(
        self, run_id: str, namespace: Optional[str]
    ) -> Optional[str]:
        if namespace is not None:
            return namespace
        run = self._tracker.get_run(run_id)
        return run.model_name if run else None

    @staticmethod
    def _coerce_config_kv_value(row: RunConfigKV) -> Any:
        if row.value_type == "json":
            return row.value_json
        if row.value_type == "str":
            return row.value_str
        if row.value_type == "int":
            return int(row.value_num) if row.value_num is not None else None
        if row.value_type == "float":
            return float(row.value_num) if row.value_num is not None else None
        if row.value_type == "bool":
            return row.value_bool
        return None

    def get_config_values(
        self,
        run_id: str,
        *,
        namespace: Optional[str] = None,
        prefix: Optional[str] = None,
        keys: Optional[Iterable[str]] = None,
        limit: int = 10_000,
    ) -> Dict[str, Any]:
        resolved_namespace = self._resolve_config_namespace(run_id, namespace)
        rows = self.get_run_config_kv(
            run_id, namespace=resolved_namespace, prefix=prefix, limit=limit
        )
        if not rows:
            return {}

        if keys is not None:
            key_set = set(keys)
            rows = [row for row in rows if row.key in key_set]

        return {row.key: self._coerce_config_kv_value(row) for row in rows}

    def diff_runs(
        self,
        run_id_a: str,
        run_id_b: str,
        *,
        namespace: Optional[str] = None,
        prefix: Optional[str] = None,
        keys: Optional[Iterable[str]] = None,
        limit: int = 10_000,
        include_equal: bool = False,
    ) -> Dict[str, Any]:
        namespace_a = self._resolve_config_namespace(run_id_a, namespace)
        namespace_b = self._resolve_config_namespace(run_id_b, namespace)

        left = self.get_config_values(
            run_id_a,
            namespace=namespace_a,
            prefix=prefix,
            keys=keys,
            limit=limit,
        )
        right = self.get_config_values(
            run_id_b,
            namespace=namespace_b,
            prefix=prefix,
            keys=keys,
            limit=limit,
        )

        changes: Dict[str, Dict[str, Any]] = {}
        all_keys = set(left.keys()) | set(right.keys())
        for key in sorted(all_keys):
            in_left = key in left
            in_right = key in right
            left_val = left.get(key)
            right_val = right.get(key)
            if in_left and not in_right:
                status = "removed"
            elif in_right and not in_left:
                status = "added"
            elif left_val == right_val:
                status = "same"
            else:
                status = "changed"
            if status == "same" and not include_equal:
                continue
            changes[key] = {
                "left": left_val,
                "right": right_val,
                "status": status,
            }

        return {
            "namespace": {"left": namespace_a, "right": namespace_b},
            "changes": changes,
        }

    def get_config_value(
        self,
        run_id: str,
        key: str,
        *,
        namespace: Optional[str] = None,
        default: Any = None,
    ) -> Any:
        resolved_namespace = self._resolve_config_namespace(run_id, namespace)
        rows = self.get_run_config_kv(
            run_id, namespace=resolved_namespace, prefix=key, limit=10_000
        )
        for row in rows:
            if row.key == key:
                return self._coerce_config_kv_value(row)
        return default

    def find_runs_by_facet_kv(
        self,
        *,
        namespace: str,
        key: str,
        value_type: Optional[str] = None,
        value_str: Optional[str] = None,
        value_num: Optional[float] = None,
        value_bool: Optional[bool] = None,
        limit: int = 100,
    ):
        if not self._tracker.db:
            return []
        return self._tracker.db.find_runs_by_facet_kv(
            namespace=namespace,
            key=key,
            value_type=value_type,
            value_str=value_str,
            value_num=value_num,
            value_bool=value_bool,
            limit=limit,
        )
