"""
RunSet primitives for multi-run grouping and alignment workflows.

This module provides a fluent layer over ``Tracker.find_runs(...)`` for common
analysis patterns:

1. Partitioning runs by a field or facet key.
2. Selecting latest runs by grouping keys.
3. Aligning two run collections 1:1 for pairwise comparison.
"""

from __future__ import annotations

from collections import OrderedDict
from dataclasses import dataclass, field
from datetime import datetime
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Iterable,
    Iterator,
    List,
    Optional,
)

import pandas as pd

from consist.models.run import Run
from consist.models.run_config_kv import RunConfigKV

if TYPE_CHECKING:
    from consist.core.tracker import Tracker


_RUN_FIELD_ALIASES: Dict[str, str] = {
    "model": "model_name",
    "parent_id": "parent_run_id",
    "run_id": "id",
}


def _coerce_config_kv_value(row: RunConfigKV) -> object | None:
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


def _sort_value(value: object | None) -> tuple[int, object]:
    if value is None:
        return (0, 0)
    if isinstance(value, bool):
        return (1, int(value))
    if isinstance(value, (int, float)):
        return (2, float(value))
    if isinstance(value, datetime):
        return (3, value.timestamp())
    if isinstance(value, str):
        return (4, value)
    if isinstance(value, tuple):
        return (5, tuple(_sort_value(item) for item in value))
    return (6, repr(value))


@dataclass
class RunSet:
    """
    Ordered run collection with grouping and alignment helpers.

    Parameters
    ----------
    runs : List[Run]
        Run records included in this collection.
    label : Optional[str], default None
        Optional descriptive label propagated to derived RunSets and
        ``to_frame()`` output.

    Notes
    -----
    Methods are non-destructive. Operations like ``filter``, ``latest``, and
    ``split_by`` return new RunSet instances.
    """

    runs: List[Run]
    label: Optional[str] = None
    _tracker: Optional["Tracker"] = field(default=None, repr=False, compare=False)

    def __post_init__(self) -> None:
        self.runs = list(self.runs)

    @classmethod
    def from_query(
        cls,
        tracker: "Tracker",
        label: Optional[str] = None,
        **filters: Any,
    ) -> "RunSet":
        """
        Build a tracker-backed RunSet from ``Tracker.find_runs`` filters.

        Parameters
        ----------
        tracker : Tracker
            Tracker used to execute the query and resolve facet fields.
        label : Optional[str], default None
            Optional label for the returned RunSet.
        **filters : Any
            Keyword filters forwarded directly to ``Tracker.find_runs``.

        Returns
        -------
        RunSet
            Tracker-backed RunSet containing matching runs.
        """
        runs = tracker.find_runs(**filters)
        run_list = list(runs.values()) if isinstance(runs, dict) else list(runs)
        return cls(runs=run_list, label=label, _tracker=tracker)

    @classmethod
    def from_runs(cls, runs: Iterable[Run], label: Optional[str] = None) -> "RunSet":
        """
        Build a RunSet from an existing iterable of runs.

        Parameters
        ----------
        runs : Iterable[Run]
            Source run objects.
        label : Optional[str], default None
            Optional label for the returned RunSet.

        Returns
        -------
        RunSet
            New RunSet containing the provided runs. Field-based helpers work on
            these sets, but facet-based helpers require a tracker-backed RunSet
            created with ``RunSet.from_query(...)`` or ``Tracker.run_set(...)``.

        Notes
        -----
        Use this constructor when you already have concrete ``Run`` objects and
        only need field-based operations such as positional access or grouping on
        built-in run attributes like ``year`` or ``status``. If you need facet-
        aware helpers such as ``filter(scenario=...)`` or ``split_by("seed")``,
        build the RunSet from a tracker-backed query instead so facet values can
        be loaded from the provenance store.
        """
        return cls(runs=list(runs), label=label)

    def split_by(self, field: str) -> OrderedDict[object, "RunSet"]:
        """
        Partition runs into keyed sub-RunSets by field or facet value.

        Parameters
        ----------
        field : str
            Run field (for example ``"status"``, ``"model"``, ``"year"``) or
            flattened facet key (for example ``"scenario_id"`` or ``"seed"``).

        Returns
        -------
        OrderedDict[object, RunSet]
            Ordered dict keyed by the resolved field value, sorted ascending.
            Missing values are grouped under ``None``.
        """
        if not self.runs:
            return OrderedDict()

        values_by_run = self._values_for_fields([field])
        grouped: Dict[object, List[Run]] = {}
        for run in self.runs:
            key = values_by_run[run.id].get(field)
            grouped.setdefault(key, []).append(run)

        ordered = OrderedDict()
        for key in sorted(grouped.keys(), key=_sort_value):
            ordered[key] = self._clone(grouped[key])
        return ordered

    def filter(self, **field_values: object) -> "RunSet":
        """
        Filter runs by exact field/facet matches.

        Parameters
        ----------
        **field_values : object
            Key-value predicates. A run is retained only if all predicates match.

        Returns
        -------
        RunSet
            New RunSet with runs that satisfy all predicates.
        """
        if not self.runs:
            return self._clone([])
        if not field_values:
            return self._clone(self.runs)

        values_by_run = self._values_for_fields(list(field_values.keys()))
        filtered = [
            run
            for run in self.runs
            if all(
                values_by_run[run.id].get(field) == value
                for field, value in field_values.items()
            )
        ]
        return self._clone(filtered)

    def latest(self, group_by: Optional[List[str]] = None) -> "RunSet":
        """
        Keep the most recent run by ``created_at`` globally or per group.

        Parameters
        ----------
        group_by : Optional[List[str]], default None
            Grouping fields/facet keys. When omitted, returns a single-run RunSet
            containing the overall latest run.

        Returns
        -------
        RunSet
            New RunSet containing latest run(s) for each group.
        """
        if not self.runs:
            return self._clone([])

        if not group_by:
            latest_run = max(self.runs, key=self._latest_key)
            return self._clone([latest_run])

        values_by_run = self._values_for_fields(group_by)
        latest_by_group: Dict[tuple[object, ...], Run] = {}
        for run in self.runs:
            group_key = tuple(values_by_run[run.id].get(field) for field in group_by)
            current = latest_by_group.get(group_key)
            if current is None or self._latest_key(run) > self._latest_key(current):
                latest_by_group[group_key] = run

        ordered_group_keys = sorted(latest_by_group.keys(), key=_sort_value)
        return self._clone([latest_by_group[key] for key in ordered_group_keys])

    def align(self, other: "RunSet", on: str) -> "AlignedPair":
        """
        Align two RunSets 1:1 on a shared field or facet key.

        Parameters
        ----------
        other : RunSet
            Comparison RunSet.
        on : str
            Alignment key. Can reference a Run field or facet key.

        Returns
        -------
        AlignedPair
            Pair object containing only keys present on both sides, in sorted order.

        Raises
        ------
        ValueError
            If either side has duplicate values for ``on``.
        """
        left_values = self._values_for_fields([on])
        right_values = other._values_for_fields([on])

        left_lookup: Dict[object, Run] = {}
        right_lookup: Dict[object, Run] = {}

        for run in self.runs:
            key = left_values[run.id].get(on)
            if key in left_lookup:
                raise ValueError(
                    f"Cannot align on '{on}': left RunSet has duplicate alignment key "
                    f"{key!r}. Use .latest() or .filter() first."
                )
            left_lookup[key] = run

        for run in other.runs:
            key = right_values[run.id].get(on)
            if key in right_lookup:
                raise ValueError(
                    f"Cannot align on '{on}': right RunSet has duplicate alignment key "
                    f"{key!r}. Use .latest() or .filter() first."
                )
            right_lookup[key] = run

        shared_keys = sorted(
            set(left_lookup).intersection(right_lookup), key=_sort_value
        )
        left_tracker = self._tracker or other._tracker
        right_tracker = other._tracker or self._tracker
        left = RunSet(
            runs=[left_lookup[key] for key in shared_keys],
            label=self.label,
            _tracker=left_tracker,
        )
        right = RunSet(
            runs=[right_lookup[key] for key in shared_keys],
            label=other.label,
            _tracker=right_tracker,
        )
        return AlignedPair(on=on, left=left, right=right, keys=list(shared_keys))

    def __iter__(self) -> Iterator[Run]:
        """Iterate over runs in collection order."""
        return iter(self.runs)

    def __len__(self) -> int:
        """Return the number of runs in the collection."""
        return len(self.runs)

    def __getitem__(self, index: int) -> Run:
        """Return run at positional index."""
        return self.runs[index]

    def to_frame(self) -> pd.DataFrame:
        """
        Materialize a run summary DataFrame.

        Returns
        -------
        pandas.DataFrame
            One row per run with base columns:
            ``run_id, label, status, model, created_at, ended_at`` plus one
            column per facet key present across the RunSet.
        """
        columns = ["run_id", "label", "status", "model", "created_at", "ended_at"]
        if not self.runs:
            return pd.DataFrame(columns=columns)

        facets_by_run = self._load_facet_values()
        facet_keys = sorted(
            {key for values in facets_by_run.values() for key in values.keys()}
            - set(columns)
        )
        rows: List[Dict[str, object]] = []
        for run in self.runs:
            row: Dict[str, object] = {
                "run_id": run.id,
                "label": self.label,
                "status": run.status,
                "model": run.model_name,
                "created_at": run.created_at,
                "ended_at": run.ended_at,
            }
            for key in facet_keys:
                row[key] = facets_by_run.get(run.id, {}).get(key)
            rows.append(row)
        return pd.DataFrame(rows, columns=columns + facet_keys)

    def _clone(self, runs: Iterable[Run]) -> "RunSet":
        return RunSet(runs=list(runs), label=self.label, _tracker=self._tracker)

    @staticmethod
    def _latest_key(run: Run) -> tuple[float, str]:
        created_ts = run.created_at.timestamp() if run.created_at else float("-inf")
        return (created_ts, run.id)

    @staticmethod
    def _run_field_name(field: str) -> str:
        return _RUN_FIELD_ALIASES.get(field, field)

    def _resolve_run_field(self, run: Run, field: str) -> tuple[bool, object | None]:
        run_field_name = self._run_field_name(field)
        if run_field_name in Run.model_fields:
            return True, run.__dict__.get(run_field_name)
        return False, None

    def _values_for_fields(
        self, fields: List[str]
    ) -> Dict[str, Dict[str, object | None]]:
        values_by_run = {run.id: {} for run in self.runs}
        unresolved_fields: set[str] = set()

        for run in self.runs:
            for field_name in fields:
                found, value = self._resolve_run_field(run, field_name)
                if found:
                    values_by_run[run.id][field_name] = value
                else:
                    unresolved_fields.add(field_name)

        if unresolved_fields and self._tracker is None:
            missing = ", ".join(sorted(unresolved_fields))
            raise RuntimeError(
                "Facet-based RunSet operations require a tracker-backed RunSet. "
                f"Could not resolve: {missing}. Build the RunSet with "
                "RunSet.from_query(...) or Tracker.run_set(...)."
            )

        if unresolved_fields:
            facets_by_run = self._load_facet_values(fields=unresolved_fields)
            for run in self.runs:
                run_facets = facets_by_run.get(run.id, {})
                for field in unresolved_fields:
                    values_by_run[run.id][field] = run_facets.get(field)

        return values_by_run

    def _load_facet_values(
        self, fields: Optional[set[str]] = None
    ) -> Dict[str, Dict[str, object | None]]:
        if not self.runs or self._tracker is None:
            return {}
        queries = self._tracker.queries

        run_ids = [run.id for run in self.runs]
        run_model_by_id = {run.id: run.model_name for run in self.runs}
        rows = queries.get_run_config_kv_for_runs(
            run_ids,
            keys=sorted(fields) if fields else None,
        )
        if not rows:
            return {}

        values_by_run: Dict[str, Dict[str, object | None]] = {}
        namespace_by_run_key: Dict[str, Dict[str, str]] = {}
        for row in rows:
            run_values = values_by_run.setdefault(row.run_id, {})
            run_namespaces = namespace_by_run_key.setdefault(row.run_id, {})
            key = row.key
            value = _coerce_config_kv_value(row)
            if key not in run_values:
                run_values[key] = value
                run_namespaces[key] = row.namespace
                continue

            preferred_namespace = run_model_by_id.get(row.run_id)
            selected_namespace = run_namespaces.get(key)
            if (
                preferred_namespace is not None
                and row.namespace == preferred_namespace
                and selected_namespace != preferred_namespace
            ):
                run_values[key] = value
                run_namespaces[key] = row.namespace

        return values_by_run


@dataclass
class AlignedPair:
    """
    Two RunSets matched 1:1 along a shared field/facet dimension.

    Parameters
    ----------
    on : str
        Field/facet key used for alignment.
    left : RunSet
        Left-hand RunSet with keys ordered to match ``keys``.
    right : RunSet
        Right-hand RunSet with keys ordered to match ``keys``.
    keys : List[object]
        Shared alignment key values present in both RunSets.
    """

    on: str
    left: RunSet
    right: RunSet
    keys: List[object]

    def _resolve_diff_tracker(self) -> "Tracker":
        left_tracker = self.left._tracker
        right_tracker = self.right._tracker
        if left_tracker is None and right_tracker is None:
            raise RuntimeError(
                "AlignedPair.config_diffs requires Tracker-backed RunSets. "
                "Build RunSets with RunSet.from_query(...) first."
            )
        if left_tracker is None:
            if right_tracker is None:
                raise RuntimeError(
                    "AlignedPair.config_diffs requires Tracker-backed RunSets. "
                    "Build RunSets with RunSet.from_query(...) first."
                )
            return right_tracker
        if right_tracker is None:
            return left_tracker
        if left_tracker is right_tracker:
            return left_tracker

        left_db_path = left_tracker.db_path
        right_db_path = right_tracker.db_path
        if left_db_path is not None and right_db_path is not None:
            if str(left_db_path) == str(right_db_path):
                return left_tracker

        raise RuntimeError(
            "AlignedPair.config_diffs requires both RunSets to use the same "
            "tracker/database."
        )

    def pairs(self) -> Iterator[tuple[Run, Run]]:
        """
        Iterate over matched ``(left_run, right_run)`` pairs.

        Yields
        ------
        tuple[Run, Run]
            Pair of aligned runs.
        """
        for left_run, right_run in zip(self.left, self.right):
            yield left_run, right_run

    def apply(self, fn: Callable[[Run, Run, object], pd.DataFrame]) -> pd.DataFrame:
        """
        Apply a pairwise function over aligned runs and concatenate results.

        Parameters
        ----------
        fn : Callable[[Run, Run, object], pandas.DataFrame]
            Function called as ``fn(left_run, right_run, key)`` for each pair.

        Returns
        -------
        pandas.DataFrame
            Concatenated DataFrame with an added ``_align_key`` column.

        Raises
        ------
        TypeError
            If ``fn`` does not return a pandas DataFrame.
        """
        frames: List[pd.DataFrame] = []
        for key, (left_run, right_run) in zip(self.keys, self.pairs()):
            frame = fn(left_run, right_run, key)
            if not isinstance(frame, pd.DataFrame):
                raise TypeError(
                    "AlignedPair.apply expected fn to return a pandas DataFrame."
                )
            with_key = frame.copy()
            with_key["_align_key"] = key
            frames.append(with_key)

        if not frames:
            return pd.DataFrame(columns=["_align_key"])
        return pd.concat(frames, ignore_index=True)

    def config_diffs(
        self,
        namespace: Optional[str] = None,
        prefix: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Compute config diffs for each aligned pair using ``Tracker.diff_runs``.

        Parameters
        ----------
        namespace : Optional[str], default None
            Namespace passed to ``Tracker.diff_runs``.
        prefix : Optional[str], default None
            Optional key prefix filter passed to ``Tracker.diff_runs``.

        Returns
        -------
        pandas.DataFrame
            Columns: ``on_value, key, namespace, status, left_value, right_value``.

        Raises
        ------
        RuntimeError
            If neither RunSet is tracker-backed.
        """
        tracker = self._resolve_diff_tracker()

        rows: List[Dict[str, object]] = []
        for on_value, (left_run, right_run) in zip(self.keys, self.pairs()):
            diff = tracker.diff_runs(
                left_run.id,
                right_run.id,
                namespace=namespace,
                prefix=prefix,
                include_equal=True,
            )
            namespace_info = diff.get("namespace", {})
            left_ns = namespace_info.get("left")
            right_ns = namespace_info.get("right")
            if namespace is not None:
                row_namespace: Optional[str] = namespace
            elif left_ns == right_ns:
                row_namespace = left_ns
            else:
                row_namespace = f"{left_ns}|{right_ns}"

            for key, change in diff.get("changes", {}).items():
                rows.append(
                    {
                        "on_value": on_value,
                        "key": key,
                        "namespace": row_namespace,
                        "status": change.get("status"),
                        "left_value": change.get("left"),
                        "right_value": change.get("right"),
                    }
                )

        columns = [
            "on_value",
            "key",
            "namespace",
            "status",
            "left_value",
            "right_value",
        ]
        return pd.DataFrame(rows, columns=columns)

    def to_frame(self) -> pd.DataFrame:
        """
        Materialize aligned-pair summary rows.

        Returns
        -------
        pandas.DataFrame
            Columns:
            ``key, left_run_id, right_run_id, left_status, right_status``.
        """
        rows: List[Dict[str, object]] = []
        for key, (left_run, right_run) in zip(self.keys, self.pairs()):
            rows.append(
                {
                    "key": key,
                    "left_run_id": left_run.id,
                    "right_run_id": right_run.id,
                    "left_status": left_run.status,
                    "right_status": right_run.status,
                }
            )
        return pd.DataFrame(
            rows,
            columns=[
                "key",
                "left_run_id",
                "right_run_id",
                "left_status",
                "right_status",
            ],
        )
