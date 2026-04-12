from __future__ import annotations

import uuid
from typing import Any, Dict, Iterable, List, Optional, TYPE_CHECKING, Union

from consist.core._tracker_service_base import _TrackerServiceBase
from consist.models.artifact import Artifact, set_tracker_ref
from consist.models.run import Run

if TYPE_CHECKING:
    from consist.core.persistence import ArtifactSchemaSelection, SchemaProfileSource


class TrackerArtifactQueryService(_TrackerServiceBase):
    def get_artifact(
        self,
        key_or_id: Union[str, uuid.UUID],
        *,
        run_id: Optional[str] = None,
    ) -> Optional[Artifact]:
        """
        Retrieve an artifact by semantic key or artifact id.

        Parameters
        ----------
        key_or_id : str | uuid.UUID
            Artifact key or artifact UUID.
        run_id : Optional[str], optional
            Optional run id that limits lookup to artifacts linked to that run.

        Returns
        -------
        Optional[Artifact]
            Matching artifact, or ``None`` when no match exists.
        """
        if self.db:
            artifact = self.db.get_artifact(key_or_id, run_id=run_id)
            if artifact is not None:
                set_tracker_ref(artifact, self._tracker)
            return artifact
        return None

    def find_artifacts_with_same_content(
        self, artifact: Union[Artifact, str, uuid.UUID]
    ) -> List[Artifact]:
        """
        Find artifacts that share the same content identity.

        Parameters
        ----------
        artifact : Artifact | str | uuid.UUID
            Artifact instance or identifier to resolve first.

        Returns
        -------
        List[Artifact]
            Artifacts with the same ``content_id`` as the target artifact.
        """
        if not self.db:
            return []
        target = (
            artifact if isinstance(artifact, Artifact) else self.get_artifact(artifact)
        )
        if target is None or target.content_id is None:
            return []
        artifacts = self.db.find_artifacts_by_content_id(target.content_id)
        for item in artifacts:
            set_tracker_ref(item, self._tracker)
        return artifacts

    def find_runs_producing_same_content(
        self, artifact: Union[Artifact, str, uuid.UUID]
    ) -> List[str]:
        """
        Find run ids that produced artifacts with the same content identity.

        Parameters
        ----------
        artifact : Artifact | str | uuid.UUID
            Artifact instance or identifier to resolve first.

        Returns
        -------
        List[str]
            Run ids linked to output artifacts sharing the same ``content_id``.
        """
        if not self.db:
            return []
        target = (
            artifact if isinstance(artifact, Artifact) else self.get_artifact(artifact)
        )
        if target is None or target.content_id is None:
            return []
        return self.db.find_runs_producing_content(target.content_id)

    def select_artifact_schema_for_artifact(
        self,
        *,
        artifact_id: Union[str, uuid.UUID],
        source: Optional["SchemaProfileSource"] = None,
        strict_source: bool = False,
    ) -> Optional["ArtifactSchemaSelection"]:
        """
        Resolve schema-selection metadata for an artifact.

        Parameters
        ----------
        artifact_id : str | uuid.UUID
            Artifact identifier to inspect.
        source : Optional[SchemaProfileSource], optional
            Preferred schema profile source.
        strict_source : bool, default False
            Require the selected schema to come from ``source`` when provided.

        Returns
        -------
        Optional[ArtifactSchemaSelection]
            Selection metadata used to explain which schema was chosen.
        """
        if not self.db:
            raise ValueError(
                "Schema selection requires a configured database (db_path)."
            )
        artifact_uuid = (
            uuid.UUID(artifact_id) if isinstance(artifact_id, str) else artifact_id
        )
        return self.db.select_artifact_schema_for_artifact(
            artifact_id=artifact_uuid,
            prefer_source=source,
            strict_source=strict_source,
        )

    def get_artifact_by_uri(
        self,
        uri: str,
        *,
        table_path: Optional[str] = None,
        array_path: Optional[str] = None,
    ) -> Optional[Artifact]:
        """
        Find an artifact by URI and optional sub-path selectors.

        Parameters
        ----------
        uri : str
            Artifact container URI to match.
        table_path : Optional[str], optional
            Optional table path to match for container-backed artifacts.
        array_path : Optional[str], optional
            Optional array path to match for array-backed artifacts.

        Returns
        -------
        Optional[Artifact]
            Matching artifact, or ``None`` when nothing matches.
        """
        if self.current_consist:
            for art in self.current_consist.inputs + self.current_consist.outputs:
                if art.container_uri != uri:
                    continue
                if table_path is not None and art.table_path != table_path:
                    continue
                if array_path is not None and art.array_path != array_path:
                    continue
                return art

        if self.db:
            artifact = self.db.get_artifact_by_uri(
                uri, table_path=table_path, array_path=array_path
            )
            if artifact is not None:
                set_tracker_ref(artifact, self._tracker)
            return artifact

        return None

    def find_artifacts(
        self,
        *,
        creator: Optional[Union[str, Run]] = None,
        consumer: Optional[Union[str, Run]] = None,
        key: Optional[str] = None,
        limit: int = 100,
    ) -> List[Artifact]:
        """
        Find artifacts by producing run, consuming run, and optional key.

        Parameters
        ----------
        creator : Optional[str | Run], optional
            Producing run or run id.
        consumer : Optional[str | Run], optional
            Consuming run or run id.
        key : Optional[str], optional
            Exact artifact key to match.
        limit : int, default 100
            Maximum number of artifacts to return.

        Returns
        -------
        List[Artifact]
            Matching artifact records.
        """
        if not self.db:
            return []

        creator_id = creator.id if isinstance(creator, Run) else creator
        consumer_id = consumer.id if isinstance(consumer, Run) else consumer

        artifacts = self.db.find_artifacts(
            creator=creator_id, consumer=consumer_id, key=key, limit=limit
        )
        for artifact in artifacts:
            set_tracker_ref(artifact, self._tracker)
        return artifacts

    @staticmethod
    def _parse_artifact_param_value(raw: str) -> tuple[str, Any]:
        value = raw.strip()
        lowered = value.lower()
        if lowered == "true":
            return "bool", True
        if lowered == "false":
            return "bool", False
        if lowered == "null":
            return "null", None
        try:
            if "." not in value and "e" not in lowered:
                return "num", int(value)
            return "num", float(value)
        except ValueError:
            return "str", value

    def _parse_artifact_param_expression(self, expression: str) -> Dict[str, Any]:
        raw = expression.strip()
        operator = None
        for candidate in (">=", "<=", "="):
            idx = raw.find(candidate)
            if idx > 0:
                operator = candidate
                lhs = raw[:idx].strip()
                rhs = raw[idx + len(candidate) :].strip()
                break

        if operator is None:
            raise ValueError(
                f"Invalid artifact facet predicate {expression!r}. "
                "Expected <key>=<value>, <key>>=<value>, or <key><=<value>."
            )
        if not lhs:
            raise ValueError(
                f"Artifact facet predicate is missing a key: {expression!r}"
            )
        if rhs == "":
            raise ValueError(
                f"Artifact facet predicate is missing a value: {expression!r}"
            )

        namespace = None
        key_path = lhs
        if "." in lhs:
            maybe_namespace, remainder = lhs.split(".", 1)
            if maybe_namespace and remainder:
                namespace = maybe_namespace
                key_path = remainder

        value_kind, value = self._parse_artifact_param_value(rhs)
        if operator in {">=", "<="} and value_kind != "num":
            raise ValueError(
                f"Artifact facet predicate {expression!r} uses {operator} with a "
                "non-numeric value."
            )

        return {
            "namespace": namespace,
            "key_path": key_path,
            "op": operator,
            "kind": value_kind,
            "value": value,
        }

    def find_artifacts_by_params(
        self,
        *,
        params: Optional[Iterable[str]] = None,
        namespace: Optional[str] = None,
        key_prefix: Optional[str] = None,
        artifact_family_prefix: Optional[str] = None,
        limit: int = 100,
    ) -> List[Artifact]:
        """
        Find artifacts by indexed facet predicates and prefix filters.

        Parameters
        ----------
        params : Optional[Iterable[str]], optional
            Facet predicate expressions such as ``year=2018``.
        namespace : Optional[str], optional
            Default namespace for predicates without an explicit namespace.
        key_prefix : Optional[str], optional
            Optional artifact-key prefix filter.
        artifact_family_prefix : Optional[str], optional
            Optional artifact-family prefix filter.
        limit : int, default 100
            Maximum number of artifacts to return.

        Returns
        -------
        List[Artifact]
            Matching artifact records.
        """
        if not self.db:
            return []

        predicates = (
            [self._parse_artifact_param_expression(param) for param in params]
            if params
            else []
        )

        artifacts = self.db.find_artifacts_by_facet_params(
            predicates=predicates,
            namespace=namespace,
            key_prefix=key_prefix,
            artifact_family_prefix=artifact_family_prefix,
            limit=limit,
        )
        for artifact in artifacts:
            set_tracker_ref(artifact, self._tracker)
        return artifacts

    def get_artifact_kv(
        self,
        artifact: Union[Artifact, uuid.UUID],
        *,
        namespace: Optional[str] = None,
        prefix: Optional[str] = None,
        limit: int = 10_000,
    ):
        """
        Retrieve flattened artifact facet key/value rows.

        Parameters
        ----------
        artifact : Artifact | uuid.UUID
            Artifact instance or artifact id.
        namespace : Optional[str], optional
            Optional namespace filter.
        prefix : Optional[str], optional
            Optional key prefix filter.
        limit : int, default 10_000
            Maximum number of rows to return.

        Returns
        -------
        list
            Flattened key/value rows for the selected artifact.
        """
        if not self.db:
            return []
        artifact_id = artifact.id if isinstance(artifact, Artifact) else artifact
        return self.db.get_artifact_kv(
            artifact_id=artifact_id,
            namespace=namespace,
            prefix=prefix,
            limit=limit,
        )
