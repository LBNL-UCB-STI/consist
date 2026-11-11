from __future__ import annotations

from collections.abc import Mapping
import json
import logging
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    List,
    Literal,
    Optional,
    Type,
    get_args,
    get_origin,
)

from consist.artifact import Artifact
from consist.core.stores import get_hot_data_ledger
from consist.digest import canonical_json_sha256
from consist.meta import strip_system_meta
from consist.models.run import Run
from consist.types import DriverType

if TYPE_CHECKING:
    from sqlmodel import SQLModel

    from consist.core.persistence import DatabaseManager
    from consist.core.tracker import Tracker
    from consist.store.ledger import StoreLedger


def _schema_view_name(
    *,
    run_id: str,
    primary_uri: str,
    member: Optional[str],
) -> str:
    base = {
        "container_uri": primary_uri,
        "member": member,
    }
    digest = canonical_json_sha256(base)[:12]
    safe_run = "".join(ch for ch in run_id if ch.isalnum() or ch == "_")
    if not safe_run:
        safe_run = "run"
    return f"consist_tmp_{safe_run}_{digest}"


def _json_size_bytes(obj: Any) -> int:
    return len(
        json.dumps(
            obj,
            sort_keys=True,
            ensure_ascii=True,
            separators=(",", ":"),
        ).encode("utf-8")
    )


def _profile_duckdb_table_on_ledger(
    *,
    ledger: StoreLedger,
    table_schema: str,
    table_name: str,
    source: str = "duckdb",
    sample_rows: Optional[int] = None,
) -> Any:
    from consist.tools.schema_profile import (
        MAX_FIELDS,
        MAX_INLINE_PROFILE_BYTES,
        MAX_SCHEMA_JSON_BYTES,
        PROFILE_VERSION,
        SchemaFieldProfile,
        SchemaProfileResult,
    )

    with ledger.exec_cursor() as cur:
        rows = cur.execute(
            "SELECT column_name, data_type, is_nullable, ordinal_position "
            "FROM information_schema.columns "
            "WHERE table_catalog = current_database() "
            "AND table_schema = ? AND table_name = ? "
            "ORDER BY ordinal_position",
            [table_schema, table_name],
        ).fetchall()

    fields: List[SchemaFieldProfile] = []
    for col_name, data_type, is_nullable, ordinal in rows:
        fields.append(
            SchemaFieldProfile(
                name=str(col_name),
                logical_type=str(data_type).lower(),
                nullable=str(is_nullable).upper() == "YES",
                ordinal_position=int(ordinal),
            )
        )

    truncated_flags: Dict[str, Any] = {
        "fields": False,
        "schema_json": False,
        "inline_profile": False,
    }
    field_rows = [f.to_row() for f in fields]
    hash_obj: Dict[str, Any] = {
        "profile_version": PROFILE_VERSION,
        "source": source,
        "table_schema": table_schema,
        "table_name": table_name,
        "sample_rows": sample_rows,
        "fields": field_rows,
    }
    schema_id = canonical_json_sha256(hash_obj)
    summary = {
        "profile_version": PROFILE_VERSION,
        "schema_id": schema_id,
        "source": source,
        "table_schema": table_schema,
        "table_name": table_name,
        "sample_rows": sample_rows,
        "n_columns": len(fields),
        "truncated": truncated_flags,
    }

    profile_obj: Dict[str, Any] = dict(hash_obj)
    if len(fields) > MAX_FIELDS:
        truncated_flags["fields"] = True
        profile_obj["fields"] = []

    schema_json: Optional[Dict[str, Any]] = profile_obj
    if _json_size_bytes(profile_obj) > MAX_SCHEMA_JSON_BYTES:
        truncated_flags["schema_json"] = True
        schema_json = None

    inline_profile_json: Optional[Dict[str, Any]] = profile_obj
    if schema_json is None or _json_size_bytes(profile_obj) > MAX_INLINE_PROFILE_BYTES:
        truncated_flags["inline_profile"] = True
        inline_profile_json = None

    return SchemaProfileResult(
        schema_id=schema_id,
        summary=summary,
        schema_json=schema_json,
        inline_profile_json=inline_profile_json,
        fields=fields,
    )


def _extract_model_table_name(schema_model: Type[Any]) -> str:
    for cls in schema_model.__mro__:
        raw_table = cls.__dict__.get("__tablename__")
        if isinstance(raw_table, str) and raw_table:
            return raw_table
    return schema_model.__name__


def _resolve_fk_target(field_info: Any) -> Optional[str]:
    try:
        raw_fk = field_info.foreign_key
    except AttributeError:
        raw_fk = None
    if isinstance(raw_fk, str) and raw_fk:
        return raw_fk

    try:
        sa_column = field_info.sa_column
    except AttributeError:
        sa_column = None
    if sa_column is None:
        return None
    fk_set = sa_column.foreign_keys if hasattr_sa_foreign_keys(sa_column) else None
    if not fk_set:
        return None
    for fk in fk_set:
        try:
            target = fk.target_fullname
            if target:
                return str(target)
        except AttributeError:
            pass
    return None


def hasattr_sa_foreign_keys(sa_column: Any) -> bool:
    try:
        _ = sa_column.foreign_keys
        return True
    except AttributeError:
        return False


class ArtifactSchemaManager:
    """
    Handles schema discovery and persistence for artifacts on ``StoreLedger``.

    The manager:
    - profiles an ingested DuckDB table (post-dlt "ground truth")
    - persists a deduped schema blob + normalized per-field rows
    - records per-artifact observations for historical/audit use
    - returns updated immutable ``Artifact`` handles carrying ``schema_id`` and summary metadata
    """

    def __init__(self, tracker: Tracker):
        self.tracker = tracker

    def _metadata_db(self) -> DatabaseManager | None:
        metadata_store = self.tracker.metadata_store
        if metadata_store is not None:
            return metadata_store.db
        return self.tracker.db

    def _hot_ledger(self) -> StoreLedger | None:
        return get_hot_data_ledger(self.tracker)

    def _type_annotation_to_logical_type(self, annotation: Any) -> str:
        """
        Convert a Python type annotation to a logical_type string for schema storage.
        """
        origin = get_origin(annotation)
        if origin is type(None):
            return "varchar"

        if origin is not None:
            args = get_args(annotation)
            non_none_types = [arg for arg in args if arg is not type(None)]
            if non_none_types:
                return self._type_annotation_to_logical_type(non_none_types[0])

        type_mapping = {
            str: "varchar",
            int: "integer",
            float: "double",
            bool: "boolean",
            bytes: "blob",
        }
        if annotation in type_mapping:
            return type_mapping[annotation]

        type_str = str(annotation).lower()
        if "datetime.datetime" in type_str or "datetime" in type_str:
            return "timestamp"
        if "datetime.date" in type_str or "date" in type_str:
            return "date"
        if "decimal" in type_str or "numeric" in type_str:
            return "decimal(18, 2)"
        return "varchar"

    def profile_user_provided_schema(
        self,
        *,
        artifact: Artifact,
        run: Run,
        schema_model: Type[SQLModel],
        source: str = "user_provided",
    ) -> Artifact:
        """
        Store a user-provided SQLModel/Pydantic schema as an artifact profile and return
        the updated ``Artifact`` (attaching schema metadata in memory even when ``db_path=None``).
        """
        metadata_db = self._metadata_db()

        try:
            from consist.models.artifact_schema import (
                ArtifactSchema,
                ArtifactSchemaField,
                ArtifactSchemaObservation,
                ArtifactSchemaRelation,
            )

            fields: list[ArtifactSchemaField] = []
            relations: list[dict[str, str]] = []
            table_name = _extract_model_table_name(schema_model)

            def _split_fk_target(target: str) -> Optional[tuple[str, str]]:
                parts = [part for part in str(target).split(".") if part]
                if len(parts) < 2:
                    return None
                return ".".join(parts[:-1]), parts[-1]

            if schema_model.model_fields:
                for ordinal, (field_name, field_info) in enumerate(
                    schema_model.model_fields.items(), start=1
                ):
                    annotation = (
                        field_info.annotation
                        if field_info.annotation is not None
                        else str
                    )
                    logical_type = self._type_annotation_to_logical_type(annotation)
                    nullable = not bool(field_info.is_required())

                    fields.append(
                        ArtifactSchemaField(
                            schema_id="",
                            ordinal_position=ordinal,
                            name=field_name,
                            logical_type=logical_type,
                            nullable=nullable,
                            stats_json=None,
                            is_enum=False,
                            enum_values_json=None,
                        )
                    )

                    fk_target = _resolve_fk_target(field_info)
                    if fk_target:
                        parsed = _split_fk_target(fk_target)
                        if parsed is not None:
                            to_table, to_field = parsed
                            relations.append(
                                {
                                    "from": field_name,
                                    "to_table": to_table,
                                    "to_field": to_field,
                                }
                            )

            field_rows = [
                {
                    "name": f.name,
                    "logical_type": f.logical_type,
                    "nullable": f.nullable,
                    "ordinal_position": f.ordinal_position,
                }
                for f in fields
            ]
            sorted_relations = sorted(
                relations,
                key=lambda rel: (
                    rel["from"],
                    rel["to_table"],
                    rel["to_field"],
                ),
            )
            hash_obj: Dict[str, Any] = {
                "profile_version": 1,
                "table_name": table_name,
                "fields": field_rows,
                "relations": sorted_relations,
            }
            schema_id = canonical_json_sha256(hash_obj)
            summary_payload: Dict[str, Any] = {
                "table_name": table_name,
                "n_columns": len(fields),
                "profile_version": 1,
            }
            meta_updates: Dict[str, Any] = {
                "schema_name": schema_model.__name__,
                "schema_summary": summary_payload,
            }

            if metadata_db is None:
                return artifact.with_schema(
                    schema_id, summary=summary_payload
                ).with_meta(meta_updates, merge=True)

            schema_row = ArtifactSchema(
                id=schema_id,
                profile_version=1,
                summary_json={
                    "profile_version": 1,
                    "schema_id": schema_id,
                    "table_name": table_name,
                    "n_columns": len(fields),
                    "truncated": {
                        "fields": False,
                        "schema_json": False,
                        "inline_profile": False,
                    },
                },
                profile_json=hash_obj,
            )
            field_rows_with_schema_id = [
                ArtifactSchemaField(
                    schema_id=schema_id,
                    ordinal_position=f.ordinal_position,
                    name=f.name,
                    logical_type=f.logical_type,
                    nullable=f.nullable,
                    stats_json=f.stats_json,
                    is_enum=f.is_enum,
                    enum_values_json=f.enum_values_json,
                )
                for f in fields
            ]
            relation_rows = [
                ArtifactSchemaRelation(
                    schema_id=schema_id,
                    from_field=rel["from"],
                    to_table=rel["to_table"],
                    to_field=rel["to_field"],
                    relationship_type="foreign_key",
                    cardinality=None,
                )
                for rel in sorted_relations
            ]

            updated = metadata_db.persist_artifact_schema_profile(
                artifact=artifact,
                schema=schema_row,
                fields=field_rows_with_schema_id,
                relations=relation_rows,
                observation=ArtifactSchemaObservation(
                    artifact_id=artifact.id,
                    schema_id=schema_id,
                    run_id=run.id,
                    source=source,
                    sample_rows=None,
                ),
                meta_updates=meta_updates,
            )
            logging.info(
                "[Consist] Stored user-provided schema for artifact=%s (schema_id=%s, source=%s)",
                artifact.key,
                schema_id,
                source,
            )
            return updated

        except Exception as e:
            logging.warning(
                "[Consist] Failed to profile user-provided schema for artifact=%s: %s",
                artifact.key,
                e,
            )
            return artifact

    def profile_ingested_table(
        self,
        *,
        artifact: Artifact,
        run: Run,
        table_schema: str,
        table_name: str,
        source: str = "duckdb",
    ) -> Artifact:
        """
        Profile a materialized (ingested) DuckDB table on ``StoreLedger`` and persist schema records.
        """
        metadata_db = self._metadata_db()
        hot_ledger = self._hot_ledger()
        if metadata_db is None or hot_ledger is None:
            return artifact

        try:
            from consist.models.artifact_schema import (
                ArtifactSchema,
                ArtifactSchemaField,
                ArtifactSchemaObservation,
            )

            result = _profile_duckdb_table_on_ledger(
                ledger=hot_ledger,
                table_schema=table_schema,
                table_name=table_name,
                source=source,
            )
            truncated = result.summary.get("truncated") or {}
            if any(bool(v) for v in truncated.values()):
                logging.warning(
                    "[Consist] Schema profile for table=%s.%s was truncated (flags=%s). "
                    "Per-field rows are still stored for schema export, but the full JSON profile may be unavailable. "
                    "If this is a sparse wide table, consider reshaping to a long format before ingestion.",
                    table_schema,
                    table_name,
                    truncated,
                )

            schema_row = ArtifactSchema(
                id=result.schema_id,
                profile_version=result.summary.get("profile_version", 1),
                summary_json=result.summary,
                profile_json=result.schema_json,
            )
            field_rows = [
                ArtifactSchemaField(
                    schema_id=result.schema_id,
                    ordinal_position=f.ordinal_position,
                    name=f.name,
                    logical_type=f.logical_type,
                    nullable=f.nullable,
                    stats_json=f.stats,
                    is_enum=f.is_enum,
                    enum_values_json=f.enum_values,
                )
                for f in result.fields
            ]

            meta_updates: Dict[str, Any] = {
                "schema_summary": result.summary,
            }
            if result.inline_profile_json is not None:
                meta_updates["schema_profile"] = result.inline_profile_json

            return metadata_db.persist_artifact_schema_profile(
                artifact=artifact,
                schema=schema_row,
                fields=field_rows,
                relations=[],
                observation=ArtifactSchemaObservation(
                    artifact_id=artifact.id,
                    schema_id=result.schema_id,
                    run_id=run.id,
                    source=result.summary.get("source", source),
                    sample_rows=result.summary.get("sample_rows"),
                ),
                meta_updates=meta_updates,
            )

        except Exception as e:
            logging.warning(
                "[Consist] Failed to profile ingested schema for artifact=%s table=%s.%s: %s",
                artifact.key,
                table_schema,
                table_name,
                e,
            )
            return artifact

    def profile_file_artifact(
        self,
        *,
        artifact: Artifact,
        run: Run,
        resolved_path: str,
        driver: Literal["parquet", "csv", "gtfs", "h5_table"],
        sample_rows: Optional[int],
        reuse_if_unchanged: bool = False,
        source: str = "file",
    ) -> Artifact:
        """
        Profile a file-based artifact and persist schema records on ``StoreLedger``.
        """
        metadata_db = self._metadata_db()
        if metadata_db is None:
            return artifact

        try:
            if artifact.schema_id:
                return artifact
            from consist.models.artifact_schema import (
                ArtifactSchema,
                ArtifactSchemaField,
                ArtifactSchemaObservation,
            )
            from consist.tools.schema_profile import (
                MAX_INLINE_PROFILE_BYTES,
                profile_file_schema,
            )

            def _is_inline_profile(obj: Any) -> bool:
                if obj is None:
                    return False
                return _json_size_bytes(obj) <= MAX_INLINE_PROFILE_BYTES

            table_path: Optional[str] = None
            if driver in DriverType.table_path_drivers():
                table_path = artifact.member
                if not table_path:
                    logging.warning(
                        "[Consist] Missing member/table_path for %s schema profile: %s",
                        driver,
                        artifact.key,
                    )
                    return artifact

            if reuse_if_unchanged:
                prior_pair = metadata_db.find_schema_observation_for_anchor(
                    artifact.identity.anchor
                )
                if prior_pair is not None:
                    prior_obs, _prior_summary = prior_pair
                    schema_bundle = metadata_db.get_artifact_schema(
                        schema_id=prior_obs.schema_id,
                        backfill_ordinals=False,
                    )
                    if schema_bundle is not None:
                        schema_row, _fields = schema_bundle
                        meta_updates: Dict[str, Any] = {}
                        if isinstance(schema_row.summary_json, Mapping):
                            meta_updates["schema_summary"] = dict(
                                schema_row.summary_json
                            )
                        if _is_inline_profile(schema_row.profile_json):
                            meta_updates["schema_profile"] = schema_row.profile_json
                        return metadata_db.persist_artifact_schema_observation_bundle(
                            artifact=artifact,
                            observation=ArtifactSchemaObservation(
                                artifact_id=artifact.id,
                                schema_id=schema_row.id,
                                run_id=run.id,
                                source=prior_obs.source,
                                sample_rows=prior_obs.sample_rows,
                            ),
                            meta_updates=meta_updates,
                        )

            primary_loc = artifact.primary_location
            primary_uri = primary_loc.uri if primary_loc is not None else resolved_path
            result = profile_file_schema(
                path=resolved_path,
                driver=driver,
                sample_rows=sample_rows,
                table_path=table_path,
                source=source,
                view_name=_schema_view_name(
                    run_id=str(run.id),
                    primary_uri=primary_uri,
                    member=artifact.member,
                ),
            )
            truncated = result.summary.get("truncated") or {}
            if any(bool(v) for v in truncated.values()):
                logging.warning(
                    "[Consist] Schema profile for file=%s was truncated (flags=%s). "
                    "Per-field rows are still stored for schema export, but the full JSON profile may be unavailable.",
                    resolved_path,
                    truncated,
                )

            schema_row = ArtifactSchema(
                id=result.schema_id,
                profile_version=result.summary.get("profile_version", 1),
                summary_json=result.summary,
                profile_json=result.schema_json,
            )
            field_rows = [
                ArtifactSchemaField(
                    schema_id=result.schema_id,
                    ordinal_position=f.ordinal_position,
                    name=f.name,
                    logical_type=f.logical_type,
                    nullable=f.nullable,
                    stats_json=f.stats,
                    is_enum=f.is_enum,
                    enum_values_json=f.enum_values,
                )
                for f in result.fields
            ]

            cleaned_updates = dict(
                strip_system_meta(
                    {
                        "schema_summary": result.summary,
                        **(
                            {"schema_profile": result.inline_profile_json}
                            if result.inline_profile_json is not None
                            else {}
                        ),
                    }
                )
            )

            return metadata_db.persist_artifact_schema_profile(
                artifact=artifact,
                schema=schema_row,
                fields=field_rows,
                relations=[],
                observation=ArtifactSchemaObservation(
                    artifact_id=artifact.id,
                    schema_id=result.schema_id,
                    run_id=run.id,
                    source=result.summary.get("source", source),
                    sample_rows=result.summary.get("sample_rows"),
                ),
                meta_updates=cleaned_updates,
            )

        except Exception as e:
            logging.warning(
                "[Consist] Failed to profile file schema for artifact=%s path=%s: %s",
                artifact.key,
                resolved_path,
                e,
            )
            return artifact
