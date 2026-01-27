from __future__ import annotations

import logging
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Optional,
    Literal,
    Type,
    get_args,
    get_origin,
)

from consist.models.artifact import Artifact
from consist.models.run import Run

if TYPE_CHECKING:
    from consist.core.tracker import Tracker
    from sqlmodel import SQLModel


class ArtifactSchemaManager:
    """
    Handles schema discovery and persistence for artifacts.

    This keeps schema-specific logic out of `Tracker` to avoid it becoming a god class.
    The manager:
    - profiles an ingested DuckDB table (post-dlt "ground truth")
    - persists a deduped schema blob + normalized per-field rows
    - records per-artifact observations for historical/audit use
    - stores a small pointer/summary in `Artifact.meta`
    """

    def __init__(self, tracker: Tracker):
        self.tracker = tracker

    def _type_annotation_to_logical_type(self, annotation: Any) -> str:
        """
        Convert a Python type annotation to a logical_type string for schema storage.

        This reverses the mapping in parse_duckdb_type() by converting Python/Pydantic
        type annotations to the canonical string format (e.g., "varchar", "integer").

        Parameters
        ----------
        annotation : Any
            The type annotation from a SQLModel field (e.g., str, Optional[int], etc.)

        Returns
        -------
        str
            A canonical logical_type string (e.g., "varchar", "integer", "boolean")
        """
        # Handle Optional types: Optional[X] is Union[X, None], so unwrap it
        origin = get_origin(annotation)
        if origin is type(None):
            # NoneType itself
            return "varchar"

        # Check if this is Optional (Union with None)
        if origin is not None:
            args = get_args(annotation)
            # Optional[X] has args (X, NoneType)
            non_none_types = [arg for arg in args if arg is not type(None)]
            if non_none_types:
                annotation = non_none_types[0]
                # Recursively process the unwrapped type
                return self._type_annotation_to_logical_type(annotation)

        # Map Python builtins and common types to logical_type strings
        type_mapping = {
            str: "varchar",
            int: "integer",
            float: "double",
            bool: "boolean",
            bytes: "blob",
        }

        # Check exact type match first
        if annotation in type_mapping:
            return type_mapping[annotation]

        # Check string representation for typing constructs and special cases
        type_str = str(annotation).lower()

        # Handle datetime types
        if "datetime.datetime" in type_str or "datetime" in type_str:
            return "timestamp"
        if "datetime.date" in type_str or "date" in type_str:
            return "date"

        # Handle Decimal/Numeric
        if "decimal" in type_str or "numeric" in type_str:
            return "decimal(18, 2)"

        # Default to varchar for unknown types (conservative fallback)
        return "varchar"

    def profile_user_provided_schema(
        self,
        *,
        artifact: Artifact,
        run: Run,
        schema_model: Type[SQLModel],
        source: str = "user_provided",
    ) -> None:
        """
        Store a user-provided SQLModel schema as an artifact profile.

        When users manually define a SQLModel class for an artifact (e.g., to add
        FK constraints, indexes, or refined type information), this method converts
        that model into an ArtifactSchema record. The schema is marked with source
        "user_provided" so it can be preferred during export.

        This enables a workflow where:
        1. User profiles artifact file: profile_file_schema=True
        2. User exports SQLModel stub from file profile
        3. User manually edits stub (add FK, indexes, enums, etc.)
        4. User passes edited schema back to log_artifact(schema=MySchema)
        5. The edited schema becomes the preferred schema for export

        Parameters
        ----------
        artifact : Artifact
            The artifact being associated with this schema.
        run : Run
            The run context.
        schema_model : Type[SQLModel]
            The user-provided SQLModel class to store. Field types are extracted
            from the class definition and converted to canonical logical_type strings.
        source : str, default "user_provided"
            Source label for the schema observation record.

        Notes
        -----
        - The schema_id is computed as a hash of the normalized field definitions
        - Field ordinal positions are preserved from the model definition order
        - All fields are assumed nullable unless explicitly marked as required
        - The schema is stored deduped: if two artifacts have identical schemas,
          they share the same ArtifactSchema row
        - LIMITATION: SQLAlchemy sa_column overrides (e.g., Column("PNUM", ...))
          are not extracted; the persisted schema uses field names only. If you need
          actual database column names, pass the schema explicitly to ingest().
        """
        if not self.tracker.db or not self.tracker.engine:
            return

        try:
            from consist.models.artifact_schema import (
                ArtifactSchema,
                ArtifactSchemaField,
                ArtifactSchemaObservation,
                ArtifactSchemaRelation,
            )

            # 1. Extract fields from the SQLModel class
            fields = []
            relations: list[dict[str, str]] = []
            table_name = getattr(schema_model, "__tablename__", schema_model.__name__)

            # model_fields is a dict of field_name -> FieldInfo from Pydantic
            if hasattr(schema_model, "model_fields"):

                def _resolve_fk_target(field_info: Any) -> Optional[str]:
                    raw_fk = getattr(field_info, "foreign_key", None)
                    if raw_fk:
                        return str(raw_fk)
                    sa_column = getattr(field_info, "sa_column", None)
                    if sa_column is None:
                        return None
                    fk_set = getattr(sa_column, "foreign_keys", None)
                    if not fk_set:
                        return None
                    for fk in fk_set:
                        target = getattr(fk, "target_fullname", None)
                        if target:
                            return str(target)
                        target_col = getattr(fk, "column", None)
                        if target_col is not None:
                            return str(target_col)
                    return None

                def _split_fk_target(target: str) -> Optional[tuple[str, str]]:
                    parts = [part for part in str(target).split(".") if part]
                    if len(parts) < 2:
                        return None
                    return ".".join(parts[:-1]), parts[-1]

                for ordinal, (field_name, field_info) in enumerate(
                    schema_model.model_fields.items(), start=1
                ):
                    # Extract the type annotation from the field
                    annotation = getattr(field_info, "annotation", None)
                    if annotation is None:
                        annotation = str

                    # Convert Python type to logical_type string
                    logical_type = self._type_annotation_to_logical_type(annotation)

                    # Determine if field is nullable (default: True for safety)
                    # If required=True in the field, then it's not nullable
                    is_required = getattr(field_info, "is_required", lambda: False)()
                    nullable = not is_required

                    fields.append(
                        ArtifactSchemaField(
                            schema_id="",  # Will be set below after computing schema_id
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

            # 2. Compute schema_id by hashing the normalized field definitions.
            # IMPORTANT: We do NOT include "source" in the hash. This ensures that
            # identical schemas (same fields, types, nullability) from different sources
            # (file, duckdb, user_provided) get the SAME schema_id and automatically dedupe.
            # The "source" is tracked separately in ArtifactSchemaObservation, allowing us
            # to know which sources observed which schema without preventing deduplication.
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
            schema_id = self.tracker.identity.canonical_json_sha256(hash_obj)

            # 3. Create ArtifactSchema and ArtifactSchemaField rows.
            # NOTE: We do NOT include "source" in summary_json. The schema_id dedupes
            # across sources, so multiple sources may observe the same schema.
            # The authoritative source information is in ArtifactSchemaObservation.source.
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

            # Update field_info rows with the computed schema_id
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

            # 4. Persist schema and fields to database
            self.tracker.db.upsert_artifact_schema(
                schema_row, field_rows_with_schema_id, relation_rows
            )

            # 5. Record an observation linking this artifact to the schema
            # This is how we track that multiple sources (file, duckdb, user_provided)
            # have observed/defined schemas for the same artifact
            self.tracker.db.insert_artifact_schema_observation(
                ArtifactSchemaObservation(
                    artifact_id=artifact.id,
                    schema_id=schema_id,
                    run_id=run.id,
                    source=source,
                    sample_rows=None,
                )
            )

            # 6. Update artifact metadata with the schema reference.
            # Explicitly ensure schema_name is persisted so that ingest() can look it up
            # by name from the tracker's registered schemas. This enables the workflow
            # where log_artifact(..., schema=MySchema) is later followed by ingest()
            # without needing to pass the schema again (as long as it's registered with Tracker).
            # NOTE: We do NOT include "source" in schema_summary. Source attribution lives
            # exclusively in ArtifactSchemaObservation.source.
            meta_updates: Dict[str, Any] = {
                "schema_id": schema_id,
                "schema_name": schema_model.__name__,  # Explicit: persisted for ingest() lookup
                "schema_summary": {
                    "table_name": table_name,
                    "n_columns": len(fields),
                    "profile_version": 1,
                },
            }
            self.tracker.db.update_artifact_meta(artifact, meta_updates)

            logging.info(
                "[Consist] Stored user-provided schema for artifact=%s (schema_id=%s, source=%s)",
                getattr(artifact, "key", None),
                schema_id,
                source,
            )

        except Exception as e:
            logging.warning(
                "[Consist] Failed to profile user-provided schema for artifact=%s: %s",
                getattr(artifact, "key", None),
                e,
            )

    def profile_ingested_table(
        self,
        *,
        artifact: Artifact,
        run: Run,
        table_schema: str,
        table_name: str,
        source: str = "duckdb",
    ) -> None:
        """
        Profile a materialized (ingested) DuckDB table and persist schema records.

        Notes
        -----
        This is intended to capture the post-ingest schema (after dlt normalization),
        rather than file-side dtypes.
        """
        if not self.tracker.db or not self.tracker.engine:
            return

        try:
            from consist.models.artifact_schema import (
                ArtifactSchema,
                ArtifactSchemaField,
                ArtifactSchemaObservation,
            )
            from consist.tools.schema_profile import profile_duckdb_table

            result = profile_duckdb_table(
                engine=self.tracker.engine,
                identity=self.tracker.identity,
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

            self.tracker.db.upsert_artifact_schema(schema_row, field_rows, [])
            self.tracker.db.insert_artifact_schema_observation(
                ArtifactSchemaObservation(
                    artifact_id=artifact.id,
                    schema_id=result.schema_id,
                    run_id=run.id,
                    source=result.summary.get("source", source),
                    sample_rows=result.summary.get("sample_rows"),
                )
            )

            meta_updates: Dict[str, Any] = {
                "schema_id": result.schema_id,
                "schema_summary": result.summary,
            }
            if result.inline_profile_json is not None:
                meta_updates["schema_profile"] = result.inline_profile_json

            self.tracker.db.update_artifact_meta(artifact, meta_updates)

        except Exception as e:
            logging.warning(
                "[Consist] Failed to profile ingested schema for artifact=%s table=%s.%s: %s",
                getattr(artifact, "key", None),
                table_schema,
                table_name,
                e,
            )

    def profile_file_artifact(
        self,
        *,
        artifact: Artifact,
        run: Run,
        resolved_path: str,
        driver: Literal["parquet", "csv", "h5_table"],
        sample_rows: Optional[int],
        reuse_if_unchanged: bool = False,
        source: str = "file",
    ) -> None:
        """
        Profile a file-based artifact and persist schema records.

        Notes
        -----
        This is intended to capture a lightweight file schema snapshot without ingestion.

        Parameters
        ----------
        artifact : Artifact
            Artifact being profiled.
        run : Run
            Run context for the schema observation.
        resolved_path : str
            Resolved filesystem path to the artifact.
        driver : str
            File format driver (e.g., "csv", "parquet").
        sample_rows : Optional[int]
            Maximum rows to sample when inferring schema.
        reuse_if_unchanged : bool, default False
            If True, reuse a prior schema observation when the artifact hash matches.
        source : str, default "file"
            Source label for the schema observation.

        Returns
        -------
        None
        """
        if not self.tracker.db:
            return

        try:
            if isinstance(getattr(artifact, "meta", None), dict) and artifact.meta.get(
                "schema_id"
            ):
                return
            from consist.models.artifact_schema import (
                ArtifactSchema,
                ArtifactSchemaField,
                ArtifactSchemaObservation,
            )
            from consist.tools.schema_profile import (
                MAX_INLINE_PROFILE_BYTES,
                profile_file_schema,
            )
            import json

            def _is_inline_profile(obj: Any) -> bool:
                if obj is None:
                    return False
                return (
                    len(
                        json.dumps(
                            obj,
                            sort_keys=True,
                            ensure_ascii=True,
                            separators=(",", ":"),
                        ).encode("utf-8")
                    )
                    <= MAX_INLINE_PROFILE_BYTES
                )

            table_path = None
            if driver == "h5_table":
                if isinstance(getattr(artifact, "meta", None), dict):
                    table_path = artifact.meta.get("table_path") or artifact.meta.get(
                        "sub_path"
                    )
                if not table_path:
                    logging.warning(
                        "[Consist] Missing table_path for h5_table schema profile: %s",
                        getattr(artifact, "key", None),
                    )
                    return

            if reuse_if_unchanged and artifact.hash and self.tracker.db:
                prior_obs = self.tracker.db.find_schema_observation_for_hash(
                    artifact.hash
                )
                if prior_obs is not None:
                    schema_bundle = self.tracker.db.get_artifact_schema(
                        schema_id=prior_obs.schema_id, backfill_ordinals=False
                    )
                    if schema_bundle is not None:
                        schema_row, _fields = schema_bundle
                        self.tracker.db.insert_artifact_schema_observation(
                            ArtifactSchemaObservation(
                                artifact_id=artifact.id,
                                schema_id=schema_row.id,
                                run_id=run.id,
                                source=prior_obs.source,
                                sample_rows=prior_obs.sample_rows,
                            )
                        )
                        meta_updates: Dict[str, Any] = {
                            "schema_id": schema_row.id,
                            "schema_summary": schema_row.summary_json,
                        }
                        if _is_inline_profile(schema_row.profile_json):
                            meta_updates["schema_profile"] = schema_row.profile_json
                        self.tracker.db.update_artifact_meta(artifact, meta_updates)
                        return

            result = profile_file_schema(
                identity=self.tracker.identity,
                path=resolved_path,
                driver=driver,
                sample_rows=sample_rows,
                table_path=table_path,
                source=source,
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

            self.tracker.db.upsert_artifact_schema(schema_row, field_rows, [])
            self.tracker.db.insert_artifact_schema_observation(
                ArtifactSchemaObservation(
                    artifact_id=artifact.id,
                    schema_id=result.schema_id,
                    run_id=run.id,
                    source=result.summary.get("source", source),
                    sample_rows=result.summary.get("sample_rows"),
                )
            )

            meta_updates: Dict[str, Any] = {
                "schema_id": result.schema_id,
                "schema_summary": result.summary,
            }
            if result.inline_profile_json is not None:
                meta_updates["schema_profile"] = result.inline_profile_json

            self.tracker.db.update_artifact_meta(artifact, meta_updates)

        except Exception as e:
            logging.warning(
                "[Consist] Failed to profile file schema for artifact=%s path=%s: %s",
                getattr(artifact, "key", None),
                resolved_path,
                e,
            )
