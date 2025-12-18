import logging
from typing import Any, Dict

from consist.models.artifact import Artifact
from consist.models.run import Run


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

    def __init__(self, tracker: "Tracker"):
        self.tracker = tracker

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

            self.tracker.db.upsert_artifact_schema(schema_row, field_rows)
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
