from __future__ import annotations

import logging
import uuid
from typing import TYPE_CHECKING

from sqlmodel import col, select

from consist.models.artifact import Artifact, ArtifactContent
from consist.models.run import Run, resolve_canonical_run_meta_field

if TYPE_CHECKING:
    from consist.core.persistence import DatabaseManager


def apply_content_identity_compatibility(db: DatabaseManager) -> None:
    """
    Apply additive schema compatibility for content identity support.

    This is intentionally limited to schema shape updates needed for older DBs to
    open under the current code. It does not backfill rows automatically.
    """
    _ensure_artifact_content_id_column(db)
    _ensure_artifact_content_id_index(db)


def apply_artifact_parent_compatibility(db: DatabaseManager) -> None:
    """
    Apply additive schema compatibility for artifact parent-child relations.

    This adds the canonical ``artifact.parent_artifact_id`` column and index for
    older databases without requiring row backfills.
    """
    _ensure_artifact_parent_artifact_id_column(db)
    _ensure_artifact_parent_artifact_id_index(db)


def apply_run_stage_phase_compatibility(db: DatabaseManager) -> None:
    """
    Apply additive schema compatibility for run stage/phase support.

    The columns are added lazily for older databases and legacy rows are
    backfilled from ``run.meta`` when possible.
    """
    _ensure_run_stage_phase_columns(db)
    _ensure_run_stage_phase_indexes(db)
    backfill_run_stage_phase_from_meta(db)


def backfill_artifact_content_ids(db: DatabaseManager) -> None:
    """
    Best-effort backfill of `artifact.content_id` from existing hash+driver pairs.

    This is an explicit maintenance action rather than part of normal DB startup.
    Rows without a hash are left with NULL content_id.

    This is intentionally compatibility-oriented code: it uses ``artifact.hash``
    as the source of truth only to populate the newer shared content identity.
    """
    if not db._table_has_column(table_name="artifact", column_name="content_id"):
        return

    try:
        with db.session_scope() as session:
            artifacts = session.exec(
                select(Artifact).where(col(Artifact.hash).is_not(None))
            ).all()
            changed = False
            content_cache: dict[tuple[str, str], uuid.UUID] = {}
            for artifact in artifacts:
                if artifact.content_id is not None:
                    continue
                if not artifact.hash or not artifact.driver:
                    continue
                key = (artifact.hash, artifact.driver)
                content_id = content_cache.get(key)
                if content_id is None:
                    statement = (
                        select(ArtifactContent)
                        .where(ArtifactContent.content_hash == artifact.hash)
                        .where(ArtifactContent.driver == artifact.driver)
                    )
                    content_row = session.exec(statement.limit(1)).first()
                    if content_row is None:
                        content_row = ArtifactContent(
                            content_hash=artifact.hash,
                            driver=artifact.driver,
                            meta={},
                        )
                        session.add(content_row)
                        session.flush()
                    content_id = content_row.id
                    content_cache[key] = content_id
                artifact.content_id = content_id
                session.add(artifact)
                changed = True
            if changed:
                session.commit()
    except Exception as exc:
        logging.warning("Failed to backfill artifact.content_id values: %s", exc)


def backfill_run_stage_phase_from_meta(db: DatabaseManager) -> None:
    """
    Best-effort backfill of `run.stage` and `run.phase` from legacy metadata.

    Existing rows are only updated when the canonical columns are null and the
    mirrored JSON metadata contains a string value.
    """
    if not db._table_has_column(table_name="run", column_name="stage"):
        return
    if not db._table_has_column(table_name="run", column_name="phase"):
        return

    try:
        with db.session_scope() as session:
            runs = session.exec(
                select(Run).where(
                    (col(Run.stage).is_(None)) | (col(Run.phase).is_(None))
                )
            ).all()
            changed = False
            for run in runs:
                meta = run.meta if isinstance(run.meta, dict) else {}
                stage = resolve_canonical_run_meta_field(run, "stage")
                if run.stage != stage:
                    run.stage = stage
                    changed = True
                phase = resolve_canonical_run_meta_field(run, "phase")
                if run.phase != phase:
                    run.phase = phase
                    changed = True
                if run.stage is not None:
                    meta["stage"] = run.stage
                if run.phase is not None:
                    meta["phase"] = run.phase
                if meta is not run.meta:
                    run.meta = meta
                    changed = True
                session.add(run)
            if changed:
                session.commit()
    except Exception as exc:
        logging.warning("Failed to backfill run.stage/run.phase values: %s", exc)


def _ensure_artifact_content_id_column(db: DatabaseManager) -> None:
    """Ensure `artifact.content_id` exists for additive content identity support."""
    if db._table_has_column(table_name="artifact", column_name="content_id"):
        return
    try:
        with db.engine.begin() as conn:
            conn.exec_driver_sql("ALTER TABLE artifact ADD COLUMN content_id CHAR(36)")
    except Exception as exc:
        logging.warning("Failed to add artifact.content_id column: %s", exc)


def _ensure_artifact_content_id_index(db: DatabaseManager) -> None:
    """Ensure upgraded databases also index `artifact.content_id`."""
    if not db._table_has_column(table_name="artifact", column_name="content_id"):
        return
    if db._table_has_index_on_column(table_name="artifact", column_name="content_id"):
        return
    try:
        with db.engine.begin() as conn:
            conn.exec_driver_sql(
                "CREATE INDEX idx_artifact_content_id ON artifact(content_id)"
            )
    except Exception as exc:
        logging.warning("Failed to create artifact.content_id index: %s", exc)


def _ensure_artifact_parent_artifact_id_column(db: DatabaseManager) -> None:
    """Ensure ``artifact.parent_artifact_id`` exists for container child relations."""
    if db._table_has_column(table_name="artifact", column_name="parent_artifact_id"):
        return
    try:
        with db.engine.begin() as conn:
            conn.exec_driver_sql(
                "ALTER TABLE artifact ADD COLUMN parent_artifact_id CHAR(36)"
            )
    except Exception as exc:
        logging.warning("Failed to add artifact.parent_artifact_id column: %s", exc)


def _ensure_artifact_parent_artifact_id_index(db: DatabaseManager) -> None:
    """Ensure upgraded databases also index ``artifact.parent_artifact_id``."""
    if not db._table_has_column(
        table_name="artifact", column_name="parent_artifact_id"
    ):
        return
    if db._table_has_index_on_column(
        table_name="artifact", column_name="parent_artifact_id"
    ):
        return
    try:
        with db.engine.begin() as conn:
            conn.exec_driver_sql(
                "CREATE INDEX idx_artifact_parent_artifact_id "
                "ON artifact(parent_artifact_id)"
            )
    except Exception as exc:
        logging.warning("Failed to create artifact.parent_artifact_id index: %s", exc)


def _ensure_run_stage_phase_columns(db: DatabaseManager) -> None:
    """Ensure `run.stage` and `run.phase` exist for additive workflow support."""
    if not db._table_has_column(table_name="run", column_name="stage"):
        try:
            with db.engine.begin() as conn:
                conn.exec_driver_sql("ALTER TABLE run ADD COLUMN stage VARCHAR")
        except Exception as exc:
            logging.warning("Failed to add run.stage column: %s", exc)
    if not db._table_has_column(table_name="run", column_name="phase"):
        try:
            with db.engine.begin() as conn:
                conn.exec_driver_sql("ALTER TABLE run ADD COLUMN phase VARCHAR")
        except Exception as exc:
            logging.warning("Failed to add run.phase column: %s", exc)


def _ensure_run_stage_phase_indexes(db: DatabaseManager) -> None:
    """Ensure upgraded databases also index `run.stage` and `run.phase`."""
    if db._table_has_column(
        table_name="run", column_name="stage"
    ) and not db._table_has_index_on_column(table_name="run", column_name="stage"):
        try:
            with db.engine.begin() as conn:
                conn.exec_driver_sql("CREATE INDEX idx_run_stage ON run(stage)")
        except Exception as exc:
            logging.warning("Failed to create run.stage index: %s", exc)
    if db._table_has_column(
        table_name="run", column_name="phase"
    ) and not db._table_has_index_on_column(table_name="run", column_name="phase"):
        try:
            with db.engine.begin() as conn:
                conn.exec_driver_sql("CREATE INDEX idx_run_phase ON run(phase)")
        except Exception as exc:
            logging.warning("Failed to create run.phase index: %s", exc)
