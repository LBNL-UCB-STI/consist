from __future__ import annotations

from dataclasses import replace
import logging
import time
from typing import TYPE_CHECKING, Any, Iterable, Optional, Union
import uuid
import warnings

from pydantic import BaseModel

from consist.artifact import Artifact
from consist.core.stores import get_hot_data_db_path, get_hot_data_ledger
from consist.digest import Digest, Kind, compute_ctd1_digest
from consist.location import (
    DbTableChangeToken,
    Location,
    Observation,
    format_hot_db_member,
)
from consist.models.run import Run

if TYPE_CHECKING:
    from consist.core.tracker import Tracker
    from consist.store.ledger import StoreLedger

_DLT_EXCLUDED_SYSTEM_COLS: frozenset[str] = frozenset(
    {
        "consist_artifact_id",
        "consist_run_id",
        "consist_year",
        "consist_iteration",
    }
)


def _quote_ident(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _is_nested_duckdb_type(type_str: str) -> bool:
    upper = type_str.strip().upper()
    return (
        upper.startswith(("STRUCT", "MAP", "LIST", "UNION"))
        or upper.endswith("[]")
        or upper == "JSON"
    )


def _is_ieee_float_duckdb_type(type_str: str) -> bool:
    upper = type_str.strip().upper()
    return upper in ("FLOAT", "REAL", "DOUBLE", "FLOAT4", "FLOAT8")


def _compute_ingested_table_digest_and_witness(
    ledger: StoreLedger,
    *,
    table_name: str,
    artifact_id: uuid.UUID,
) -> tuple[Digest | None, DbTableChangeToken]:
    q_schema = _quote_ident("global_tables")
    q_table = _quote_ident(table_name)
    load_id = str(artifact_id)
    row_count = 0

    with ledger.exec_cursor() as cur:
        col_rows = cur.execute(
            "SELECT column_name, data_type FROM duckdb_columns() "
            "WHERE database_name = current_database() "
            "AND schema_name = 'global_tables' AND table_name = ? "
            "ORDER BY column_index ASC",
            [table_name],
        ).fetchall()
        if not col_rows:
            return (
                None,
                DbTableChangeToken(load_id=load_id, row_count=0),
            )

        col_types = {str(r[0]): str(r[1]) for r in col_rows}
        where_sql = ""
        params: list[Any] = []
        if "consist_artifact_id" in col_types:
            where_sql = " WHERE CAST(consist_artifact_id AS VARCHAR) = ?"
            params = [str(artifact_id)]

        cnt_row = cur.execute(
            f"SELECT COUNT(*) FROM {q_schema}.{q_table}{where_sql}",
            params,
        ).fetchone()
        row_count = (
            int(cnt_row[0]) if cnt_row is not None and cnt_row[0] is not None else 0
        )

        if "_dlt_load_id" in col_types:
            ld_row = cur.execute(
                f'SELECT CAST(MAX("_dlt_load_id") AS VARCHAR) FROM {q_schema}.{q_table}{where_sql}',
                params,
            ).fetchone()
            if ld_row is not None and ld_row[0] is not None:
                load_id = str(ld_row[0])

        witness = DbTableChangeToken(load_id=load_id, row_count=row_count)

        user_cols = [
            col
            for col in col_types
            if col not in _DLT_EXCLUDED_SYSTEM_COLS and not col.startswith("_dlt_")
        ]
        if not user_cols or any(
            _is_nested_duckdb_type(col_types[c]) for c in user_cols
        ):
            return (None, witness)

        cols_sql = ", ".join(_quote_ident(c) for c in user_cols)
        order_parts = [
            (
                f"CASE WHEN {_quote_ident(c)} IS NULL THEN 0 "
                f"WHEN isnan({_quote_ident(c)}) THEN 1 ELSE 2 END ASC, {_quote_ident(c)} ASC"
                if _is_ieee_float_duckdb_type(col_types[c])
                else f"{_quote_ident(c)} ASC NULLS FIRST"
            )
            for c in user_cols
        ]
        order_sql = ", ".join(order_parts)

        try:
            cur.execute(
                f"SELECT {cols_sql} FROM {q_schema}.{q_table}{where_sql} ORDER BY {order_sql}",
                params,
            )
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", DeprecationWarning)
                batch_reader = cur.fetch_record_batch(rows_per_batch=65_536)
            digest = compute_ctd1_digest(
                batch_reader,
                columns=user_cols,
                presorted=True,
            )
            return (digest, witness)
        except Exception:
            return (None, witness)


def resolve_ingest_run(
    *,
    tracker: Tracker,
    artifact: Artifact,
    run: Optional[Run],
) -> Run:
    """Resolve the run context to attribute an ingestion to."""
    target_run = run

    if not target_run and not tracker.current_consist:
        if artifact.run_id:
            target_run = tracker.get_run(artifact.run_id)
            if target_run:
                logging.info(
                    "[Consist] Ingesting in Analysis Mode. Attributing to Run: %s",
                    target_run.id,
                )

    if not target_run and tracker.current_consist:
        target_run = tracker.current_consist.run

    if not target_run:
        raise RuntimeError("Cannot ingest: Could not determine associated Run context.")

    return target_run


def resolve_ingest_data(
    *,
    tracker: Tracker,
    artifact: Artifact,
    data: Optional[Union[Iterable[dict[str, Any]], Any]],
) -> Any:
    """Resolve the data payload passed to ingestion without touching deprecated properties."""
    if data is not None:
        return data
    located = artifact.locate(
        ledger=tracker.ledger,
        site_context=tracker.site_context,
        mounts=tracker.mounts,
    )
    if located is not None:
        return str(located)
    primary = artifact.primary_location
    if primary is not None:
        return tracker.resolve_uri(primary.uri)
    raise ValueError(
        f"Cannot resolve ingestion source path for artifact {artifact.key!r} (id={artifact.id})"
    )


def _sync_ingested_artifact_into_tracker_state(
    tracker: Tracker,
    updated_artifact: Artifact,
) -> None:
    updated_artifact.bind_tracker(tracker)

    rec = tracker.current_consist
    if rec is not None:
        replaced_out = False
        new_outputs = []
        for item in rec.outputs:
            if item.id == updated_artifact.id:
                new_outputs.append(updated_artifact)
                replaced_out = True
            else:
                new_outputs.append(item)
        if replaced_out:
            rec.outputs = new_outputs

        replaced_in = False
        new_inputs = []
        for item in rec.inputs:
            if item.id == updated_artifact.id:
                new_inputs.append(updated_artifact)
                replaced_in = True
            else:
                new_inputs.append(item)
        if replaced_in:
            rec.inputs = new_inputs

        if replaced_out or replaced_in:
            tracker._flush_json()

    last_run = tracker.last_run
    if last_run is not None and last_run is not rec:
        last_changed = False
        new_last_outputs = []
        for item in last_run.outputs:
            if item.id == updated_artifact.id:
                new_last_outputs.append(updated_artifact)
                last_changed = True
            else:
                new_last_outputs.append(item)
        if last_changed:
            last_run.outputs = new_last_outputs

        last_in_changed = False
        new_last_inputs = []
        for item in last_run.inputs:
            if item.id == updated_artifact.id:
                new_last_inputs.append(updated_artifact)
                last_in_changed = True
            else:
                new_last_inputs.append(item)
        if last_in_changed:
            last_run.inputs = new_last_inputs

        if last_changed or last_in_changed:
            tracker.persistence.flush_record_json(last_run)

    seen_couplers: set[int] = set()
    tracker_dict = tracker.__dict__
    active_coupler = tracker_dict.get("_active_coupler")
    if active_coupler is not None:
        seen_couplers.add(id(active_coupler))
        active_coupler.replace_artifact_by_id(updated_artifact)
    for coupler in list(tracker_dict.get("_couplers") or ()):
        if id(coupler) not in seen_couplers:
            seen_couplers.add(id(coupler))
            coupler.replace_artifact_by_id(updated_artifact)


def ingest_artifact(
    *,
    tracker: Tracker,
    artifact: Artifact,
    data: Optional[Union[Iterable[dict[str, Any]], Any]],
    schema: Optional[type[BaseModel]],
    run: Optional[Run],
    profile_schema: bool,
) -> Artifact:
    """
    Ingest artifact data into ``global_tables`` via ``StoreLedger``, record its
    ``ctd1:sha256`` digest and ``db://global_tables/<table>`` observation, and
    return the updated frozen ``Artifact``.
    """
    hot_data_db_path = get_hot_data_db_path(tracker)
    ledger = get_hot_data_ledger(tracker)
    if hot_data_db_path is None or ledger is None:
        raise RuntimeError("Cannot ingest: db_path is not configured.")

    target_run = resolve_ingest_run(tracker=tracker, artifact=artifact, run=run)
    data_to_pass = resolve_ingest_data(tracker=tracker, artifact=artifact, data=data)

    from consist.integrations.dlt_loader import ingest_artifact as ingest_with_dlt

    _info, resource_name = ingest_with_dlt(
        artifact=artifact,
        run_context=target_run,
        db_path=hot_data_db_path,
        data_iterable=data_to_pass,
        schema_model=schema,
        lock_retries=tracker._dlt_lock_retries,
        lock_base_sleep_seconds=tracker._dlt_lock_base_sleep_seconds,
        lock_max_sleep_seconds=tracker._dlt_lock_max_sleep_seconds,
        ledger=ledger,
    )

    ctd1_digest, witness = _compute_ingested_table_digest_and_witness(
        ledger,
        table_name=resource_name,
        artifact_id=artifact.id,
    )

    db_uri = f"db://global_tables/{resource_name}"
    has_other_db_table = any(
        h.location.uri.startswith("db://global_tables/") and h.location.uri != db_uri
        for h in artifact.hints
    )
    existing_ctd1 = artifact.identity.get("ctd1:sha256")
    suppress_claim = bool(
        tracker.__dict__.get("_suppress_ingest_identity_claim", False)
    )
    can_claim_ctd1 = (
        ctd1_digest is not None
        and not suppress_claim
        and artifact.identity.anchor.kind is not Kind.HISTORICAL
        and artifact.is_tabular
        and not has_other_db_table
        and (existing_ctd1 is None or existing_ctd1.hex == ctd1_digest.hex)
    )

    updated_identity = artifact.identity
    if can_claim_ctd1 and ctd1_digest is not None:
        updated_identity = updated_identity.with_claim(ctd1_digest)

    site_name = (
        tracker.site_context.site if tracker.site_context is not None else "local"
    )
    db_obs = Observation(
        location=Location(
            uri=db_uri,
            member=format_hot_db_member(
                resource_name,
                consist_artifact_id=artifact.id,
            ),
        ),
        identity=updated_identity,
        access_driver="duckdb",
        witness=witness,
        basis="computed",
        site=site_name,
        observed_at_ns=time.time_ns(),
    )

    existing_non_db_hints = [
        h for h in artifact.hints if not h.location.uri.startswith("db://")
    ]
    existing_other_db_hints = [
        h
        for h in artifact.hints
        if h.location.uri.startswith("db://") and h.location.uri != db_uri
    ]
    kept_prefix = (
        (*existing_non_db_hints[:1], *existing_other_db_hints)[:3]
        if existing_other_db_hints and existing_non_db_hints
        else (*existing_non_db_hints, *existing_other_db_hints)[:3]
    )
    new_hints = tuple((*kept_prefix, db_obs))
    updated_artifact = replace(
        artifact,
        identity=updated_identity,
        origin=ledger.ref,
        hints=new_hints,
    )

    def _persist_ingestion_metadata() -> None:
        with ledger.transaction():
            ledger.record_identity(
                updated_identity,
                repr_kind="table" if not existing_non_db_hints else "file",
                schema_id=updated_artifact.schema_id,
                basis="computed" if updated_identity.verifiable else "declared",
                source=f"dlt:global_tables.{resource_name}",
            )
            if can_claim_ctd1 and ctd1_digest is not None:
                ledger.record_content_digest(
                    anchor=updated_identity.anchor,
                    digest=ctd1_digest,
                    basis="computed",
                    source=f"dlt:global_tables.{resource_name}",
                )
            ledger.record_observation(
                db_obs,
                repr_kind="table",
                schema_id=updated_artifact.schema_id,
            )
            if tracker.db is not None and tracker.db.ledger is ledger:
                tracker.db._persist_artifact_and_hints(updated_artifact)

    if tracker.db is not None:
        tracker.db.execute_with_retry(
            _persist_ingestion_metadata,
            operation_name="ingest_artifact_sync",
        )
        if tracker.db.ledger is not ledger:
            tracker.db.upsert_artifact(updated_artifact)
    else:
        _persist_ingestion_metadata()

    updated_artifact.bind_tracker(tracker)

    if profile_schema and tracker.db is not None:
        profiled = tracker.artifact_schemas.profile_ingested_table(
            artifact=updated_artifact,
            run=target_run,
            table_schema="global_tables",
            table_name=resource_name,
        )
        if isinstance(profiled, Artifact):
            updated_artifact = profiled

    _sync_ingested_artifact_into_tracker_state(tracker, updated_artifact)
    return updated_artifact
