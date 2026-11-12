-- Consist v0.5 Core Provenance Schema (Schema Version 2)
-- Executed idempotently by DatabaseManager._init_schema_locked.

CREATE TABLE IF NOT EXISTS consist_schema (
    version INTEGER PRIMARY KEY,
    db_uid VARCHAR NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

-- Runs & execution lineage
CREATE TABLE IF NOT EXISTS run (
    id VARCHAR PRIMARY KEY,
    parent_run_id VARCHAR,
    status VARCHAR NOT NULL DEFAULT 'running',
    model_name VARCHAR NOT NULL,
    description VARCHAR,
    year INTEGER,
    iteration INTEGER,
    stage VARCHAR,
    phase VARCHAR,
    tags JSON NOT NULL DEFAULT '[]',
    config_hash VARCHAR,
    git_hash VARCHAR,
    input_hash VARCHAR,
    signature VARCHAR,
    cacheable BOOLEAN NOT NULL DEFAULT TRUE,
    facet_id VARCHAR,
    meta JSON NOT NULL DEFAULT '{}',
    started_at TIMESTAMPTZ NOT NULL,
    ended_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_run_parent_run_id ON run (parent_run_id);
CREATE INDEX IF NOT EXISTS ix_run_status ON run (status);
CREATE INDEX IF NOT EXISTS ix_run_model_name ON run (model_name);
CREATE INDEX IF NOT EXISTS ix_run_config_hash ON run (config_hash);
CREATE INDEX IF NOT EXISTS ix_run_input_hash ON run (input_hash);
CREATE INDEX IF NOT EXISTS ix_run_signature ON run (signature);

-- Artifacts & verified physical location ledger
CREATE TABLE IF NOT EXISTS artifact (
    id UUID PRIMARY KEY,
    key VARCHAR NOT NULL,
    uri VARCHAR NOT NULL,
    driver VARCHAR NOT NULL,
    digest VARCHAR,
    member VARCHAR,
    run_id VARCHAR,
    parent_artifact_id UUID,
    facet_id VARCHAR,
    meta JSON NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_artifact_key ON artifact (key);
CREATE INDEX IF NOT EXISTS ix_artifact_uri ON artifact (uri);
CREATE INDEX IF NOT EXISTS ix_artifact_digest ON artifact (digest);
CREATE INDEX IF NOT EXISTS ix_artifact_run_id ON artifact (run_id);
CREATE INDEX IF NOT EXISTS ix_artifact_parent_artifact_id ON artifact (parent_artifact_id);

CREATE TABLE IF NOT EXISTS artifact_location (
    location_id UUID PRIMARY KEY,
    digest VARCHAR,
    artifact_id UUID,
    site VARCHAR NOT NULL,
    mount_name VARCHAR NOT NULL,
    mount_instance_id VARCHAR NOT NULL,
    uri VARCHAR NOT NULL,
    member VARCHAR NOT NULL DEFAULT '',
    status VARCHAR NOT NULL DEFAULT 'active',
    witness_json JSON,
    observed_at_ns BIGINT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_artifact_location_digest_status ON artifact_location (digest, status);
CREATE INDEX IF NOT EXISTS ix_artifact_location_artifact_status ON artifact_location (artifact_id, status);
CREATE INDEX IF NOT EXISTS ix_artifact_location_coord_status ON artifact_location (site, mount_name, mount_instance_id, uri, member, status);

CREATE TABLE IF NOT EXISTS run_artifact_link (
    run_id VARCHAR NOT NULL,
    artifact_id UUID NOT NULL,
    direction VARCHAR NOT NULL,
    role VARCHAR NOT NULL DEFAULT '',
    is_implicit BOOLEAN NOT NULL DEFAULT FALSE,
    PRIMARY KEY (run_id, artifact_id, direction, role)
);
CREATE INDEX IF NOT EXISTS ix_run_artifact_link_run_id ON run_artifact_link (run_id);
CREATE INDEX IF NOT EXISTS ix_run_artifact_link_artifact_id ON run_artifact_link (artifact_id);

CREATE TABLE IF NOT EXISTS run_binding_invocation (
    id UUID PRIMARY KEY,
    requested_run_id VARCHAR NOT NULL,
    execution_run_id VARCHAR NOT NULL,
    cache_source_run_id VARCHAR,
    cache_outcome VARCHAR NOT NULL,
    binding_json VARCHAR NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_run_binding_invocation_requested ON run_binding_invocation (requested_run_id);

-- Facets & flattened key-value indexes
CREATE TABLE IF NOT EXISTS config_facet (
    id VARCHAR PRIMARY KEY,
    namespace VARCHAR NOT NULL,
    schema_name VARCHAR NOT NULL,
    schema_version VARCHAR,
    "json" JSON NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_config_facet_namespace ON config_facet (namespace);

CREATE TABLE IF NOT EXISTS run_config_kv (
    run_id VARCHAR NOT NULL,
    facet_id VARCHAR NOT NULL,
    namespace VARCHAR NOT NULL,
    key VARCHAR NOT NULL,
    value_type VARCHAR NOT NULL,
    value_str VARCHAR,
    value_num DOUBLE,
    value_bool BOOLEAN,
    value_json JSON,
    created_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (run_id, facet_id, namespace, key)
);
CREATE INDEX IF NOT EXISTS ix_run_config_kv_run_id ON run_config_kv (run_id);

CREATE TABLE IF NOT EXISTS artifact_facet (
    id VARCHAR PRIMARY KEY,
    namespace VARCHAR,
    schema_name VARCHAR,
    schema_version VARCHAR,
    "json" JSON NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS artifact_kv (
    artifact_id UUID NOT NULL,
    facet_id VARCHAR NOT NULL,
    key_path VARCHAR NOT NULL,
    namespace VARCHAR,
    value_type VARCHAR NOT NULL,
    value_str VARCHAR,
    value_num DOUBLE,
    value_bool BOOLEAN,
    value_json JSON,
    created_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (artifact_id, facet_id, key_path)
);
CREATE INDEX IF NOT EXISTS ix_artifact_kv_artifact_id ON artifact_kv (artifact_id);
CREATE INDEX IF NOT EXISTS ix_artifact_kv_key_path ON artifact_kv (key_path);

-- Scenario coupler state snapshots
CREATE TABLE IF NOT EXISTS run_coupler_snapshot (
    run_id VARCHAR NOT NULL,
    phase VARCHAR NOT NULL,
    role_key VARCHAR NOT NULL,
    scenario_run_id VARCHAR NOT NULL,
    step_ordinal INTEGER NOT NULL DEFAULT 0,
    artifact_id UUID NOT NULL,
    producer_run_id VARCHAR,
    updated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (run_id, phase, role_key)
);
CREATE INDEX IF NOT EXISTS ix_run_coupler_snapshot_scenario_ordinal ON run_coupler_snapshot (scenario_run_id, step_ordinal, phase);
CREATE INDEX IF NOT EXISTS ix_run_coupler_snapshot_scenario_role ON run_coupler_snapshot (scenario_run_id, role_key);

-- Tabular schema profiling & foreign-key relation links
CREATE TABLE IF NOT EXISTS artifact_schema (
    id VARCHAR PRIMARY KEY,
    profile_version INTEGER NOT NULL DEFAULT 1,
    summary_json JSON NOT NULL DEFAULT '{}',
    profile_json JSON,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS artifact_schema_field (
    schema_id VARCHAR NOT NULL,
    ordinal_position INTEGER,
    name VARCHAR NOT NULL,
    logical_type VARCHAR NOT NULL,
    nullable BOOLEAN NOT NULL DEFAULT TRUE,
    stats_json JSON,
    is_enum BOOLEAN NOT NULL DEFAULT FALSE,
    enum_values_json JSON,
    PRIMARY KEY (schema_id, name)
);

CREATE TABLE IF NOT EXISTS artifact_schema_observation (
    id UUID PRIMARY KEY,
    artifact_id UUID NOT NULL,
    schema_id VARCHAR NOT NULL,
    run_id VARCHAR,
    source VARCHAR NOT NULL,
    sample_rows INTEGER,
    observed_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_artifact_schema_obs_artifact_id ON artifact_schema_observation (artifact_id);
CREATE INDEX IF NOT EXISTS ix_artifact_schema_obs_schema_id ON artifact_schema_observation (schema_id);

CREATE TABLE IF NOT EXISTS artifact_schema_relation (
    schema_id VARCHAR NOT NULL,
    from_field VARCHAR NOT NULL,
    to_table VARCHAR NOT NULL,
    to_field VARCHAR NOT NULL,
    relationship_type VARCHAR NOT NULL DEFAULT 'foreign_key',
    cardinality VARCHAR,
    PRIMARY KEY (schema_id, from_field)
);

CREATE OR REPLACE VIEW consist_schema_links AS
SELECT
    rel.schema_id,
    src.summary_json->>'$.table_name' AS from_table,
    rel.from_field,
    rel.to_table,
    rel.to_field,
    rel.relationship_type,
    rel.cardinality
FROM artifact_schema_relation AS rel
JOIN artifact_schema AS src ON rel.schema_id = src.id;
