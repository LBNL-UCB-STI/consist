-- Consist v2 Provenance Database Schema (`provenance.duckdb`)

-- 1. Database Identity & Schema Version Gate
CREATE TABLE IF NOT EXISTS consist_meta (
    key   VARCHAR PRIMARY KEY,
    value VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS consist_schema (
    version         INTEGER PRIMARY KEY,
    applied_at      TIMESTAMPTZ NOT NULL,
    consist_version VARCHAR NOT NULL
);

-- 2. Run Table
CREATE TABLE IF NOT EXISTS run (
    id               VARCHAR PRIMARY KEY,
    model_name       VARCHAR NOT NULL,
    description      VARCHAR,
    year             INTEGER,
    iteration        INTEGER,
    stage            VARCHAR,
    phase            VARCHAR,
    parent_run_id    VARCHAR,
    status           VARCHAR NOT NULL,
    code_hash        VARCHAR,
    git_hash         VARCHAR,
    config_hash      VARCHAR,
    input_hash       VARCHAR,
    signature        VARCHAR,
    signature_scheme VARCHAR,
    cache_status     VARCHAR NOT NULL,
    tags             JSON,
    meta             JSON NOT NULL,
    facet_kv         JSON,
    started_at       TIMESTAMPTZ,
    ended_at         TIMESTAMPTZ,
    created_at       TIMESTAMPTZ NOT NULL,
    updated_at       TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_run_signature ON run(signature);
CREATE INDEX IF NOT EXISTS idx_run_cache_lookup ON run(model_name, signature, cache_status, status);

-- 3. Artifact Handle Table
CREATE TABLE IF NOT EXISTS artifact (
    id                 UUID PRIMARY KEY,
    key                VARCHAR NOT NULL,
    driver             VARCHAR NOT NULL,
    anchor             VARCHAR NOT NULL,
    run_id             VARCHAR,
    parent_artifact_id UUID,
    schema_id          VARCHAR,
    meta               JSON NOT NULL,
    facet_kv           JSON,
    created_at         TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_artifact_anchor ON artifact(anchor);
CREATE INDEX IF NOT EXISTS idx_artifact_run_id ON artifact(run_id);
CREATE INDEX IF NOT EXISTS idx_artifact_parent ON artifact(parent_artifact_id);

-- 4. Content Hub
CREATE TABLE IF NOT EXISTS content (
    anchor     VARCHAR PRIMARY KEY,
    repr_kind  VARCHAR NOT NULL,
    schema_id  VARCHAR,
    created_at TIMESTAMPTZ NOT NULL
);

-- 5. Digest Claims & Conflict Audit
-- basis: 'computed' | 'implied' | 'declared' | 'trusted_foreign'
CREATE TABLE IF NOT EXISTS content_digest (
    anchor      VARCHAR NOT NULL,
    scheme      VARCHAR NOT NULL,
    value       VARCHAR NOT NULL,
    basis       VARCHAR NOT NULL,
    computed_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (anchor, scheme)
);
CREATE INDEX IF NOT EXISTS idx_content_digest_lookup ON content_digest(scheme, value);

CREATE TABLE IF NOT EXISTS content_digest_conflict (
    anchor         VARCHAR NOT NULL,
    scheme         VARCHAR NOT NULL,
    kept_value     VARCHAR NOT NULL,
    rejected_value VARCHAR NOT NULL,
    source         VARCHAR NOT NULL,
    seen_at        TIMESTAMPTZ NOT NULL
);

-- 6. Structural Manifests & Filter Policies
CREATE TABLE IF NOT EXISTS content_manifest (
    anchor        VARCHAR PRIMARY KEY,
    format        VARCHAR NOT NULL,
    policy_sha256 VARCHAR,
    entry_count   INTEGER NOT NULL,
    total_bytes   BIGINT NOT NULL,
    body          BLOB NOT NULL
);

CREATE TABLE IF NOT EXISTS digest_policy (
    policy_sha256 VARCHAR PRIMARY KEY,
    policy_json   JSON NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL
);

-- 7. Cross-Representation Derivations, Historical Tape Promotions & Migration Audit
CREATE TABLE IF NOT EXISTS content_derivation (
    src_anchor VARCHAR NOT NULL,
    dst_anchor VARCHAR NOT NULL,
    kind       VARCHAR NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (src_anchor, dst_anchor, kind)
);

CREATE TABLE IF NOT EXISTS anchor_promotion (
    historical_anchor VARCHAR PRIMARY KEY,
    verified_anchor   VARCHAR NOT NULL,
    basis             VARCHAR NOT NULL,
    asserted_by       VARCHAR,
    evidence          JSON,
    promoted_at       TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS artifact_migration_audit (
    artifact_id UUID PRIMARY KEY,
    v1_row_json JSON NOT NULL,
    outcome     VARCHAR NOT NULL,
    basis       VARCHAR NOT NULL
);

-- 8. Role Spoke: Canonical Input/Output Binding Occurrences & Compatibility Views
CREATE TABLE IF NOT EXISTS run_binding (
    run_id           VARCHAR NOT NULL,
    direction        VARCHAR NOT NULL,
    occurrence_index INTEGER NOT NULL,
    slot             VARCHAR NOT NULL,
    ordinal          INTEGER NOT NULL DEFAULT 0,
    artifact_id      UUID    NOT NULL,
    role             VARCHAR,
    anchor           VARCHAR NOT NULL,
    contract_scheme  VARCHAR,
    sig_scheme       VARCHAR,
    sig_hex          VARCHAR,
    PRIMARY KEY (run_id, direction, slot, ordinal),
    UNIQUE (run_id, direction, occurrence_index)
);
CREATE INDEX IF NOT EXISTS idx_run_binding_artifact ON run_binding(artifact_id);

CREATE OR REPLACE VIEW run_artifact_link AS
SELECT
    run_id,
    artifact_id,
    CASE WHEN bool_or(direction IN ('input', 'depends_on')) THEN 'input' ELSE 'output' END AS direction,
    FALSE AS is_implicit
FROM run_binding
GROUP BY run_id, artifact_id;

CREATE OR REPLACE VIEW run_artifact_role AS
SELECT
    run_id,
    artifact_id,
    bool_or(direction IN ('input', 'depends_on')) AS is_input,
    bool_or(direction = 'output')                 AS is_output
FROM run_binding
GROUP BY run_id, artifact_id;

-- 9. Location Spoke: Place-Superseded Observations + Monotonic Status Events
CREATE TABLE IF NOT EXISTS artifact_location (
    observation_id    UUID PRIMARY KEY,
    anchor            VARCHAR NOT NULL,
    uri               VARCHAR NOT NULL,
    member            VARCHAR,
    access_driver     VARCHAR NOT NULL,
    witness_json      JSON,
    basis             VARCHAR NOT NULL,
    site              VARCHAR NOT NULL,
    scope_key         VARCHAR,
    mount_instance_id VARCHAR,
    observed_at_ns    BIGINT NOT NULL,
    writer            VARCHAR NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_artifact_location_anchor_site ON artifact_location(anchor, site);
CREATE INDEX IF NOT EXISTS idx_artifact_location_site_uri ON artifact_location(site, uri);

CREATE TABLE IF NOT EXISTS location_event (
    event_id       UUID PRIMARY KEY,
    observation_id UUID NOT NULL,
    status         VARCHAR NOT NULL,
    status_rank    SMALLINT NOT NULL,
    transfer_ref   VARCHAR,
    at_ns          BIGINT NOT NULL,
    writer         VARCHAR NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_location_event_obs_id ON location_event(observation_id);

CREATE OR REPLACE VIEW artifact_location_current AS
WITH latest_per_place AS (
    SELECT * FROM artifact_location
    QUALIFY row_number() OVER (
        PARTITION BY site, scope_key, mount_instance_id, uri, member
        ORDER BY observed_at_ns DESC, observation_id DESC
    ) = 1
), latest_event AS (
    SELECT observation_id, status, transfer_ref FROM location_event
    QUALIFY row_number() OVER (
        PARTITION BY observation_id
        ORDER BY status_rank DESC, at_ns DESC, event_id DESC
    ) = 1
)
SELECT m.*, coalesce(e.status, 'active') AS status, e.transfer_ref
FROM latest_per_place m
LEFT JOIN latest_event e USING (observation_id)
WHERE m.member IS NULL
   OR NOT EXISTS (
       SELECT 1 FROM latest_per_place c
       WHERE c.member IS NULL
         AND c.site = m.site
         AND c.scope_key IS NOT DISTINCT FROM m.scope_key
         AND c.mount_instance_id IS NOT DISTINCT FROM m.mount_instance_id
         AND c.uri = m.uri
         AND c.observed_at_ns > m.observed_at_ns
         AND (
             c.witness_json IS NULL
             OR m.witness_json IS NULL
             OR (m.witness_json->>'kind') IS DISTINCT FROM 'member_of'
             OR c.witness_json IS DISTINCT FROM (m.witness_json->'parent')
         )
   );

CREATE OR REPLACE MACRO locate_current(anchors) AS TABLE
WITH req_anchors AS (
    SELECT DISTINCT UNNEST(anchors) AS anchor
), matching_places AS (
    SELECT DISTINCT
        l.site,
        l.scope_key,
        l.mount_instance_id,
        l.uri,
        l.member
    FROM artifact_location l
    JOIN req_anchors req ON l.anchor = req.anchor
), candidate_places AS (
    SELECT site, scope_key, mount_instance_id, uri, member FROM matching_places
    UNION
    SELECT site, scope_key, mount_instance_id, uri, NULL AS member
    FROM matching_places
    WHERE member IS NOT NULL
), latest_per_place AS (
    SELECT l.*
    FROM artifact_location l
    JOIN candidate_places p
      ON l.site = p.site
     AND l.scope_key IS NOT DISTINCT FROM p.scope_key
     AND l.mount_instance_id IS NOT DISTINCT FROM p.mount_instance_id
     AND l.uri = p.uri
     AND l.member IS NOT DISTINCT FROM p.member
    QUALIFY row_number() OVER (
        PARTITION BY l.site, l.scope_key, l.mount_instance_id, l.uri, l.member
        ORDER BY l.observed_at_ns DESC, l.observation_id DESC
    ) = 1
), ev AS (
    SELECT e.observation_id, e.status, e.transfer_ref
    FROM location_event e
    JOIN latest_per_place m ON e.observation_id = m.observation_id
    QUALIFY row_number() OVER (
        PARTITION BY e.observation_id
        ORDER BY e.status_rank DESC, e.at_ns DESC, e.event_id DESC
    ) = 1
)
SELECT m.*, coalesce(ev.status, 'active') AS status, ev.transfer_ref
FROM latest_per_place m
JOIN req_anchors req ON m.anchor = req.anchor
LEFT JOIN ev USING (observation_id)
WHERE m.member IS NULL
   OR NOT EXISTS (
       SELECT 1 FROM latest_per_place c
       WHERE c.member IS NULL
         AND c.site = m.site
         AND c.scope_key IS NOT DISTINCT FROM m.scope_key
         AND c.mount_instance_id IS NOT DISTINCT FROM m.mount_instance_id
         AND c.uri = m.uri
         AND c.observed_at_ns > m.observed_at_ns
         AND (
             c.witness_json IS NULL
             OR m.witness_json IS NULL
             OR (m.witness_json->>'kind') IS DISTINCT FROM 'member_of'
             OR c.witness_json IS DISTINCT FROM (m.witness_json->'parent')
         )
   );

-- 10. Preserved Operational Tables (Zero Physical Foreign Keys)
CREATE TABLE IF NOT EXISTS run_coupler_snapshot (
    run_id          VARCHAR NOT NULL,
    phase           VARCHAR NOT NULL,
    role_key        VARCHAR NOT NULL,
    scenario_run_id VARCHAR NOT NULL,
    step_ordinal    INTEGER NOT NULL DEFAULT 0,
    artifact_id     UUID    NOT NULL,
    producer_run_id VARCHAR,
    updated_at      TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (run_id, phase, role_key)
);
CREATE INDEX IF NOT EXISTS ix_run_coupler_snapshot_scenario_ordinal
    ON run_coupler_snapshot(scenario_run_id, step_ordinal, phase);
CREATE INDEX IF NOT EXISTS ix_run_coupler_snapshot_scenario_role
    ON run_coupler_snapshot(scenario_run_id, role_key);
CREATE INDEX IF NOT EXISTS idx_run_coupler_snapshot_artifact
    ON run_coupler_snapshot(artifact_id);

CREATE TABLE IF NOT EXISTS run_binding_invocation (
    id                  UUID PRIMARY KEY,
    requested_run_id    VARCHAR NOT NULL,
    execution_run_id    VARCHAR NOT NULL,
    cache_source_run_id VARCHAR,
    cache_outcome       VARCHAR NOT NULL,
    binding_json        VARCHAR NOT NULL,
    created_at          TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_run_binding_inv_requested
    ON run_binding_invocation(requested_run_id);
CREATE INDEX IF NOT EXISTS idx_run_binding_inv_execution
    ON run_binding_invocation(execution_run_id);

CREATE TABLE IF NOT EXISTS artifact_schema (
    id              VARCHAR PRIMARY KEY,
    profile_version INTEGER NOT NULL DEFAULT 1,
    summary_json    JSON NOT NULL,
    profile_json    JSON,
    created_at      TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS artifact_schema_field (
    schema_id        VARCHAR NOT NULL,
    ordinal_position INTEGER,
    name             VARCHAR NOT NULL,
    logical_type     VARCHAR NOT NULL,
    nullable         BOOLEAN NOT NULL DEFAULT TRUE,
    stats_json       JSON,
    is_enum          BOOLEAN NOT NULL DEFAULT FALSE,
    enum_values_json JSON,
    PRIMARY KEY (schema_id, name)
);
CREATE INDEX IF NOT EXISTS idx_artifact_schema_field_name
    ON artifact_schema_field(name);

CREATE TABLE IF NOT EXISTS artifact_schema_observation (
    id          UUID PRIMARY KEY,
    artifact_id UUID NOT NULL,
    schema_id   VARCHAR NOT NULL,
    run_id      VARCHAR,
    source      VARCHAR NOT NULL,
    sample_rows INTEGER,
    observed_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_artifact_schema_obs_artifact
    ON artifact_schema_observation(artifact_id);
CREATE INDEX IF NOT EXISTS idx_artifact_schema_obs_schema
    ON artifact_schema_observation(schema_id);

CREATE TABLE IF NOT EXISTS artifact_schema_relation (
    schema_id         VARCHAR NOT NULL,
    from_field        VARCHAR NOT NULL,
    to_table          VARCHAR NOT NULL,
    to_field          VARCHAR NOT NULL,
    relationship_type VARCHAR NOT NULL DEFAULT 'foreign_key',
    cardinality       VARCHAR,
    PRIMARY KEY (schema_id, from_field)
);

-- 11. Single-Level JSON Facet Index Views
CREATE OR REPLACE VIEW run_config_kv AS
SELECT
    r.id AS run_id,
    COALESCE(
        r.meta->>'config_facet_id',
        r.meta->>'facet_id',
        sha256(CAST(r.facet_kv AS VARCHAR))
    ) AS facet_id,
    COALESCE(r.meta->>'facet_namespace', r.meta->>'config_facet_namespace', r.model_name) AS namespace,
    kv.key AS key,
    kv.value->>'t' AS value_type,
    kv.value->>'s' AS value_str,
    CAST(kv.value->>'n' AS DOUBLE) AS value_num,
    CAST(kv.value->>'b' AS BOOLEAN) AS value_bool,
    kv.value->'j' AS value_json,
    r.created_at AS created_at
FROM run r, LATERAL json_each(r.facet_kv) kv
WHERE r.facet_kv IS NOT NULL;

CREATE OR REPLACE VIEW artifact_kv AS
SELECT
    a.id AS artifact_id,
    COALESCE(
        a.meta->>'artifact_facet_id',
        a.meta->>'facet_id',
        sha256(CAST(a.facet_kv AS VARCHAR))
    ) AS facet_id,
    COALESCE(a.meta->>'facet_namespace', a.meta->>'artifact_facet_namespace') AS namespace,
    kv.key AS key_path,
    kv.value->>'t' AS value_type,
    kv.value->>'s' AS value_str,
    CAST(kv.value->>'n' AS DOUBLE) AS value_num,
    CAST(kv.value->>'b' AS BOOLEAN) AS value_bool,
    a.created_at AS created_at
FROM artifact a, LATERAL json_each(a.facet_kv) kv
WHERE a.facet_kv IS NOT NULL;
