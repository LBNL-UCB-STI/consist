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
    parent_run_id    VARCHAR,
    status           VARCHAR NOT NULL,
    code_hash        VARCHAR,
    config_hash      VARCHAR,
    input_hash       VARCHAR,
    signature        VARCHAR,
    signature_scheme VARCHAR,
    cache_status     VARCHAR NOT NULL,
    tags             JSON,
    meta             JSON NOT NULL,
    started_at       TIMESTAMPTZ,
    ended_at         TIMESTAMPTZ,
    created_at       TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_run_signature ON run(signature);

-- 3. Artifact Handle Table
CREATE TABLE IF NOT EXISTS artifact (
    id         UUID PRIMARY KEY,
    key        VARCHAR NOT NULL,
    driver     VARCHAR NOT NULL,
    anchor     VARCHAR NOT NULL,
    run_id     VARCHAR,
    schema_id  VARCHAR,
    meta       JSON NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_artifact_anchor ON artifact(anchor);
CREATE INDEX IF NOT EXISTS idx_artifact_run_id ON artifact(run_id);

-- 4. Content Hub
CREATE TABLE IF NOT EXISTS content (
    anchor     VARCHAR PRIMARY KEY,
    repr_kind  VARCHAR NOT NULL,
    schema_id  VARCHAR,
    created_at TIMESTAMPTZ NOT NULL
);

-- 5. Digest Claims & Conflict Audit
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
    CASE WHEN bool_or(direction IN ('input', 'depends_on')) THEN 'input' ELSE 'output' END AS direction
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

CREATE TABLE IF NOT EXISTS location_event (
    event_id       UUID PRIMARY KEY,
    observation_id UUID NOT NULL,
    status         VARCHAR NOT NULL,
    status_rank    SMALLINT NOT NULL,
    transfer_ref   VARCHAR,
    at_ns          BIGINT NOT NULL,
    writer         VARCHAR NOT NULL
);

CREATE OR REPLACE VIEW artifact_location_current AS
WITH latest_at_place AS (
    SELECT * FROM artifact_location
    QUALIFY row_number() OVER (
        PARTITION BY site, coalesce(scope_key, ''), coalesce(mount_instance_id, ''),
                     uri, coalesce(member, '')
        ORDER BY observed_at_ns DESC, observation_id DESC
    ) = 1
), latest_event AS (
    SELECT observation_id, status, transfer_ref FROM location_event
    QUALIFY row_number() OVER (
        PARTITION BY observation_id
        ORDER BY status_rank DESC, at_ns DESC, event_id DESC
    ) = 1
)
SELECT p.*, coalesce(e.status, 'active') AS status, e.transfer_ref
FROM latest_at_place p
LEFT JOIN latest_event e USING (observation_id);

CREATE OR REPLACE MACRO locate_current(anchors) AS TABLE
WITH places AS (
    SELECT DISTINCT
        site,
        coalesce(scope_key, '') AS sk,
        coalesce(mount_instance_id, '') AS mi,
        uri,
        coalesce(member, '') AS mb
    FROM artifact_location
    WHERE list_contains(anchors, anchor)
), cand AS (
    SELECT l.* FROM artifact_location l
    JOIN places p
      ON l.site = p.site
     AND coalesce(l.scope_key, '') = p.sk
     AND coalesce(l.mount_instance_id, '') = p.mi
     AND l.uri = p.uri
     AND coalesce(l.member, '') = p.mb
    QUALIFY row_number() OVER (
        PARTITION BY l.site, coalesce(l.scope_key, ''), coalesce(l.mount_instance_id, ''),
                     l.uri, coalesce(l.member, '')
        ORDER BY l.observed_at_ns DESC, l.observation_id DESC
    ) = 1
), ev AS (
    SELECT e.observation_id, e.status, e.transfer_ref
    FROM location_event e
    JOIN cand c ON e.observation_id = c.observation_id
    QUALIFY row_number() OVER (
        PARTITION BY e.observation_id
        ORDER BY e.status_rank DESC, e.at_ns DESC, e.event_id DESC
    ) = 1
)
SELECT c.*, coalesce(ev.status, 'active') AS status, ev.transfer_ref
FROM cand c
LEFT JOIN ev USING (observation_id)
WHERE list_contains(anchors, c.anchor);
