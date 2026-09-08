-- 0001_init.sql — verbatim per ARCHITECTURE.md §14.5 (норматив).

CREATE TABLE sources(
  id BIGSERIAL PRIMARY KEY,
  url TEXT NOT NULL UNIQUE,
  kind TEXT NOT NULL DEFAULT 'sub',
  enabled BOOLEAN NOT NULL DEFAULT TRUE,
  last_fetch_at TIMESTAMPTZ,
  last_status TEXT,
  note TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE nodes(
  id BIGSERIAL PRIMARY KEY,
  uri_hash TEXT NOT NULL UNIQUE,
  uri TEXT NOT NULL,
  protocol TEXT NOT NULL,          -- vless|vmess|ss|trojan|hysteria2|socks|anytls|tuic|hysteria
  transport TEXT NOT NULL DEFAULT 'tcp',
  host TEXT NOT NULL,
  host_ip TEXT,
  cc TEXT, city TEXT, lat DOUBLE PRECISION, lon DOUBLE PRECISION,
  status TEXT NOT NULL DEFAULT 'unknown',   -- unknown|alive|dead
  best_class TEXT NOT NULL DEFAULT 'none',  -- none|cloud|rf
  score DOUBLE PRECISION NOT NULL DEFAULT 0,
  first_seen TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_seen TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_l1_ok_at TIMESTAMPTZ
);
CREATE INDEX nodes_status_class_idx ON nodes(status, best_class);
CREATE INDEX nodes_geo_idx ON nodes(cc);

CREATE TABLE node_checks(
  id BIGSERIAL,
  node_id BIGINT NOT NULL REFERENCES nodes(id) ON DELETE CASCADE,
  vantage TEXT NOT NULL,
  stage TEXT NOT NULL,             -- L0|L1|L2
  ok BOOLEAN NOT NULL,
  latency_ms INTEGER,
  speed_mbps DOUBLE PRECISION,
  error_code TEXT,
  ts TIMESTAMPTZ NOT NULL DEFAULT now(),
  ts_bucket INTEGER NOT NULL,      -- epoch_seconds / 300
  -- SPEC DEVIATION (issue #1): postgres cannot enforce a UNIQUE constraint on
  -- a partitioned table unless it includes the partition key (ts). §14.5 as
  -- written ("UNIQUE(node_id, vantage, stage, ts_bucket)" + PARTITION BY ts)
  -- is not executable; adding ts keeps redelivery idempotency exact because
  -- redelivered results carry the identical ts (§18.7).
  UNIQUE(node_id, vantage, stage, ts_bucket, ts)
) PARTITION BY RANGE (ts);

CREATE TABLE vantages(
  id TEXT PRIMARY KEY,             -- 'cloud-eu-1', 'rf-home-1'
  kind TEXT NOT NULL,              -- cloud|edge
  region TEXT,
  enabled BOOLEAN NOT NULL DEFAULT TRUE,
  last_heartbeat_at TIMESTAMPTZ
);

CREATE TABLE publishes(
  version BIGSERIAL PRIMARY KEY,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  class_filter TEXT NOT NULL,
  node_count INTEGER NOT NULL,
  checksum TEXT NOT NULL,
  kv_key TEXT NOT NULL
);
