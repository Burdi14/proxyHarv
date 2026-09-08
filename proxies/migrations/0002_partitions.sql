-- 0002_partitions.sql — monthly partitions for node_checks (§14.5):
-- raw checks are kept 30 days; aggregates live in nodes forever.

CREATE OR REPLACE FUNCTION proxyfarm_ensure_check_partition(ts timestamptz)
RETURNS void AS $$
DECLARE
  part text;
  start_ts timestamptz;
  end_ts timestamptz;
BEGIN
  start_ts := date_trunc('month', ts);
  end_ts := start_ts + interval '1 month';
  part := 'node_checks_' || to_char(start_ts, 'YYYY_MM');
  IF NOT EXISTS (SELECT 1 FROM pg_class WHERE relname = part) THEN
    EXECUTE format('CREATE TABLE %I PARTITION OF node_checks FOR VALUES FROM (%L) TO (%L)',
                   part, start_ts, end_ts);
  END IF;
END;
$$ LANGUAGE plpgsql;

-- create current + next month partitions; coordinator re-runs the function monthly
SELECT proxyfarm_ensure_check_partition(now());
SELECT proxyfarm_ensure_check_partition(now() + interval '1 month');
