-- 0003_retention.sql — drop node_checks partitions older than the retention
-- (default 30 days). Partition bounds are derived from the _YYYY_MM name.

CREATE OR REPLACE FUNCTION proxyfarm_drop_old_check_partitions(ret_days integer DEFAULT 30)
RETURNS integer AS $$
DECLARE
  part record;
  cutoff timestamptz := now() - make_interval(days => ret_days);
  dropped integer := 0;
  part_month timestamptz;
BEGIN
  FOR part IN
    SELECT c.relname AS name
    FROM pg_inherits i
    JOIN pg_class c ON c.oid = i.inhrelid
    JOIN pg_class p ON p.oid = i.inhparent
    WHERE p.relname = 'node_checks'
      AND c.relname ~ '^node_checks_[0-9]{4}_[0-9]{2}$'
  LOOP
    part_month := to_date(substring(part.name from '[0-9]{4}_[0-9]{2}'), 'YYYY_MM');
    IF part_month < date_trunc('month', cutoff) THEN
      EXECUTE format('DROP TABLE IF EXISTS %I', part.name);
      dropped := dropped + 1;
    END IF;
  END LOOP;
  RETURN dropped;
END;
$$ LANGUAGE plpgsql;
