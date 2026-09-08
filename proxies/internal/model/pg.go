package model

import (
	"context"
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"proxyfarm/migrations"
)

// PGStore implements Store on top of pgx.
type PGStore struct {
	pool *pgxpool.Pool
}

// Open creates a connection pool and pings it.
func Open(ctx context.Context, dsn string) (*PGStore, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		return nil, err
	}
	return &PGStore{pool: pool}, nil
}

func (s *PGStore) Close() { s.pool.Close() }

// ---- schema ----

func (s *PGStore) Migrate(ctx context.Context) error {
	if _, err := s.pool.Exec(ctx,
		`CREATE TABLE IF NOT EXISTS schema_migrations(
		   version TEXT PRIMARY KEY, applied_at TIMESTAMPTZ NOT NULL DEFAULT now())`); err != nil {
		return err
	}
	for i, name := range migrations.Names() {
		var exists bool
		if err := s.pool.QueryRow(ctx,
			`SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE version=$1)`, name).Scan(&exists); err != nil {
			return err
		}
		if exists {
			continue
		}
		f, err := migrations.FS().Open(name)
		if err != nil {
			return err
		}
		body, err := io.ReadAll(f)
		_ = f.Close()
		if err != nil {
			return err
		}
		tx, err := s.pool.Begin(ctx)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, string(body)); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("migration %s: %w", name, err)
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO schema_migrations(version) VALUES($1)`, name); err != nil {
			_ = tx.Rollback(ctx)
			return err
		}
		if err := tx.Commit(ctx); err != nil {
			return err
		}
		_ = i
	}
	return nil
}

func (s *PGStore) SeedSQL(ctx context.Context, sqlText string) error {
	_, err := s.pool.Exec(ctx, sqlText)
	return err
}

func (s *PGStore) EnsureCheckPartition(ctx context.Context, ts time.Time) error {
	_, err := s.pool.Exec(ctx, `SELECT proxyfarm_ensure_check_partition($1)`, ts)
	return err
}

func (s *PGStore) DropOldCheckPartitions(ctx context.Context, days int) error {
	_, err := s.pool.Exec(ctx, `SELECT proxyfarm_drop_old_check_partitions($1)`, days)
	return err
}

func (s *PGStore) DeleteLongDead(ctx context.Context, olderThan time.Time) (int64, error) {
	tag, err := s.pool.Exec(ctx,
		`DELETE FROM nodes WHERE status='dead'
		   AND COALESCE(last_l1_ok_at, first_seen) < $1`, olderThan)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

// ---- nodes ----

func (s *PGStore) UpsertNode(ctx context.Context, n *Node) (int64, bool, error) {
	var id int64
	var created bool
	err := s.pool.QueryRow(ctx, `
		INSERT INTO nodes(uri_hash, uri, protocol, transport, host)
		VALUES($1,$2,$3,$4,$5)
		ON CONFLICT (uri_hash) DO UPDATE SET last_seen = now()
		RETURNING id, (xmax = 0)`,
		n.URIHash, n.URI, n.Protocol, n.Transport, n.Host).Scan(&id, &created)
	if err != nil {
		return 0, false, err
	}
	n.ID = id
	return id, created, nil
}

func (s *PGStore) UpdateNodeGeo(ctx context.Context, id int64, hostIP string, cc, city *string, lat, lon *float64) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE nodes SET host_ip=$2, cc=$3, city=$4, lat=$5, lon=$6 WHERE id=$1`,
		id, nullableStr(hostIP), cc, city, lat, lon)
	return err
}

func nullableStr(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func (s *PGStore) GetNode(ctx context.Context, id int64) (*Node, error) {
	return s.scanNode(s.pool.QueryRow(ctx, nodeCols+` WHERE id=$1`, id))
}

func (s *PGStore) GetNodeByHash(ctx context.Context, hash string) (*Node, error) {
	return s.scanNode(s.pool.QueryRow(ctx, nodeCols+` WHERE uri_hash=$1`, hash))
}

const nodeCols = `SELECT id, uri_hash, uri, protocol, transport, host, host_ip,
	cc, city, lat, lon, status, best_class, score, first_seen, last_seen, last_l1_ok_at FROM nodes `

func (s *PGStore) scanNode(row pgx.Row) (*Node, error) {
	var n Node
	err := row.Scan(&n.ID, &n.URIHash, &n.URI, &n.Protocol, &n.Transport, &n.Host,
		&n.HostIP, &n.CC, &n.City, &n.Lat, &n.Lon, &n.Status, &n.BestClass,
		&n.Score, &n.FirstSeen, &n.LastSeen, &n.LastL1OKAt)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &n, nil
}

// dueNodes implements the §4 retest policy:
//
//	alive fresh (<24h) → 3h; alive → 6h; dead → backoff 1h→2h→…→48h cap.
//
// Consecutive failures are approximated by failed L1s among the last 8.
func (s *PGStore) dueNodes(ctx context.Context, limit int, transportIn []string, transportNotIn []string) ([]Node, error) {
	args := []any{limit}
	tFilter := ""
	if transportIn != nil {
		args = append(args, transportIn)
		tFilter = fmt.Sprintf(" AND n.transport = ANY($%d) ", len(args))
	}
	if transportNotIn != nil {
		args = append(args, transportNotIn)
		tFilter += fmt.Sprintf(" AND NOT (n.transport = ANY($%d)) ", len(args))
	}
	q := `
	SELECT n.id, n.uri_hash, n.uri, n.protocol, n.transport, n.host, n.host_ip,
	       n.cc, n.city, n.lat, n.lon, n.status, n.best_class, n.score,
	       n.first_seen, n.last_seen, n.last_l1_ok_at
	FROM nodes n
	LEFT JOIN LATERAL (
	  SELECT ts FROM node_checks c WHERE c.node_id = n.id
	  ORDER BY ts DESC LIMIT 1
	) lc ON true
	LEFT JOIN LATERAL (
	  SELECT count(*) FILTER (WHERE NOT ok) AS fails FROM (
	    SELECT ok FROM node_checks c
	    WHERE c.node_id = n.id AND c.stage = 'L1'
	    ORDER BY ts DESC LIMIT 8
	  ) recent
	) ff ON true
	WHERE 1=1 ` + tFilter + ` AND (
	  n.status = 'unknown'
	  OR (n.status = 'alive' AND (lc.ts IS NULL OR lc.ts < (
	        CASE WHEN n.last_l1_ok_at > now() - interval '24 hours'
	             THEN now() - interval '3 hours'
	             ELSE now() - interval '6 hours' END)))
	  OR (n.status = 'dead' AND (lc.ts IS NULL OR lc.ts < now() - make_interval(
	        hours => least(48, 1 << least(greatest(COALESCE(ff.fails, 1) - 1, 0)::int, 6))))))
	ORDER BY n.last_seen DESC
	LIMIT $1`
	rows, err := s.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Node{}
	for rows.Next() {
		var n Node
		if err := rows.Scan(&n.ID, &n.URIHash, &n.URI, &n.Protocol, &n.Transport, &n.Host,
			&n.HostIP, &n.CC, &n.City, &n.Lat, &n.Lon, &n.Status, &n.BestClass,
			&n.Score, &n.FirstSeen, &n.LastSeen, &n.LastL1OKAt); err != nil {
			return nil, err
		}
		out = append(out, n)
	}
	return out, rows.Err()
}

// tcpFamily are transports worth a cheap L0 dial first (§4).
var tcpFamily = []string{"tcp", "ws", "grpc", "http", "httpupgrade", "xhttp", "kcp"}

func (s *PGStore) NodesDueL0(ctx context.Context, limit int) ([]Node, error) {
	return s.dueNodes(ctx, limit, tcpFamily, nil)
}

func (s *PGStore) NodesDueL1(ctx context.Context, limit int) ([]Node, error) {
	return s.dueNodes(ctx, limit, nil, tcpFamily)
}

func (s *PGStore) NodesForL2(ctx context.Context, topK int, notCheckedSince time.Duration, limit int) ([]Node, error) {
	rows, err := s.pool.Query(ctx, `
	SELECT n.id, n.uri_hash, n.uri, n.protocol, n.transport, n.host, n.host_ip,
	       n.cc, n.city, n.lat, n.lon, n.status, n.best_class, n.score,
	       n.first_seen, n.last_seen, n.last_l1_ok_at
	FROM nodes n
	WHERE n.status = 'alive'
	  AND NOT EXISTS (
	    SELECT 1 FROM node_checks c
	    WHERE c.node_id = n.id AND c.stage = 'L2' AND c.ok
	      AND c.ts > now() - $2::interval)
	ORDER BY n.score DESC
	LIMIT LEAST($1, $3)`,
		topK, intervalStr(notCheckedSince), limit)
	return scanNodes(rows, err)
}

func (s *PGStore) NodesForValidation(ctx context.Context, vantage string, topN int) ([]Node, error) {
	rows, err := s.pool.Query(ctx, `
	SELECT n.id, n.uri_hash, n.uri, n.protocol, n.transport, n.host, n.host_ip,
	       n.cc, n.city, n.lat, n.lon, n.status, n.best_class, n.score,
	       n.first_seen, n.last_seen, n.last_l1_ok_at
	FROM nodes n
	WHERE n.status = 'alive'
	  AND NOT EXISTS (
	    SELECT 1 FROM node_checks c
	    WHERE c.node_id = n.id AND c.vantage = $1 AND c.stage = 'L1' AND c.ok
	      AND c.ts > now() - interval '3 hours')
	ORDER BY n.score DESC
	LIMIT $2`, vantage, topN)
	return scanNodes(rows, err)
}

func (s *PGStore) ListNodesForPublish(ctx context.Context) ([]Node, error) {
	rows, err := s.pool.Query(ctx, nodeCols+`WHERE status='alive' ORDER BY score DESC`)
	return scanNodes(rows, err)
}

func scanNodes(rows pgx.Rows, err error) ([]Node, error) {
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Node{}
	for rows.Next() {
		var n Node
		if err := rows.Scan(&n.ID, &n.URIHash, &n.URI, &n.Protocol, &n.Transport, &n.Host,
			&n.HostIP, &n.CC, &n.City, &n.Lat, &n.Lon, &n.Status, &n.BestClass,
			&n.Score, &n.FirstSeen, &n.LastSeen, &n.LastL1OKAt); err != nil {
			return nil, err
		}
		out = append(out, n)
	}
	return out, rows.Err()
}

func intervalStr(d time.Duration) string { return strconv.Itoa(int(d.Seconds())) + " seconds" }

// ---- checks ----

func (s *PGStore) InsertCheck(ctx context.Context, c *Check) (bool, error) {
	var id int64
	err := s.pool.QueryRow(ctx, `
		INSERT INTO node_checks(node_id, vantage, stage, ok, latency_ms, speed_mbps, error_code, ts, ts_bucket)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)
		ON CONFLICT (node_id, vantage, stage, ts_bucket, ts) DO NOTHING
		RETURNING id`,
		c.NodeID, c.Vantage, c.Stage, c.OK, c.LatencyMs, c.SpeedMbps, c.ErrorCode,
		c.TS, c.TSBucket).Scan(&id)
	if err == pgx.ErrNoRows {
		return false, nil // redelivery duplicate (§18.7)
	}
	if err != nil {
		return false, err
	}
	c.ID = id
	return true, nil
}

func (s *PGStore) RecentL1(ctx context.Context, nodeID int64, vantage string, limit int) ([]bool, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT ok FROM node_checks
		WHERE node_id=$1 AND stage='L1' AND ($2='' OR vantage=$2)
		ORDER BY ts DESC LIMIT $3`, nodeID, vantage, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []bool{}
	for rows.Next() {
		var ok bool
		if err := rows.Scan(&ok); err != nil {
			return nil, err
		}
		out = append(out, ok)
	}
	return out, rows.Err()
}

func (s *PGStore) LatestCheck(ctx context.Context, nodeID int64, stages []string) (*Check, error) {
	var c Check
	err := s.pool.QueryRow(ctx, `
		SELECT id, node_id, vantage, stage, ok, latency_ms, speed_mbps, error_code, ts, ts_bucket
		FROM node_checks
		WHERE node_id=$1 AND stage = ANY($2)
		ORDER BY ts DESC LIMIT 1`, nodeID, stages).Scan(
		&c.ID, &c.NodeID, &c.Vantage, &c.Stage, &c.OK, &c.LatencyMs,
		&c.SpeedMbps, &c.ErrorCode, &c.TS, &c.TSBucket)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &c, nil
}

func (s *PGStore) LatestByVantage(ctx context.Context, nodeID int64) (map[string]Check, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT DISTINCT ON (vantage)
		  id, node_id, vantage, stage, ok, latency_ms, speed_mbps, error_code, ts, ts_bucket
		FROM node_checks WHERE node_id=$1
		ORDER BY vantage, ts DESC`, nodeID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]Check{}
	for rows.Next() {
		var c Check
		if err := rows.Scan(&c.ID, &c.NodeID, &c.Vantage, &c.Stage, &c.OK,
			&c.LatencyMs, &c.SpeedMbps, &c.ErrorCode, &c.TS, &c.TSBucket); err != nil {
			return nil, err
		}
		out[c.Vantage] = c
	}
	return out, rows.Err()
}

func (s *PGStore) UpdateNodeAggregate(ctx context.Context, u NodeUpdate) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE nodes SET status=$2, best_class=$3, score=$4, last_l1_ok_at=$5
		WHERE id=$1`, u.NodeID, u.Status, u.BestClass, u.Score, u.LastL1OKAt)
	return err
}

// ---- vantages / sources / publishes ----

func (s *PGStore) UpsertVantage(ctx context.Context, v *Vantage) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO vantages(id, kind, region, enabled, last_heartbeat_at)
		VALUES($1,$2,$3,$4,$5)
		ON CONFLICT (id) DO UPDATE SET
		  kind=EXCLUDED.kind,
		  region=EXCLUDED.region,
		  last_heartbeat_at=EXCLUDED.last_heartbeat_at`,
		v.ID, v.Kind, v.Region, v.Enabled, v.LastHeartbeat)
	return err
}

func (s *PGStore) DeleteVantage(ctx context.Context, id string) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM vantages WHERE id=$1`, id)
	return err
}

func (s *PGStore) ListVantages(ctx context.Context, enabledOnly bool) ([]Vantage, error) {
	q := `SELECT id, kind, region, enabled, last_heartbeat_at FROM vantages`
	if enabledOnly {
		q += ` WHERE enabled`
	}
	rows, err := s.pool.Query(ctx, q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Vantage{}
	for rows.Next() {
		var v Vantage
		if err := rows.Scan(&v.ID, &v.Kind, &v.Region, &v.Enabled, &v.LastHeartbeat); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

func (s *PGStore) ListSources(ctx context.Context, enabledOnly bool) ([]Source, error) {
	q := `SELECT id, url, kind, enabled, last_fetch_at, last_status, note, created_at FROM sources`
	if enabledOnly {
		q += ` WHERE enabled`
	}
	rows, err := s.pool.Query(ctx, q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Source{}
	for rows.Next() {
		var src Source
		if err := rows.Scan(&src.ID, &src.URL, &src.Kind, &src.Enabled, &src.LastFetchAt,
			&src.LastStatus, &src.Note, &src.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, src)
	}
	return out, rows.Err()
}

func (s *PGStore) UpsertSource(ctx context.Context, src *Source) (int64, error) {
	var id int64
	err := s.pool.QueryRow(ctx, `
		INSERT INTO sources(url, kind, enabled, note)
		VALUES($1,$2,$3,$4)
		ON CONFLICT (url) DO UPDATE SET
		  kind=EXCLUDED.kind, enabled=EXCLUDED.enabled, note=EXCLUDED.note
		RETURNING id`,
		src.URL, src.Kind, src.Enabled, src.Note).Scan(&id)
	return id, err
}

func (s *PGStore) DeleteSource(ctx context.Context, id int64) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM sources WHERE id=$1`, id)
	return err
}

func (s *PGStore) UpdateSourceStatus(ctx context.Context, id int64, status string, at time.Time) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE sources SET last_status=$2, last_fetch_at=$3 WHERE id=$1`, id, status, at)
	return err
}

func (s *PGStore) InsertPublish(ctx context.Context, p *Publish) error {
	return s.pool.QueryRow(ctx, `
		INSERT INTO publishes(class_filter, node_count, checksum, kv_key)
		VALUES($1,$2,$3,$4) RETURNING version, created_at`,
		p.ClassFilter, p.NodeCount, p.Checksum, p.KVKey).Scan(&p.Version, &p.CreatedAt)
}

func (s *PGStore) ListPublishes(ctx context.Context, limit int) ([]Publish, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT version, created_at, class_filter, node_count, checksum, kv_key
		FROM publishes ORDER BY version DESC LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Publish{}
	for rows.Next() {
		var p Publish
		if err := rows.Scan(&p.Version, &p.CreatedAt, &p.ClassFilter, &p.NodeCount,
			&p.Checksum, &p.KVKey); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

func (s *PGStore) LatestPublish(ctx context.Context) (*Publish, error) {
	var p Publish
	err := s.pool.QueryRow(ctx, `
		SELECT version, created_at, class_filter, node_count, checksum, kv_key
		FROM publishes ORDER BY version DESC LIMIT 1`).Scan(
		&p.Version, &p.CreatedAt, &p.ClassFilter, &p.NodeCount, &p.Checksum, &p.KVKey)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &p, nil
}

// ---- api reads ----

func (s *PGStore) CityAggregates(ctx context.Context, class string) ([]CityAgg, error) {
	q := `
	SELECT n.city, n.cc,
	       avg(n.lat)::float8 AS lat, avg(n.lon)::float8 AS lon,
	       count(*) AS cnt,
	       min(l.latency_ms) AS best_latency,
	       array_agg(DISTINCT n.protocol) AS protocols
	FROM nodes n
	LEFT JOIN LATERAL (
	  SELECT latency_ms FROM node_checks c
	  WHERE c.node_id = n.id AND c.stage='L1' AND c.ok
	  ORDER BY ts DESC LIMIT 1
	) l ON true
	WHERE n.status='alive' AND n.city IS NOT NULL
	  AND ($1 = '' OR n.best_class = $1 OR ($1 = 'cloud' AND n.best_class = 'rf'))
	GROUP BY n.city, n.cc
	ORDER BY cnt DESC`
	rows, err := s.pool.Query(ctx, q, class)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []CityAgg{}
	for rows.Next() {
		var a CityAgg
		if err := rows.Scan(&a.City, &a.CC, &a.Lat, &a.Lon, &a.Count, &a.BestLatencyMs, &a.Protocols); err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

func (s *PGStore) NodeCards(ctx context.Context, city, class, proto string, limit int) ([]NodeCard, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	q := `
	SELECT n.id, n.protocol, n.transport, COALESCE(n.cc,''), COALESCE(n.city,''),
	       COALESCE(n.lat,0), COALESCE(n.lon,0), n.best_class,
	       l.latency_ms, sp.speed_mbps, l.ts, n.uri
	FROM nodes n
	LEFT JOIN LATERAL (
	  SELECT latency_ms, ts FROM node_checks c
	  WHERE c.node_id=n.id AND c.stage='L1' ORDER BY ts DESC LIMIT 1
	) l ON true
	LEFT JOIN LATERAL (
	  SELECT speed_mbps FROM node_checks c
	  WHERE c.node_id=n.id AND c.stage='L2' AND c.ok ORDER BY ts DESC LIMIT 1
	) sp ON true
	WHERE n.status='alive'
	  AND ($1 = '' OR n.city = $1)
	  AND ($2 = '' OR n.best_class = $2 OR ($2 = 'cloud' AND n.best_class = 'rf'))
	  AND ($3 = '' OR n.protocol = $3)
	ORDER BY n.score DESC
	LIMIT $4`
	rows, err := s.pool.Query(ctx, q, city, class, proto, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanCards(rows)
}

func (s *PGStore) NodeCard(ctx context.Context, id int64) (*NodeCard, error) {
	rows, err := s.pool.Query(ctx, `
	SELECT n.id, n.protocol, n.transport, COALESCE(n.cc,''), COALESCE(n.city,''),
	       COALESCE(n.lat,0), COALESCE(n.lon,0), n.best_class,
	       l.latency_ms, sp.speed_mbps, l.ts, n.uri
	FROM nodes n
	LEFT JOIN LATERAL (
	  SELECT latency_ms, ts FROM node_checks c
	  WHERE c.node_id=n.id AND c.stage='L1' ORDER BY ts DESC LIMIT 1
	) l ON true
	LEFT JOIN LATERAL (
	  SELECT speed_mbps FROM node_checks c
	  WHERE c.node_id=n.id AND c.stage='L2' AND c.ok ORDER BY ts DESC LIMIT 1
	) sp ON true
	WHERE n.id = $1`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	cards, err := scanCards(rows)
	if err != nil || len(cards) == 0 {
		return nil, err
	}
	return &cards[0], nil
}

func scanCards(rows pgx.Rows) ([]NodeCard, error) {
	out := []NodeCard{}
	for rows.Next() {
		var c NodeCard
		var lastCheck *time.Time
		if err := rows.Scan(&c.ID, &c.Protocol, &c.Transport, &c.CC, &c.City,
			&c.Lat, &c.Lon, &c.Class, &c.LatencyMs, &c.SpeedMbps, &lastCheck, &c.ShareLink); err != nil {
			return nil, err
		}
		c.LastCheck = lastCheck
		out = append(out, c)
	}
	return out, rows.Err()
}

func (s *PGStore) AliveCount(ctx context.Context) (int, error) {
	var n int
	err := s.pool.QueryRow(ctx,
		`SELECT count(*) FROM nodes WHERE status='alive'`).Scan(&n)
	return n, err
}

func (s *PGStore) CountNodes(ctx context.Context) (int64, error) {
	var n int64
	err := s.pool.QueryRow(ctx, `SELECT count(*) FROM nodes`).Scan(&n)
	return n, err
}
