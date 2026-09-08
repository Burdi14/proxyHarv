// Command api serves the public read-only REST API (§8): map aggregates,
// node cards, the base64 subscription for v2rayN, SSE publish events, and
// token-guarded admin endpoints for sources/vantages.
package main

import (
	"context"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"proxyfarm/internal/logx"
	"proxyfarm/internal/model"
	"proxyfarm/internal/natsb"
	"proxyfarm/internal/parse"
)

var log = logx.New("api")

func main() {
	var (
		addr        = flag.String("addr", envOr("API_ADDR", ":8080"), "listen address")
		pgDSN       = flag.String("pg", envOr("PG_DSN", "postgres://proxyfarm:proxyfarm@localhost:5432/proxyfarm?sslmode=disable"), "postgres dsn")
		natsURL     = flag.String("nats", envOr("NATS_URL", "nats://127.0.0.1:4222"), "nats url")
		natsCreds   = flag.String("nats-creds", envOr("NATS_CREDS", ""), "nats credentials")
		adminTok    = flag.String("admin-token", envOr("ADMIN_TOKEN", ""), "X-Admin-Token value (empty disables admin)")
		webDir      = flag.String("web-dir", envOr("WEB_DIR", "web"), "static frontend dir")
		subInterval = flag.Int("profile-update-interval", 6, "Profile-Update-Interval hours (§14.7)")
	)
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	store, err := model.Open(ctx, *pgDSN)
	if err != nil {
		log.Error("pg open", "err", err.Error())
		os.Exit(1)
	}
	defer store.Close()

	nc, err := natsb.Connect(*natsURL, *natsCreds)
	if err != nil {
		log.Error("nats connect", "err", err.Error())
		os.Exit(1)
	}
	defer nc.Drain()
	js, err := jetstream.New(nc)
	if err != nil {
		log.Error("jetstream", "err", err.Error())
		os.Exit(1)
	}
	bus := natsb.NewBus(js)

	app := &apiApp{store: store, bus: bus, adminToken: *adminTok, subInterval: *subInterval}
	mux := app.routes(*webDir)

	srv := &http.Server{
		Addr:              *addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		<-ctx.Done()
		sctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = srv.Shutdown(sctx)
	}()
	log.Info("api listening", "addr", *addr)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Error("server", "err", err.Error())
		os.Exit(1)
	}
}

// routes builds the full ServeMux (also used by tests).
func (a *apiApp) routes(webDir string) *http.ServeMux {
	mux := http.NewServeMux()
	// public api (§8)
	mux.HandleFunc("GET /api/cities", a.handleCities)
	mux.HandleFunc("GET /api/nodes", a.handleNodes)
	mux.HandleFunc("GET /api/node/{id}", a.handleNode)
	mux.HandleFunc("GET /api/sub", a.handleSub)
	mux.HandleFunc("GET /api/publishes", a.handlePublishes)
	mux.HandleFunc("GET /api/events", a.handleEvents)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	// admin (§8: X-Admin-Token)
	mux.HandleFunc("GET /api/admin/sources", a.admin(a.handleAdminListSources))
	mux.HandleFunc("POST /api/admin/sources", a.admin(a.handleAdminPostSource))
	mux.HandleFunc("DELETE /api/admin/sources/{id}", a.admin(a.handleAdminDeleteSource))
	mux.HandleFunc("GET /api/admin/vantages", a.admin(a.handleAdminListVantages))
	mux.HandleFunc("POST /api/admin/vantages", a.admin(a.handleAdminPostVantage))
	mux.HandleFunc("DELETE /api/admin/vantages/{id}", a.admin(a.handleAdminDeleteVantage))

	// static frontend (skipped silently when the dir has no index.html)
	if st, err := fs.Stat(os.DirFS(webDir), "index.html"); err == nil && !st.IsDir() {
		mux.Handle("/", http.FileServerFS(os.DirFS(webDir)))
	}
	return mux
}

type apiApp struct {
	store       model.Store
	bus         *natsb.Bus
	adminToken  string
	subInterval int
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetIndent("", " ")
	_ = enc.Encode(v)
}

func fail(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

// GET /api/cities?class=rf|cloud (§14.7)
func (a *apiApp) handleCities(w http.ResponseWriter, r *http.Request) {
	class := r.URL.Query().Get("class")
	if class != "" && class != "rf" && class != "cloud" {
		fail(w, http.StatusBadRequest, "class must be rf|cloud")
		return
	}
	cities, err := a.store.CityAggregates(r.Context(), class)
	if err != nil {
		fail(w, http.StatusInternalServerError, "db error")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"cities": cities})
}

// GET /api/nodes?city=&class=&proto=
func (a *apiApp) handleNodes(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	limit := 100
	if v := q.Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 && n <= 500 {
			limit = n
		}
	}
	cards, err := a.store.NodeCards(r.Context(), q.Get("city"), q.Get("class"), q.Get("proto"), limit)
	if err != nil {
		fail(w, http.StatusInternalServerError, "db error")
		return
	}
	// share_link only in /api/node/{id} detail (§8) — strip here
	for i := range cards {
		cards[i].ShareLink = ""
	}
	writeJSON(w, http.StatusOK, map[string]any{"nodes": cards})
}

// GET /api/node/{id} (§14.7)
func (a *apiApp) handleNode(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil || id <= 0 {
		fail(w, http.StatusBadRequest, "bad node id")
		return
	}
	card, err := a.store.NodeCard(r.Context(), id)
	if err != nil {
		fail(w, http.StatusInternalServerError, "db error")
		return
	}
	if card == nil {
		fail(w, http.StatusNotFound, "node not found")
		return
	}
	resp := map[string]any{
		"id":         card.ID,
		"protocol":   card.Protocol,
		"transport":  card.Transport,
		"cc":         card.CC,
		"city":       card.City,
		"class":      card.Class,
		"latency_ms": card.LatencyMs,
		"speed_mbps": card.SpeedMbps,
		"last_check": card.LastCheck,
		"share_link": card.ShareLink,
	}
	// ready-to-import sing-box outbound
	if parsed, err := parse.ParseLine(card.ShareLink); err == nil && parsed != nil {
		resp["outbound"] = json.RawMessage(parsed.Outbound)
	}
	writeJSON(w, http.StatusOK, resp)
}

// GET /api/sub?class=rf&proto=&limit=50 → base64 subscription (§14.7, §18.12)
func (a *apiApp) handleSub(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	class := q.Get("class")
	if class == "" {
		class = "rf"
	}
	if class != "rf" && class != "cloud" {
		fail(w, http.StatusBadRequest, "class must be rf|cloud")
		return
	}
	limit := 50
	if v := q.Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 && n <= 500 {
			limit = n
		}
	}
	cards, err := a.store.NodeCards(r.Context(), "", class, q.Get("proto"), limit)
	if err != nil {
		fail(w, http.StatusInternalServerError, "db error")
		return
	}
	lines := make([]string, 0, len(cards))
	for _, c := range cards {
		latency := 0
		if c.LatencyMs != nil {
			latency = *c.LatencyMs
		}
		name := parse.ShareName(strOr(c.CC, "XX"), strOr(c.City, "Unknown"), latency, c.Class)
		base := c.ShareLink
		if i := strings.IndexByte(base, '#'); i >= 0 {
			base = base[:i]
		}
		lines = append(lines, base+"#"+url.QueryEscape(name))
	}
	body := base64.StdEncoding.EncodeToString([]byte(strings.Join(lines, "\n")))
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("Profile-Update-Interval", strconv.Itoa(a.subInterval))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(body))
}

// GET /api/publishes
func (a *apiApp) handlePublishes(w http.ResponseWriter, r *http.Request) {
	pubs, err := a.store.ListPublishes(r.Context(), 50)
	if err != nil {
		fail(w, http.StatusInternalServerError, "db error")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"publishes": pubs})
}

// GET /api/events — SSE: "publish" events on new KV versions (§8)
func (a *apiApp) handleEvents(w http.ResponseWriter, r *http.Request) {
	fl, ok := w.(http.Flusher)
	if !ok {
		fail(w, http.StatusInternalServerError, "streaming unsupported")
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, ": connected\n\n")
	fl.Flush()

	ctx := r.Context()
	events := make(chan []byte, 8)
	go func() {
		defer close(events)
		_ = a.bus.WatchPublishes(ctx, func(version int64, value []byte) {
			select {
			case events <- value:
			default:
			}
		})
	}()
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case val, ok := <-events:
			if !ok {
				return
			}
			fmt.Fprintf(w, "event: publish\ndata: %s\n\n", val)
			fl.Flush()
		case <-ticker.C:
			fmt.Fprint(w, ": ping\n\n")
			fl.Flush()
		}
	}
}

// ---- admin ----

func (a *apiApp) admin(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if a.adminToken == "" {
			fail(w, http.StatusServiceUnavailable, "admin disabled")
			return
		}
		got := r.Header.Get("X-Admin-Token")
		if subtle.ConstantTimeCompare([]byte(got), []byte(a.adminToken)) != 1 {
			fail(w, http.StatusUnauthorized, "bad token")
			return
		}
		next(w, r)
	}
}

func (a *apiApp) handleAdminListSources(w http.ResponseWriter, r *http.Request) {
	srcs, err := a.store.ListSources(r.Context(), false)
	if err != nil {
		fail(w, http.StatusInternalServerError, "db error")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"sources": srcs})
}

func (a *apiApp) handleAdminPostSource(w http.ResponseWriter, r *http.Request) {
	var req struct {
		URL     string `json:"url"`
		Kind    string `json:"kind"`
		Enabled *bool  `json:"enabled"`
		Note    string `json:"note"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil || req.URL == "" {
		fail(w, http.StatusBadRequest, "body must be {url, kind, enabled, note}")
		return
	}
	enabled := true
	if req.Enabled != nil {
		enabled = *req.Enabled
	}
	kind := req.Kind
	if kind == "" {
		kind = "sub"
	}
	src := &model.Source{URL: req.URL, Kind: kind, Enabled: enabled}
	if req.Note != "" {
		src.Note = &req.Note
	}
	id, err := a.store.UpsertSource(r.Context(), src)
	if err != nil {
		fail(w, http.StatusInternalServerError, "db error")
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{"id": id})
}

func (a *apiApp) handleAdminDeleteSource(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		fail(w, http.StatusBadRequest, "bad id")
		return
	}
	if err := a.store.DeleteSource(r.Context(), id); err != nil {
		fail(w, http.StatusInternalServerError, "db error")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (a *apiApp) handleAdminListVantages(w http.ResponseWriter, r *http.Request) {
	vs, err := a.store.ListVantages(r.Context(), false)
	if err != nil {
		fail(w, http.StatusInternalServerError, "db error")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"vantages": vs})
}

func (a *apiApp) handleAdminPostVantage(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ID      string  `json:"id"`
		Kind    string  `json:"kind"`
		Region  *string `json:"region"`
		Enabled *bool   `json:"enabled"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil ||
		req.ID == "" || (req.Kind != "cloud" && req.Kind != "edge") {
		fail(w, http.StatusBadRequest, "body must be {id, kind: cloud|edge, region, enabled}")
		return
	}
	enabled := true
	if req.Enabled != nil {
		enabled = *req.Enabled
	}
	v := &model.Vantage{ID: req.ID, Kind: req.Kind, Region: req.Region, Enabled: enabled}
	if err := a.store.UpsertVantage(r.Context(), v); err != nil {
		fail(w, http.StatusInternalServerError, "db error")
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{"id": req.ID})
}

func (a *apiApp) handleAdminDeleteVantage(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		fail(w, http.StatusBadRequest, "bad id")
		return
	}
	if err := a.store.DeleteVantage(r.Context(), id); err != nil {
		fail(w, http.StatusInternalServerError, "db error")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func strOr(s, def string) string {
	if s == "" {
		return def
	}
	return s
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
