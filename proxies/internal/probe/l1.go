package probe

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

// Port range reserved for per-node sing-box socks inbounds (§14.6).
const (
	portMin = 20000
	portMax = 20999
)

// PortAllocator hands out listen ports from the 20000–20999 range.
type PortAllocator struct{ ch chan int }

func NewPortAllocator() *PortAllocator {
	pa := &PortAllocator{ch: make(chan int, portMax-portMin+1)}
	for p := portMin; p <= portMax; p++ {
		pa.ch <- p
	}
	return pa
}

func (pa *PortAllocator) Acquire() int  { return <-pa.ch }
func (pa *PortAllocator) Release(p int) { pa.ch <- p }

// Runner executes L1/L2 probes through an external sing-box process.
type Runner struct {
	SingBox   string        // sing-box binary path
	Curl      string        // curl binary path
	ProbeURLs []string      // L1 targets, fallback order (§14.6)
	L2URL     string        // speed test URL template with %d for bytes
	L2Bytes   int64         // default 25 MB (§17)
	L1Timeout time.Duration // curl max-time (10s) — wall clock 20s
	L2Timeout time.Duration // curl max-time (30s) — wall clock 45s
	ReadyWait time.Duration // wait for socks inbound to accept
	ports     *PortAllocator
	sem       chan struct{}
	mu        sync.Mutex
	started   bool
}

// NewRunner builds a Runner; concurrency = PROBE_CONCURRENCY (default 32, §17).
func NewRunner(singBox, curl string, concurrency int, probeURLs []string, l2URL string, l2Bytes int64) *Runner {
	if concurrency <= 0 || concurrency > 1000 {
		concurrency = 32
	}
	if l2Bytes <= 0 {
		l2Bytes = 25_000_000
	}
	if len(probeURLs) == 0 {
		probeURLs = defaultProbeURLs("")
	}
	if l2URL == "" {
		l2URL = "https://speed.cloudflare.com/__down?bytes=%d"
	}
	return &Runner{
		SingBox: singBox, Curl: curl,
		ProbeURLs: probeURLs, L2URL: l2URL, L2Bytes: l2Bytes,
		L1Timeout: 10 * time.Second, L2Timeout: 30 * time.Second,
		ReadyWait: 3 * time.Second,
		ports:     NewPortAllocator(),
		sem:       make(chan struct{}, concurrency),
	}
}

// DefaultProbeURLs builds the §14.6 fallback chain from the probe domain.
func defaultProbeURLs(probeDomain string) []string {
	urls := []string{}
	if probeDomain != "" {
		urls = append(urls, fmt.Sprintf("http://%s/generate_204", probeDomain))
	}
	urls = append(urls,
		"http://cp.cloudflare.com/generate_204",
		"http://www.gstatic.com/generate_204",
	)
	return urls
}

// DefaultProbeURLs is the exported variant used by cmd/worker.
func DefaultProbeURLs(probeDomain string) []string { return defaultProbeURLs(probeDomain) }

// ProbeL1 runs the full-protocol liveness probe for one node.
func (r *Runner) ProbeL1(ctx context.Context, outbound json.RawMessage) Result {
	r.sem <- struct{}{}
	defer func() { <-r.sem }()
	return r.run(ctx, outbound, "L1")
}

// ProbeL2 runs the speed test for one node.
func (r *Runner) ProbeL2(ctx context.Context, outbound json.RawMessage) Result {
	r.sem <- struct{}{}
	defer func() { <-r.sem }()
	return r.run(ctx, outbound, "L2")
}

func (r *Runner) run(ctx context.Context, outbound json.RawMessage, stage string) Result {
	stageTimeout := r.L1Timeout*2 + r.ReadyWait // wall-clock L1=20s
	curlTimeout := r.L1Timeout
	if stage == "L2" {
		stageTimeout = r.L2Timeout + 15*time.Second // 45s
		curlTimeout = r.L2Timeout
	}
	ctx, cancel := context.WithTimeout(ctx, stageTimeout)
	defer cancel()

	var res Result
	var done = make(chan struct{})
	go func() {
		defer close(done)
		res = r.runOnce(ctx, outbound, stage, curlTimeout)
	}()
	select {
	case <-done:
		return res
	case <-ctx.Done():
		return Result{Code: CodeProcessTimeout}
	}
}

func (r *Runner) runOnce(ctx context.Context, outbound json.RawMessage, stage string, curlTimeout time.Duration) Result {
	var lastCode = CodeInternal
	for attempt := 0; attempt < 2; attempt++ {
		port := r.ports.Acquire()
		code, res := r.attempt(ctx, outbound, stage, curlTimeout, port)
		r.ports.Release(port)
		if code == "" || code != CodeInternal || attempt == 1 {
			return res
		}
		// port collision or transient spawn failure → single retry (§14.6 allocator)
		lastCode = code
	}
	return Result{Code: lastCode}
}

func (r *Runner) attempt(ctx context.Context, outbound json.RawMessage, stage string, curlTimeout time.Duration, port int) (string, Result) {
	dir, err := os.MkdirTemp("", "proxyfarm-probe-*")
	if err != nil {
		return CodeInternal, Result{Code: CodeInternal}
	}
	defer os.RemoveAll(dir)

	cfg, err := buildSingboxConfig(outbound, port)
	if err != nil {
		return "", Result{Code: CodeInternal}
	}
	cfgPath := filepath.Join(dir, "config.json")
	if err := os.WriteFile(cfgPath, cfg, 0o600); err != nil {
		return CodeInternal, Result{Code: CodeInternal}
	}

	cmd := exec.CommandContext(ctx, r.SingBox, "run", "-c", cfgPath)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard
	if err := cmd.Start(); err != nil {
		return CodeInternal, Result{Code: CodeInternal}
	}
	defer func() {
		if cmd.Process != nil {
			_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		}
		_, _ = cmd.Process.Wait()
	}()

	if !waitSockReady(ctx, "127.0.0.1", port, r.ReadyWait) {
		if ctx.Err() != nil {
			return CodeProcessTimeout, Result{Code: CodeProcessTimeout}
		}
		return CodeInternal, Result{Code: CodeInternal}
	}

	proxy := fmt.Sprintf("--socks5-hostname 127.0.0.1:%d", port) // §18.9: hostname through proxy
	if stage == "L1" {
		return "", r.curlL1(ctx, curlTimeout, proxy)
	}
	return "", r.curlL2(ctx, curlTimeout, proxy)
}

// buildSingboxConfig enforces the §14.6 config shape and the "probe" tag.
func buildSingboxConfig(outbound json.RawMessage, port int) ([]byte, error) {
	var out map[string]any
	if err := json.Unmarshal(outbound, &out); err != nil {
		return nil, err
	}
	if out["type"] == nil {
		return nil, errors.New("outbound missing type")
	}
	out["tag"] = "probe"
	cfg := map[string]any{
		"log": map[string]any{"level": "error"},
		"inbounds": []any{map[string]any{
			"type": "socks", "listen": "127.0.0.1", "listen_port": port,
		}},
		"outbounds": []any{out},
		"route":     map[string]any{"final": "probe"},
	}
	return json.Marshal(cfg)
}

func waitSockReady(ctx context.Context, host string, port int, wait time.Duration) bool {
	deadline := time.Now().Add(wait)
	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return false
		case <-time.After(50 * time.Millisecond):
		}
		conn, err := net.DialTimeout("tcp", host+":"+strconv.Itoa(port), 300*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return true
		}
	}
	return false
}

func (r *Runner) curlL1(ctx context.Context, timeout time.Duration, proxy string) Result {
	var last Result
	for _, target := range r.ProbeURLs {
		args := []string{"-s", "-o", "/dev/null", "-w", "%{http_code} %{time_total}",
			"--max-time", durSeconds(timeout), proxy, target}
		out, exit := runCurl(ctx, r.Curl, args)
		last = parseL1(out, exit)
		if last.OK {
			return last
		}
		// Fall back to the next target only for target-side problems; a
		// network-level failure means the node itself is unreachable.
		if last.Code != CodeHTTPStatus && last.Code != CodeContentMismatch {
			return last
		}
	}
	return last
}

func (r *Runner) curlL2(ctx context.Context, timeout time.Duration, proxy string) Result {
	url := fmt.Sprintf(r.L2URL, r.L2Bytes)
	args := []string{"-s", "-o", "/dev/null", "-w", "%{speed_download}",
		"--max-time", durSeconds(timeout), proxy, url}
	out, exit := runCurl(ctx, r.Curl, args)
	return parseL2(out, exit, r.L2Bytes)
}

func durSeconds(d time.Duration) string { return strconv.Itoa(int(d.Seconds())) }

// parseL1 parses "code time_total" curl output; exit code mapping per §18.10.
func parseL1(out string, exit int) Result {
	if exit != 0 {
		return Result{Code: curlExitToCode(exit, false)}
	}
	fields := strings.Fields(strings.TrimSpace(out))
	if len(fields) != 2 {
		return Result{Code: CodeContentMismatch}
	}
	code, err := strconv.Atoi(fields[0])
	if err != nil {
		return Result{Code: CodeContentMismatch}
	}
	sec, err := strconv.ParseFloat(fields[1], 64)
	if err != nil {
		return Result{Code: CodeInternal}
	}
	if code == 204 {
		return Result{OK: true, LatencyMs: int(sec * 1000)}
	}
	return Result{Code: CodeHTTPStatus}
}

// parseL2 parses "%{speed_download}" (bytes/sec) into mbps (§14.6).
func parseL2(out string, exit int, bytes int64) Result {
	if exit != 0 {
		return Result{Code: curlExitToCode(exit, true)}
	}
	fields := strings.Fields(strings.TrimSpace(out))
	if len(fields) == 0 {
		return Result{Code: CodeContentMismatch}
	}
	bps, err := strconv.ParseFloat(fields[0], 64)
	if err != nil || bps <= 0 {
		return Result{Code: CodeContentMismatch}
	}
	mbps := bps * 8 / 1e6
	if mbps < 0.01 {
		return Result{Code: CodeSlow}
	}
	return Result{OK: true, SpeedMbps: mathRound(mbps*100) / 100}
}

func mathRound(v float64) float64 {
	if v < 0 {
		return float64(int(v - 0.5))
	}
	return float64(int(v + 0.5))
}

// curlExitToCode maps curl exit codes to the §16 enum.
// 28 (timeout) → E_DIAL_TIMEOUT on L1, E_SLOW on L2 (§18.10).
func curlExitToCode(exit int, isL2 bool) string {
	switch exit {
	case 28:
		if isL2 {
			return CodeSlow
		}
		return CodeDialTimeout
	case 7, 8:
		return CodeDialRefused
	case 35, 51, 53, 54, 58, 60, 64, 66, 77, 80, 82, 83, 90, 91:
		return CodeTLS
	case 56, 94, 95, 97, 98:
		return CodeProtoHandshake
	case 22:
		return CodeHTTPStatus
	case 4, 5, 6:
		return CodeInternal
	default:
		return CodeInternal
	}
}

// runCurl executes curl and returns trimmed output + exit code.
func runCurl(ctx context.Context, curl string, args []string) (string, int) {
	// split proxy arg (passed pre-joined for readability)
	final := []string{}
	for _, a := range args {
		final = append(final, strings.Fields(a)...)
	}
	var buf bytes.Buffer
	cmd := exec.CommandContext(ctx, curl, final...)
	cmd.Stdout = &buf
	cmd.Stderr = &buf
	err := cmd.Run()
	exit := 0
	if err != nil {
		var ee *exec.ExitError
		if errors.As(err, &ee) {
			exit = ee.ExitCode()
		} else {
			exit = 127
		}
	}
	return buf.String(), exit
}
