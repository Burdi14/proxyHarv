package probe

import (
	"context"
	"errors"
	"fmt"
	"net"
	"syscall"
	"time"
)

// L0 runs the cheap TCP prefilter: dial host:port with a timeout (default 3s,
// §17). Only meaningful for TCP-family transports (§4).
func L0(ctx context.Context, host string, port int, timeout time.Duration) Result {
	if timeout <= 0 {
		timeout = 3 * time.Second
	}
	host = stripBrackets(host)
	d := net.Dialer{Timeout: timeout}
	ctx, cancel := context.WithTimeout(ctx, timeout+500*time.Millisecond)
	defer cancel()
	start := time.Now()
	conn, err := d.DialContext(ctx, "tcp", net.JoinHostPort(host, fmt.Sprintf("%d", port)))
	latency := int(time.Since(start).Milliseconds())
	if err == nil {
		_ = conn.Close()
		return Result{OK: true, LatencyMs: latency}
	}
	return Result{Code: classifyDialError(err)}
}

func classifyDialError(err error) string {
	var nerr net.Error
	if errors.As(err, &nerr) && nerr.Timeout() {
		return CodeDialTimeout
	}
	if errors.Is(err, syscall.ECONNREFUSED) {
		return CodeDialRefused
	}
	return CodeInternal
}

func stripBrackets(host string) string {
	if len(host) > 1 && host[0] == '[' && host[len(host)-1] == ']' {
		return host[1 : len(host)-1]
	}
	return host
}
