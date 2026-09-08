// Package logx implements the §16 logging convention: one JSON line per
// event to stdout, fields ts/lvl/cmp/msg first, snake_case extras after.
// Full proxy uris must never be logged (server secrets) — log node_id only.
package logx

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sync"
	"time"
)

type Logger struct {
	cmp string
	mu  *sync.Mutex
	w   io.Writer
}

// New returns a logger for the given component name (cmp).
func New(cmp string) *Logger {
	return &Logger{cmp: cmp, mu: &sync.Mutex{}, w: os.Stdout}
}

func (l *Logger) log(lvl, msg string, kv ...any) {
	rec := make(map[string]any, 4+len(kv)/2)
	rec["ts"] = time.Now().UTC().Format(time.RFC3339Nano)
	rec["lvl"] = lvl
	rec["cmp"] = l.cmp
	rec["msg"] = msg
	for i := 0; i+1 < len(kv); i += 2 {
		k, ok := kv[i].(string)
		if !ok {
			k = fmt.Sprint(kv[i])
		}
		rec[k] = kv[i+1]
	}
	b, err := json.Marshal(rec)
	if err != nil {
		b = []byte(fmt.Sprintf(`{"ts":%q,"lvl":"error","cmp":%q,"msg":"log marshal failed"}`,
			time.Now().UTC().Format(time.RFC3339Nano), l.cmp))
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	_, _ = l.w.Write(append(b, '\n'))
}

func (l *Logger) Info(msg string, kv ...any)  { l.log("info", msg, kv...) }
func (l *Logger) Warn(msg string, kv ...any)  { l.log("warn", msg, kv...) }
func (l *Logger) Error(msg string, kv ...any) { l.log("error", msg, kv...) }
func (l *Logger) Debug(msg string, kv ...any) { l.log("debug", msg, kv...) }
