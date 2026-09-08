// Package probe implements the three-stage check pyramid (§4):
//
//	L0 prefilter — plain TCP dial (no sing-box)
//	L1 probe     — full protocol via external sing-box process + curl
//	L2 speedtest — download N MB through the tunnel
//
// sing-box is executed as an external binary (GPL, never linked — §5).
package probe

import "errors"

// Error codes — the closed enum from §16.
const (
	CodeDialTimeout     = "E_DIAL_TIMEOUT"
	CodeDialRefused     = "E_DIAL_REFUSED"
	CodeTLS             = "E_TLS"
	CodeProtoHandshake  = "E_PROTO_HANDSHAKE"
	CodeHTTPStatus      = "E_HTTP_STATUS"
	CodeContentMismatch = "E_CONTENT_MISMATCH"
	CodeSlow            = "E_SLOW"
	CodeProcessTimeout  = "E_PROCESS_TIMEOUT"
	CodeInternal        = "E_INTERNAL"
)

// ErrSkip marks nodes that cannot be probed at this stage.
var ErrSkip = errors.New("probe: stage not applicable")

// Result is one probe outcome for one node.
type Result struct {
	OK        bool
	LatencyMs int
	SpeedMbps float64
	Code      string // empty iff OK
}
