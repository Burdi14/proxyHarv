// Package parse converts proxy share-links (vless/vmess/ss/trojan/hysteria2/
// socks/anytls/tuic/hysteria) into deduplicated node records and ready-to-run
// sing-box outbound JSON, per ARCHITECTURE.md §14.2.
package parse

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Node is the parsed share-link record. JSON tags follow §14.3 RAW batches.
type Node struct {
	URI       string          `json:"uri"`
	URIHash   string          `json:"uri_hash"`
	Protocol  string          `json:"protocol"`
	Transport string          `json:"transport"`
	Host      string          `json:"host"`
	Port      int             `json:"port"`
	Name      string          `json:"-"`
	Outbound  json.RawMessage `json:"-"`
}

// ErrNotShareLink marks lines that are not recognized proxy links.
var ErrNotShareLink = errors.New("not a share link")

var schemes = map[string]string{
	"vless": "vless", "vmess": "vmess", "ss": "ss", "shadowsocks": "ss",
	"trojan": "trojan", "hysteria2": "hysteria2", "hy2": "hysteria2",
	"socks": "socks", "socks5": "socks", "socks4": "socks",
	"anytls": "anytls", "tuic": "tuic", "hysteria": "hysteria",
}

// stripDefaults: ports dropped in canonical form — ONLY these (§18.6).
var stripDefaults = map[string]int{"http": 80, "https": 443, "socks": 1080}

// fallbackPorts: used by Parse when the uri carries no explicit port.
var fallbackPorts = map[string]int{
	"ss": 8388, "vmess": 443, "vless": 443, "trojan": 443,
	"hysteria2": 443, "tuic": 443, "hysteria": 443, "anytls": 443,
	"socks": 1080, "socks5": 1080, "socks4": 1080,
}

func trim(s string) string {
	s = strings.TrimPrefix(s, "\ufeff")
	s = strings.TrimPrefix(s, "\ufeff\u00a0")
	return strings.TrimSpace(s)
}

// CanonicalHash returns canonical form and uri_hash per §14.2.
func CanonicalHash(raw string) (canonical, hash string, err error) {
	canonical, err = Canonical(raw)
	if err != nil {
		return "", "", err
	}
	return canonical, Hash(canonical), nil
}

// Hash returns "sha256:"+hex(sha256(canonical)).
func Hash(canonical string) string {
	sum := sha256.Sum256([]byte(canonical))
	return "sha256:" + hex.EncodeToString(sum[:])
}

// Canonical builds the canonical uri form used for dedup (§14.2).
func Canonical(raw string) (string, error) {
	s := trim(raw)
	if s == "" {
		return "", errors.New("empty uri")
	}
	i := strings.Index(s, "://")
	if i < 0 {
		return "", errors.New("missing scheme delimiter")
	}
	scheme := strings.ToLower(s[:i])
	rest := s[i+3:]

	if scheme == "vmess" {
		return canonicalVmess(s, rest)
	}

	// Drop fragment (node name) — §14.2 step 2.
	if j := strings.IndexByte(rest, '#'); j >= 0 {
		rest = rest[:j]
	}
	// Split authority | path?query.
	auth, tail := splitAuthority(rest)
	path, rawQuery := splitPathQuery(tail)

	userinfo, host, port, err := splitAuth(auth)
	if err != nil {
		return "", err
	}
	host = strings.ToLower(host)
	if dp, ok := stripDefaults[scheme]; ok && port != "" && port == strconv.Itoa(dp) {
		port = ""
	}

	q := sortQuery(rawQuery)

	var b strings.Builder
	b.WriteString(scheme)
	b.WriteString("://")
	if userinfo != "" {
		b.WriteString(userinfo)
		b.WriteByte('@')
	}
	b.WriteString(host)
	if port != "" {
		b.WriteByte(':')
		b.WriteString(port)
	}
	b.WriteString(path)
	if q != "" {
		b.WriteByte('?')
		b.WriteString(q)
	}
	return b.String(), nil
}

// splitAuthority returns the authority part and the remainder (path?query).
func splitAuthority(rest string) (auth, tail string) {
	for i := 0; i < len(rest); i++ {
		switch rest[i] {
		case '/', '?':
			return rest[:i], rest[i:]
		}
	}
	return rest, ""
}

func splitPathQuery(tail string) (path, query string) {
	if j := strings.IndexByte(tail, '?'); j >= 0 {
		return tail[:j], tail[j+1:]
	}
	return tail, ""
}

// sortQuery drops empty pairs and sorts raw pairs as strings (§14.2 step 3).
func sortQuery(rawQuery string) string {
	if rawQuery == "" {
		return ""
	}
	parts := strings.Split(rawQuery, "&")
	kept := make([]string, 0, len(parts))
	for _, p := range parts {
		if p != "" {
			kept = append(kept, p)
		}
	}
	sort.Strings(kept)
	return strings.Join(kept, "&")
}

// splitAuth splits "userinfo@host:port"; userinfo = up to the LAST '@',
// host:port split at the LAST ':' (IPv6 kept in brackets) — §18.4.
func splitAuth(auth string) (userinfo, host, port string, err error) {
	userinfo = ""
	rest := auth
	if at := strings.LastIndexByte(auth, '@'); at >= 0 {
		userinfo, rest = auth[:at], auth[at+1:]
	}
	if strings.HasPrefix(rest, "[") {
		end := strings.IndexByte(rest, ']')
		if end < 0 {
			return "", "", "", errors.New("unterminated ipv6 host")
		}
		host = rest[:end+1]
		rest = rest[end+1:]
		if strings.HasPrefix(rest, ":") {
			port = rest[1:]
		} else if rest != "" {
			return "", "", "", fmt.Errorf("junk after ipv6 host: %q", rest)
		}
		return userinfo, host, port, nil
	}
	if c := strings.LastIndexByte(rest, ':'); c >= 0 {
		host, port = rest[:c], rest[c+1:]
	} else {
		host = rest
	}
	if port != "" && !isDigits(port) {
		return "", "", "", fmt.Errorf("invalid port %q", port)
	}
	return userinfo, host, port, nil
}

func isDigits(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

// canonicalVmess implements §14.2 rule 8: base64 (std or urlsafe, padded with
// '='), JSON; canonical = vmess|v|add|port|id|net|path|host|tls|sni|alpn|scy.
// Any decode error → opaque|<raw uri>.
func canonicalVmess(raw, rest string) (string, error) {
	payload := rest
	if j := strings.IndexByte(payload, '#'); j >= 0 {
		payload = payload[:j]
	}
	v, ok := decodeVmessB64(payload)
	if !ok {
		return "opaque|" + raw, nil
	}
	lower := strings.ToLower(v.Add)
	parts := []string{"vmess", v.V, lower, v.Port, v.ID, v.Net, v.Path, v.Host, v.TLS, v.SNI, v.Alpn, v.SCY}
	return strings.Join(parts, "|"), nil
}

type vmessJSON struct {
	V    string `json:"v"`
	Add  string `json:"add"`
	Port string `json:"port"`
	ID   string `json:"id"`
	Aid  string
	Net  string `json:"net"`
	Path string `json:"path"`
	Host string `json:"host"`
	TLS  string `json:"tls"`
	SNI  string `json:"sni"`
	Alpn string `json:"alpn"`
	SCY  string `json:"scy"`
}

// decodeVmessB64 decodes strictly base64-payload vmess (canonical path §14.2
// rule 8: decode error → opaque).
func decodeVmessB64(payload string) (vmessJSON, bool) {
	decoded, ok := b64Decode(payload)
	if !ok {
		return vmessJSON{}, false
	}
	return vmessFromBytes(decoded)
}

// decodeVmessAny accepts base64 OR raw-JSON payloads (real subscriptions ship
// both); canonicalization still treats raw JSON as opaque via decodeVmessB64.
func decodeVmessAny(payload string) (vmessJSON, bool) {
	if v, ok := decodeVmessB64(payload); ok {
		return v, true
	}
	return vmessFromBytes([]byte(payload))
}

func vmessFromBytes(decoded []byte) (vmessJSON, bool) {
	raw := map[string]any{}
	if err := json.Unmarshal(decoded, &raw); err != nil {
		return vmessJSON{}, false
	}
	if _, hasID := raw["id"]; !hasID {
		return vmessJSON{}, false
	}
	get := func(k string) string {
		switch x := raw[k].(type) {
		case string:
			return x
		case float64:
			return strconv.FormatFloat(x, 'f', -1, 64)
		case bool:
			if x {
				return "true"
			}
			return "false"
		}
		return ""
	}
	return vmessJSON{
		V: get("v"), Add: get("add"), Port: get("port"), ID: get("id"),
		Aid: get("aid"), Net: get("net"), Path: get("path"), Host: get("host"),
		TLS: get("tls"), SNI: get("sni"), Alpn: get("alpn"), SCY: get("scy"),
	}, true
}

// b64Decode tries std then urlsafe alphabets, padding to a multiple of 4.
func b64Decode(s string) ([]byte, bool) {
	if s == "" {
		return nil, false
	}
	if len(s)%4 != 0 {
		pad := 4 - len(s)%4
		s += strings.Repeat("=", pad)
	}
	if b, err := base64.StdEncoding.DecodeString(s); err == nil {
		return b, true
	}
	if b, err := base64.URLEncoding.DecodeString(s); err == nil {
		return b, true
	}
	return nil, false
}

// ParseLine parses one subscription line; returns (nil, ErrNotShareLink) for
// lines that are not share-links, (nil, nil) for blank lines.
func ParseLine(line string) (*Node, error) {
	s := trim(line)
	if s == "" {
		return nil, nil
	}
	i := strings.Index(s, "://")
	if i < 0 {
		return nil, ErrNotShareLink
	}
	scheme := strings.ToLower(s[:i])
	proto, ok := schemes[scheme]
	if !ok {
		return nil, ErrNotShareLink
	}
	n, err := parseByProto(scheme, proto, s[i+3:])
	if err != nil {
		return nil, fmt.Errorf("%s: %w", proto, err)
	}
	n.URI = s
	canonical, err := Canonical(s)
	if err != nil {
		return nil, err
	}
	n.URIHash = Hash(canonical)
	return n, nil
}

type parsed struct {
	userinfo string
	host     string // brackets stripped
	port     int
	rawQuery orderedPairs
	fragment string
}

type orderedPairs struct{ pairs [][2]string }

func (o *orderedPairs) get(k string) (string, bool) {
	for _, p := range o.pairs {
		if p[0] == k {
			return p[1], true
		}
	}
	return "", false
}

func (o *orderedPairs) getDecoded(k string) string {
	v, ok := o.get(k)
	if !ok {
		return ""
	}
	if d, err := url.QueryUnescape(v); err == nil {
		return d
	}
	return v
}

func parseCommon(rest string) (*parsed, error) {
	p := &parsed{}
	tail := rest
	if j := strings.IndexByte(tail, '#'); j >= 0 {
		p.fragment, tail = tail[j+1:], tail[:j]
	}
	auth, pathQuery := splitAuthority(tail)
	_, rawQuery := splitPathQuery(pathQuery)
	for _, pair := range strings.Split(rawQuery, "&") {
		if pair == "" {
			continue
		}
		k, v, _ := strings.Cut(pair, "=")
		p.rawQuery.pairs = append(p.rawQuery.pairs, [2]string{k, v})
	}
	userinfo, host, portStr, err := splitAuth(auth)
	if err != nil {
		return nil, err
	}
	p.userinfo = userinfo
	p.host = strings.TrimPrefix(strings.TrimSuffix(host, "]"), "[")
	if portStr == "" {
		// resolved by caller via defaultPorts
	} else if pt, err := strconv.Atoi(portStr); err == nil && pt > 0 && pt < 65536 {
		p.port = pt
	} else {
		return nil, fmt.Errorf("bad port %q", portStr)
	}
	return p, nil
}

func parseByProto(scheme, proto, rest string) (*Node, error) {
	n := &Node{Protocol: proto, Transport: "tcp"}
	var out map[string]any
	var err error
	switch proto {
	case "vless":
		out, err = buildVless(scheme, rest, n)
	case "vmess":
		out, err = buildVmess(rest, n)
	case "ss":
		out, err = buildSS(scheme, rest, n)
	case "trojan":
		out, err = buildTrojan(scheme, rest, n)
	case "hysteria2":
		out, err = buildHysteria2(scheme, rest, n)
	case "socks":
		out, err = buildSocks(scheme, rest, n)
	case "anytls":
		out, err = buildAnyTLS(scheme, rest, n)
	case "tuic":
		out, err = buildTUIC(scheme, rest, n)
	case "hysteria":
		out, err = buildHysteria(scheme, rest, n)
	default:
		return nil, fmt.Errorf("unsupported protocol %q", proto)
	}
	if err != nil {
		return nil, err
	}
	out["tag"] = "probe"
	b, err := json.Marshal(out)
	if err != nil {
		return nil, err
	}
	n.Outbound = b
	if j := strings.IndexByte(rest, '#'); j >= 0 {
		if name, err2 := url.QueryUnescape(rest[j+1:]); err2 == nil {
			n.Name = name
		}
	}
	return n, nil
}

func applyPort(n *Node, scheme string, p *parsed, fallback int) {
	n.Host = p.host
	n.Port = p.port
	if n.Port == 0 {
		if dp, ok := fallbackPorts[scheme]; ok {
			n.Port = dp
		} else {
			n.Port = fallback
		}
	}
}

func buildVless(scheme, rest string, n *Node) (map[string]any, error) {
	p, err := parseCommon(rest)
	if err != nil {
		return nil, err
	}
	applyPort(n, scheme, p, 443)
	out := map[string]any{
		"type": "vless", "server": n.Host, "server_port": n.Port,
		"uuid": p.userinfo,
	}
	if flow, ok := p.rawQuery.get("flow"); ok && flow != "" {
		out["flow"] = flow
	}
	if pe := p.rawQuery.getDecoded("packetEncoding"); pe == "xudp" {
		out["packet_encoding"] = "xudp"
	}
	security := p.rawQuery.getDecoded("security")
	n.Transport = mapTransport(p.rawQuery.getDecoded("type"))
	if security == "tls" || security == "reality" {
		tls := map[string]any{"enabled": true}
		sni := firstNonEmpty(p.rawQuery.getDecoded("sni"), p.rawQuery.getDecoded("host"), n.Host)
		tls["server_name"] = sni
		if insecure(p) {
			tls["insecure"] = true
		}
		if alpn := p.rawQuery.getDecoded("alpn"); alpn != "" {
			tls["alpn"] = strings.Split(alpn, ",")
		}
		if fp := p.rawQuery.getDecoded("fp"); fp != "" {
			tls["utls"] = map[string]any{"enabled": true, "fingerprint": fp}
		}
		if security == "reality" {
			reality := map[string]any{"enabled": true}
			if pbk, ok := p.rawQuery.get("pbk"); ok {
				reality["public_key"] = pbk
			}
			if sid, ok := p.rawQuery.get("sid"); ok {
				reality["short_id"] = sid
			}
			tls["reality"] = reality
		}
		out["tls"] = tls
	}
	if tr := buildTransport(n.Transport, p); tr != nil {
		out["transport"] = tr
	}
	return out, nil
}

func buildVmess(rest string, n *Node) (map[string]any, error) {
	payload := rest
	if j := strings.IndexByte(payload, '#'); j >= 0 {
		payload = payload[:j]
	}
	v, ok := decodeVmessAny(payload)
	if !ok {
		return nil, errors.New("vmess payload is neither base64 nor JSON")
	}
	port, _ := strconv.Atoi(v.Port)
	if v.Add == "" || v.ID == "" || port == 0 {
		return nil, errors.New("vmess missing add/id/port")
	}
	n.Host = v.Add
	n.Port = port
	scy := v.SCY
	if scy == "" {
		scy = "auto"
	}
	out := map[string]any{
		"type": "vmess", "server": v.Add, "server_port": port,
		"uuid": v.ID, "security": scy, "alter_id": atoiOr(v.Aid, 0),
	}
	n.Transport = mapTransport(v.Net)
	if v.TLS == "tls" || strings.HasPrefix(strings.ToLower(v.TLS), "tls") {
		tls := map[string]any{"enabled": true, "server_name": firstNonEmpty(v.SNI, v.Host, v.Add)}
		if v.Alpn != "" {
			tls["alpn"] = strings.Split(v.Alpn, ",")
		}
		out["tls"] = tls
	}
	if tr := buildTransportPlain(n.Transport, v.Path, v.Host, v.Path); tr != nil {
		out["transport"] = tr
	}
	return out, nil
}

func atoiOr(s string, def int) int {
	if v, err := strconv.Atoi(strings.TrimSpace(s)); err == nil {
		return v
	}
	return def
}

func buildSS(scheme, rest string, n *Node) (map[string]any, error) {
	p, err := parseCommon(rest)
	if err != nil {
		return nil, err
	}
	method, password := "", ""
	if p.userinfo != "" {
		method, password = splitUserInfoSecret(p.userinfo)
		applyPort(n, scheme, p, 8388)
	} else {
		// legacy: whole opaque part is base64(method:password@host:port)
		opaque := rest
		if j := strings.IndexByte(opaque, '#'); j >= 0 {
			opaque = opaque[:j]
		}
		decoded, ok := b64Decode(opaque)
		if !ok {
			return nil, errors.New("ss legacy payload not base64")
		}
		s := string(decoded)
		at := strings.LastIndexByte(s, '@')
		if at < 0 {
			return nil, errors.New("ss legacy payload missing host")
		}
		method, password = splitUserInfoSecret(s[:at])
		_, host, portStr, err2 := splitAuth(s[at+1:])
		if err2 != nil {
			return nil, err2
		}
		p.host = strings.TrimPrefix(strings.TrimSuffix(host, "]"), "[")
		p.port = atoiOr(portStr, 0)
		applyPort(n, scheme, p, 8388)
	}
	if method == "" || password == "" {
		return nil, errors.New("ss missing method/password")
	}
	return map[string]any{
		"type": "shadowsocks", "server": n.Host, "server_port": n.Port,
		"method": method, "password": password,
	}, nil
}

// splitUserInfoSecret handles both base64(method:password) and plain forms.
func splitUserInfoSecret(ui string) (method, password string) {
	if dec, ok := b64Decode(ui); ok {
		if m, p, found := strings.Cut(string(dec), ":"); found && m != "" {
			return m, p
		}
	}
	if m, p, found := strings.Cut(ui, ":"); found {
		return m, p
	}
	return "", ui
}

func buildTrojan(scheme, rest string, n *Node) (map[string]any, error) {
	p, err := parseCommon(rest)
	if err != nil {
		return nil, err
	}
	applyPort(n, scheme, p, 443)
	out := map[string]any{
		"type": "trojan", "server": n.Host, "server_port": n.Port,
		"password": unescapeOrRaw(p.userinfo),
	}
	n.Transport = mapTransport(p.rawQuery.getDecoded("type"))
	if p.rawQuery.getDecoded("security") != "none" {
		tls := map[string]any{"enabled": true,
			"server_name": firstNonEmpty(p.rawQuery.getDecoded("sni"), p.rawQuery.getDecoded("host"), n.Host)}
		if insecure(p) {
			tls["insecure"] = true
		}
		if alpn := p.rawQuery.getDecoded("alpn"); alpn != "" {
			tls["alpn"] = strings.Split(alpn, ",")
		}
		if fp := p.rawQuery.getDecoded("fp"); fp != "" {
			tls["utls"] = map[string]any{"enabled": true, "fingerprint": fp}
		}
		out["tls"] = tls
	}
	if tr := buildTransport(n.Transport, p); tr != nil {
		out["transport"] = tr
	}
	return out, nil
}

func buildHysteria2(scheme, rest string, n *Node) (map[string]any, error) {
	p, err := parseCommon(rest)
	if err != nil {
		return nil, err
	}
	applyPort(n, scheme, p, 443)
	n.Transport = "quic"
	out := map[string]any{
		"type": "hysteria2", "server": n.Host, "server_port": n.Port,
		"password": unescapeOrRaw(p.userinfo),
	}
	tls := map[string]any{"enabled": true,
		"server_name": firstNonEmpty(p.rawQuery.getDecoded("sni"), n.Host)}
	if insecure(p) {
		tls["insecure"] = true
	}
	out["tls"] = tls
	if obfsPW := p.rawQuery.getDecoded("obfs-password"); obfsPW != "" {
		out["obfs"] = map[string]any{"type": "salamander", "password": obfsPW}
	}
	if up := atoiOr(p.rawQuery.getDecoded("upmbps"), 0); up > 0 {
		out["up_mbps"] = up
	}
	if down := atoiOr(p.rawQuery.getDecoded("downmbps"), 0); down > 0 {
		out["down_mbps"] = down
	}
	return out, nil
}

func buildSocks(scheme, rest string, n *Node) (map[string]any, error) {
	p, err := parseCommon(rest)
	if err != nil {
		return nil, err
	}
	applyPort(n, scheme, p, 1080)
	out := map[string]any{
		"type": "socks", "server": n.Host, "server_port": n.Port,
		"version": socksVersion(scheme),
	}
	if p.userinfo != "" {
		ui := unescapeOrRaw(p.userinfo)
		if u, pw, found := strings.Cut(ui, ":"); found {
			out["username"] = u
			out["password"] = pw
		} else {
			out["username"] = ui
		}
	}
	return out, nil
}

func socksVersion(scheme string) string {
	switch scheme {
	case "socks4":
		return "4"
	default:
		return "5"
	}
}

func buildAnyTLS(scheme, rest string, n *Node) (map[string]any, error) {
	p, err := parseCommon(rest)
	if err != nil {
		return nil, err
	}
	applyPort(n, scheme, p, 443)
	out := map[string]any{
		"type": "anytls", "server": n.Host, "server_port": n.Port,
		"password": unescapeOrRaw(p.userinfo),
	}
	if p.rawQuery.getDecoded("security") != "none" {
		tls := map[string]any{"enabled": true,
			"server_name": firstNonEmpty(p.rawQuery.getDecoded("sni"), n.Host)}
		if insecure(p) {
			tls["insecure"] = true
		}
		out["tls"] = tls
	}
	return out, nil
}

func buildTUIC(scheme, rest string, n *Node) (map[string]any, error) {
	p, err := parseCommon(rest)
	if err != nil {
		return nil, err
	}
	applyPort(n, scheme, p, 443)
	n.Transport = "quic"
	uuid := firstNonEmpty(p.rawQuery.getDecoded("uuid"), userOf(p.userinfo))
	password := firstNonEmpty(p.rawQuery.getDecoded("password"), passOf(p.userinfo))
	if uuid == "" {
		return nil, errors.New("tuic missing uuid")
	}
	cc := firstNonEmpty(p.rawQuery.getDecoded("congestion_control"), "bbr")
	out := map[string]any{
		"type": "tuic", "server": n.Host, "server_port": n.Port,
		"uuid": uuid, "password": password, "congestion_control": cc,
		"tls": map[string]any{"enabled": true, "alpn": []string{"h3"},
			"server_name": firstNonEmpty(p.rawQuery.getDecoded("sni"), n.Host)},
	}
	if insecure(p) {
		out["tls"].(map[string]any)["insecure"] = true
	}
	return out, nil
}

func buildHysteria(scheme, rest string, n *Node) (map[string]any, error) {
	p, err := parseCommon(rest)
	if err != nil {
		return nil, err
	}
	applyPort(n, scheme, p, 443)
	n.Transport = "quic"
	auth := firstNonEmpty(p.rawQuery.getDecoded("auth"), unescapeOrRaw(p.userinfo))
	out := map[string]any{
		"type": "hysteria", "server": n.Host, "server_port": n.Port,
		"auth": auth, "up_mbps": atoiOr(p.rawQuery.getDecoded("upmbps"), 100),
		"down_mbps": atoiOr(p.rawQuery.getDecoded("downmbps"), 100),
		"tls": map[string]any{"enabled": true, "alpn": []string{"h3"},
			"server_name": firstNonEmpty(p.rawQuery.getDecoded("peer"), p.rawQuery.getDecoded("sni"), n.Host)},
	}
	if insecure(p) {
		out["tls"].(map[string]any)["insecure"] = true
	}
	return out, nil
}

func userOf(ui string) string {
	u, _, _ := strings.Cut(ui, ":")
	return u
}

func passOf(ui string) string {
	_, p, found := strings.Cut(ui, ":")
	if !found {
		return ""
	}
	return p
}

// buildTransport maps the uri "type"/"net" param to a sing-box transport
// block. path is kept raw (percent-encoded as-is) per §18.5.
func buildTransport(transport string, p *parsed) map[string]any {
	path, _ := p.rawQuery.get("path")
	hostHeader, _ := p.rawQuery.get("host")
	serviceName, _ := p.rawQuery.get("serviceName")
	return buildTransportPlain(transport, path, hostHeader, serviceName)
}

func buildTransportPlain(transport, path, hostHeader, serviceName string) map[string]any {
	switch transport {
	case "ws":
		t := map[string]any{"type": "ws"}
		if path != "" {
			t["path"] = path
		}
		if hostHeader != "" {
			t["headers"] = map[string]any{"Host": hostHeader}
		}
		return t
	case "grpc":
		t := map[string]any{"type": "grpc"}
		if serviceName != "" {
			t["service_name"] = serviceName
		}
		return t
	case "httpupgrade":
		t := map[string]any{"type": "httpupgrade"}
		if path != "" {
			t["path"] = path
		}
		if hostHeader != "" {
			t["host"] = hostHeader
		}
		return t
	case "xhttp", "splithttp":
		t := map[string]any{"type": "xhttp"}
		if path != "" {
			t["path"] = path
		}
		if hostHeader != "" {
			t["host"] = hostHeader
		}
		return t
	case "http", "h2":
		t := map[string]any{"type": "http"}
		if path != "" {
			t["path"] = path
		}
		if hostHeader != "" {
			t["host"] = []string{hostHeader}
		}
		return t
	default: // tcp, raw, none
		return nil
	}
}

func mapTransport(t string) string {
	switch strings.ToLower(t) {
	case "ws", "websocket":
		return "ws"
	case "grpc":
		return "grpc"
	case "quic":
		return "quic"
	case "httpupgrade":
		return "httpupgrade"
	case "xhttp", "splithttp":
		return "xhttp"
	case "http", "h2", "http2":
		return "http"
	case "kcp":
		return "kcp"
	default:
		return "tcp"
	}
}

func insecure(p *parsed) bool {
	v, _ := p.rawQuery.get("insecure")
	v2, _ := p.rawQuery.get("allowInsecure")
	return v == "1" || v == "true" || v2 == "1" || v2 == "true"
}

func unescapeOrRaw(s string) string {
	if u, err := url.QueryUnescape(s); err == nil {
		return u
	}
	return s
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}

// ParseMany parses a whole subscription body (plain or base64) into nodes.
func ParseMany(body []byte) []*Node {
	nodes := []*Node{}
	seen := map[string]bool{}
	add := func(line string) {
		n, err := ParseLine(line)
		if err != nil || n == nil || seen[n.URIHash] {
			return
		}
		seen[n.URIHash] = true
		nodes = append(nodes, n)
	}
	if isProbablyBase64(body) {
		if dec, ok := b64Decode(string(body)); ok {
			body = dec
		}
	}
	for _, line := range strings.Split(string(body), "\n") {
		line = strings.TrimRight(line, "\r")
		add(line)
	}
	return nodes
}

func isProbablyBase64(body []byte) bool {
	s := strings.TrimSpace(string(body))
	if s == "" || strings.Contains(s, "://") {
		return false
	}
	for _, r := range s {
		switch {
		case r >= 'A' && r <= 'Z', r >= 'a' && r <= 'z', r >= '0' && r <= '9',
			r == '+', r == '/', r == '-', r == '_', r == '=', r == '\n', r == '\r':
		default:
			return false
		}
	}
	return true
}

// ShareName builds the subscription display name: {CC}-{City}-{latency}ms-{class}.
func ShareName(cc, city string, latencyMs int, class string) string {
	return fmt.Sprintf("%s-%s-%dms-%s", cc, city, latencyMs, class)
}

// NowRFC3339 is a tiny helper centralizing the §14.1 time format.
func NowRFC3339() string { return time.Now().UTC().Format(time.RFC3339) }
