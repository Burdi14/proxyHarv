#!/usr/bin/env python3
import base64, json, re, sys, time, urllib.parse as up
import requests

LINKS = [l.strip() for l in open('subs.txt') if l.strip().startswith('https://') and 't.me' not in l]

UA = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'}

proto_re = re.compile(r'(vless|vmess|trojan|ss|ssr|hysteria2|hy2|tuic|socks|http)\:\/\/')

def decode_b64(s):
    for pad in ('', '=', '=='):
        try:
            return base64.b64decode(s + pad).decode('utf-8', errors='ignore')
        except Exception:
            continue
    return None

def host_port_from(params, netloc):
    # netloc may be host:port or user:pass@host:port or host  (hysteria2://pass@host:port)
    if '@' in netloc:
        netloc = netloc.split('@')[-1]
    if netloc.startswith('['):
        m = re.match(r'\[([^\]]+)\](?::(\d+))?', netloc)
        if m:
            return m.group(1), (m.group(2) or ''), False
    host, _, port = netloc.partition(':')
    return host, port, False

def parse_line(line):
    line = line.strip()
    if line.startswith(('vless://','vmess://','trojan://','ss://','ssr://','hysteria2://','hy2://','tuic://','socks://','socks5://','socks4://','http://','https://')):
        m = proto_re.match(line)
        proto = m.group(1) if m else '?'
        try:
            u = up.urlparse(line)
        except ValueError:
            return None
        host, port, _ = host_port_from(None, u.netloc)
        return proto, host.strip('[]').strip(), port
    # plain host:port (classic http/socks list lines, may have trailing junk)
    m2 = re.match(r'^([0-9A-Za-z.\-]+)\s*[:\s]\s*(\d{1,5})(?:\s|$)', line)
    if m2:
        return 'ip:port', m2.group(1).strip(), m2.group(2)
    return None

def parse_clash_yaml(txt):
    out = []
    try:
        import yaml
        data = yaml.safe_load(txt)
    except Exception:
        return out
    if not isinstance(data, dict):
        return out
    for grp in data.get('proxies', []) or []:
        if not isinstance(grp, dict):
            continue
        srv = grp.get('server')
        if not srv:
            continue
        out.append((str(grp.get('type', '?')).lower(), str(srv), str(grp.get('port', ''))))
    return out

def process_text(txt):
    nodes = []
    # plain lines
    for ln in txt.splitlines():
        p = parse_line(ln)
        if p:
            nodes.append(p)
    # base64 whole-body
    cleaned = txt.strip()
    if cleaned and not any(k in cleaned[:50] for k in ('vless://','vmess://','trojan://','ss://','hysteria2://')):
        dec = decode_b64(cleaned)
        if dec:
            for ln in dec.splitlines():
                p = parse_line(ln)
                if p:
                    nodes.append(p)
    # clash yaml
    if 'proxies:' in txt:
        nodes.extend(parse_clash_yaml(txt))
    # sing-box / plain json list with server fields
    if txt.lstrip().startswith('[') or txt.lstrip().startswith('{'):
        try:
            jd = json.loads(txt)
            arr = jd if isinstance(jd, list) else (jd.get('servers') or jd.get('proxies') or [])
            for it in arr:
                if isinstance(it, dict):
                    srv = it.get('server') or it.get('address') or it.get('host')
                    if srv:
                        nodes.append((str(it.get('type', it.get('protocol', '?'))).lower(), str(srv), str(it.get('port', ''))))
        except Exception:
            pass
    return nodes

agg = {}  # host -> {proto:set, ports:set, count, src:set}
src_of = {}

for url in LINKS:
    try:
        r = requests.get(url, headers=UA, timeout=40)
        r.raise_for_status()
        txt = r.text
    except Exception as e:
        print('DL-FAIL', url, str(e)[:60])
        continue
    nodes = process_text(txt)
    print('DL-OK  %5d nodes  %s' % (len(nodes), url))
    for proto, host, port in nodes:
        if not host:
            continue
        h2 = host.lower()
        e = agg.setdefault(h2, {'proto': set(), 'port': set(), 'n': 0, 'src': set()})
        e['proto'].add(proto)
        e['port'].add(port)
        e['n'] += 1
        e['src'].add(url)

out = []
for h, e in agg.items():
    out.append({'host': h, 'proto': sorted(e['proto']), 'ports': sorted(e['port']),
                'n': e['n'], 'srcs': len(e['src'])})
out.sort(key=lambda x: -x['n'])
json.dump({'total_hosts': len(out), 'total_configs': sum(o['n'] for o in out),
           'hosts': out}, open('nodes.json', 'w'), indent=1)
print('TOTAL unique hosts:', len(out), 'total config lines:', sum(o['n'] for o in out))