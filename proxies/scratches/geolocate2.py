#!/usr/bin/env python3
import json, socket, time, concurrent.futures as cf
import requests

data = json.load(open('nodes.json'))
hosts = data['hosts']
host_idx = {h['host']: h for h in hosts}

prev = json.load(open('to_geo.json'))           # [{host, ip, domain, n}]
old_ip = {}
for e in prev:
    old_ip[e['host']] = e['ip']

geo = json.load(open('geo_pass1.json'))

# hosts already resolved previously
known = {}
for h in hosts:
    if h['host'] in old_ip:
        known[h['host']] = old_ip[h['host']]
new_hosts = [h for h in hosts if h['host'] not in known]
print('already resolved hosts:', len(known), '| new hosts to resolve:', len(new_hosts))

def resolve(h):
    if ':' in h or h.replace('.', '').isdigit():
        return h
    try:
        infos = socket.getaddrinfo(h, None)
        for info in infos:
            ip = info[4][0]
            if not ip.startswith('127.') and ':' not in ip:
                return ip
        return None
    except Exception:
        return None

if new_hosts:
    with cf.ThreadPoolExecutor(max_workers=64) as ex:
        futs = {ex.submit(resolve, h['host']): h for h in new_hosts}
        for f in cf.as_completed(futs):
            ip = f.result()
            if ip:
                known[futs[f]['host']] = ip
            else:
                known[futs[f]['host']] = None

ip_to_hosts = {}
for h, ip in known.items():
    if ip:
        ip_to_hosts.setdefault(ip, []).append(h)

new_ips = [ip for ip in ip_to_hosts if ip not in geo]
print('unique IPs:', len(ip_to_hosts), '| already geolocated:', len(ip_to_hosts) - len(new_ips), '| new to geolocate:', len(new_ips))

B = 100
n_ok = 0
for i in range(0, len(new_ips), B):
    batch = new_ips[i:i + B]
    for attempt in range(4):
        try:
            r = requests.post('http://ip-api.com/batch', json=batch,
                              headers={'Content-Type': 'application/json'}, timeout=30)
            if r.status_code == 200:
                arr = r.json()
                for ip, g in zip(batch, arr):
                    geo[ip] = g if isinstance(g, dict) else None
                    if g and g.get('status') == 'success':
                        n_ok += 1
                break
        except Exception:
            pass
        time.sleep(3 + attempt * 2)
    time.sleep(1.3)
print('appended geolocation for', n_ok, 'new IPs')

json.dump(geo, open('geo_merged.json', 'w'))

from collections import defaultdict
city = defaultdict(lambda: {'host_i': 0, 'n': 0, 'cc': '', 'nm': '', 'lat': 0.0, 'lon': 0.0})
for ip, hosts_ip in ip_to_hosts.items():
    g = geo.get(ip)
    if not g or g.get('status') != 'success':
        continue
    cityname = g.get('city') or ''
    cc = g.get('countryCode') or '?'
    key = (cc, cityname)
    c = city[key]
    for h in hosts_ip:
        hh = host_idx.get(h)
        if hh:
            c['n'] += hh['n']
    c['cc'] = cc
    c['nm'] = g.get('country') or cc
    c['lat'] = g.get('lat', 0)
    c['lon'] = g.get('lon', 0)

rows = sorted([{'cc': c['cc'], 'country': c['nm'], 'city': k[1], 'lat': c['lat'],
                'lon': c['lon'], 'hosts': 1, 'configs': c['n']} for k, c in city.items()],
              key=lambda x: -x['configs'])
json.dump({'geolocated_ip_hosts': len(ip_to_hosts), 'cities': rows}, open('cities.json', 'w'), indent=2)
print('cities now:', len(rows), 'total configs in cities:', sum(r['configs'] for r in rows))