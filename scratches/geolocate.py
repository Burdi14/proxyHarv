#!/usr/bin/env python3
import json, socket, time, concurrent.futures as cf
import requests

data = json.load(open('nodes.json'))
hosts = data['hosts']

def resolve(h):
    if ':' in h or h.replace('.','').isdigit():
        return h, None
    try:
        infos = socket.getaddrinfo(h, None)
        for info in infos:
            ip = info[4][0]
            if not ip.startswith('127.') and ':' not in ip:
                return ip, h
        return None, h
    except Exception:
        return None, h

resolved = {}
with cf.ThreadPoolExecutor(max_workers=64) as ex:
    futs = {ex.submit(resolve, h['host']): h for h in hosts}
    n = 0
    for f in cf.as_completed(futs):
        ip, dom = f.result()
        h = futs[f]['host']
        if ip:
            resolved[h] = (ip, dom)
        n += 1
print('resolved unique hosts:', len(resolved), '/', len(hosts))
json.dump([{'host': k, 'ip': v[0], 'domain': v[1],
            'n': next(x['n'] for x in hosts if x['host'] == k)}
           for k, v in resolved.items()], open('to_geo.json', 'w'))

ip_to_hosts = {}
for h, (ip, dom) in resolved.items():
    ip_to_hosts.setdefault(ip, []).append(h)

ips = list(ip_to_hosts.keys())
print('unique IPs to geolocate:', len(ips))

geo = {}
B = 100
for i in range(0, len(ips), B):
    batch = ips[i:i+B]
    for attempt in range(4):
        try:
            r = requests.post('http://ip-api.com/batch', json=batch,
                              headers={'Content-Type': 'application/json'}, timeout=30)
            if r.status_code == 200:
                arr = r.json()
                for ip, g in zip(batch, arr):
                    geo[ip] = g if isinstance(g, dict) else None
                break
        except Exception:
            pass
        time.sleep(3 + attempt * 2)
    time.sleep(1.35)
    if (i // B) % 25 == 0:
        print('geo progress', (i // B) + 1, 'batches')

print('geolocated:', len([1 for g in geo.values() if g and g.get('status') == 'success']))
json.dump(geo, open('geo.json', 'w'))

# aggregate by city
from collections import defaultdict
city = defaultdict(lambda: {'n': 0, 'host_n': 0, 'hosts': set(), 'cc': '', 'nm': '', 'lat': 0.0, 'lon': 0.0})
host_idx = {h['host']: h for h in hosts}
ok = 0
for ip, hosts_ip in ip_to_hosts.items():
    g = geo.get(ip)
    if not g or g.get('status') != 'success':
        continue
    ok += 1
    cityname = g.get('city') or ''
    cc = g.get('countryCode') or '?'
    key = (cc, cityname)
    c = city[key]
    for h in hosts_ip:
        hh = host_idx.get(h)
        if hh:
            c['host_n'] += hh['n']
        c['hosts'].add(h)
    c['cc'] = cc
    c['nm'] = g.get('country') or cc
    c['lat'] = g.get('lat', 0)
    c['lon'] = g.get('lon', 0)

rows = sorted([{'cc': c['cc'], 'country': c['nm'], 'city': k[1], 'lat': c['lat'],
                'lon': c['lon'], 'hosts': len(c['hosts']), 'configs': c['host_n']}
               for k, c in city.items()], key=lambda x: -x['configs'])
json.dump({'geolocated_ip_hosts': ok, 'cities': rows}, open('cities.json', 'w'), indent=2)
print('cities with geolocated proxies:', len(rows))
print('--- top 25 cities by config count ---')
for r in rows[:25]:
    print('%6d  %4s  %s' % (r['configs'], r['cc'], r['city']))