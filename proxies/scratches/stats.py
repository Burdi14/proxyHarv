#!/usr/bin/env python3
import json

AFRICA = set('DZ AO BJ BW BF BI CM CV CF TD KM CG CD CI DJ EG GQ ER SZ ET GA GM GH GN GW KE LS LR LY MG MW ML MR MU MA MZ NA NE NG RW ST SN SC SL SO ZA SS SD TZ TG TN UG EH ZM ZW'.split())
ASIA = set('AF AM AZ BH BD BT BN KH CN CY GE IN ID IR IQ IL JP JO KZ KW KG LA LB MY MV MN MM NP KP OM PK PS PH QA SA SG KR LK SY TW TJ TH TL TR TM AE UZ VN YE'.split())
EUROPE = set('AL AD AT BY BE BA BG HR CY CZ DK EE FI FR DE GR HU IS IE IT LV LI LT LU MT MD MC ME NL NO PL PT RO RU SM RS SK SI ES SE CH UA GB VA XK'.split())
N_AMERICA = set('AG BS BB BZ CA CR CU DM DO SV GD GT HT HN JM MX NI PA KN LC VC TT US'.split())
S_AMERICA = set('AR BO BR CL CO EC GY PY PE SR UY VE GF'.split())
OCEANIA = set('AU FJ KI MH FM NR NZ PW PG WS SB TK TO TV VU'.split())

def continent(cc):
    cc = cc.upper()
    for name, s in (('Africa', AFRICA), ('Asia', ASIA), ('Europe', EUROPE),
                    ('North America', N_AMERICA), ('South America', S_AMERICA),
                    ('Oceania', OCEANIA)):
        if cc in s:
            return name
    return 'Other'

data = json.load(open('cities.json'))
cities = data['cities']
total_configs = sum(c['configs'] for c in cities)
total_hosts = sum(c['hosts'] for c in cities)

from collections import defaultdict
cont = defaultdict(lambda: {'configs': 0, 'hosts': 0, 'countries': set(), 'cities': 0})
for c in cities:
    ct = continent(c['cc'])
    cont[ct]['configs'] += c['configs']
    cont[ct]['hosts'] += c['hosts']
    cont[ct]['countries'].add(c['cc'])
    cont[ct]['cities'] += 1

print('cities known:', len(cities), ' total configs:', total_configs, ' total hosts:', total_hosts)
print()
print('%-15s %10s %8s %6s' % ('CONTINENT', 'CONFIGS', 'CITIES', 'COUNTRIES'))
print('-' * 42)
for name, d in sorted(cont.items(), key=lambda kv: -kv[1]['configs']):
    print('%-15s %10d %8d %6d' % (name, d['configs'], d['cities'], len(d['countries'])))
print('-' * 42)
for name, d in sorted(cont.items(), key=lambda kv: -kv[1]['configs']):
    print('%-15s %6.1f%% of configs' % (name, 100 * d['configs'] / max(1, total_configs)))

def to_filename(name):
    return name.lower().replace(' ', '_')

with open('continent_stats.json', 'w') as f:
    json.dump({name: {'configs': d['configs'], 'hosts': d['hosts'],
                      'cities': d['cities'], 'countries': sorted(d['countries'])}
               for name, d in cont.items()}, f, indent=1)