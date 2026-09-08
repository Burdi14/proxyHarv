#!/usr/bin/env python3
import json, math
import folium

AFRICA = set('DZ AO BJ BW BF BI BM CM CV CF TD KM CG CD CI DJ EG GQ ER SZ ET GA GM GH GN GW KE LS LR LY MG MW ML MR MU MA MZ NA NE NG RW ST SN SC SL SO ZA SS SD TZ TG TN UG EH ZM ZW'.split())
ASIA = set('AF AM AZ BH BD BT BN KH CN CY GE IN ID IR IL JP JO KZ KW KG LA LB MY MV MN MM NP KP OM PK PS PH QA SA SG KR LK SY TW TJ TH TL TR TM AE UZ VN YE'.split())
EUROPE = set('AL AD AT BY BE BA BG HR CY CZ DK EE FI FR DE GR HU IS IE IT LV LI LT LU MT MD MC ME NL NO PL PT RO RU SM RS SK SI ES SE CH UA GB VA XK'.split())
N_AMERICA = set('AG BS BB BZ CA CR CU DM DO SV GD GT HT HN JM MX NI PA KN LC VC TT US'.split())
S_AMERICA = set('AR BO BR CL CO EC GY PY PE SR UY VE GF'.split())
OCEANIA = set('AU FJ KI MH FM NR NZ PW PG WS SB TK TO TV VU'.split())

CONT_COLOR = {
    'Africa': '#ff6b6b',
    'Asia': '#ffd166',
    'Europe': '#06d6a0',
    'North America': '#118ab2',
    'South America': '#8338ec',
    'Oceania': '#ef476f',
    'Other': '#adb5bd',
}

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

m = folium.Map(location=[20, 10], zoom_start=2, tiles='CartoDB positron', width='100%', height='100%')

def radius_for(n):
    return max(1.5, min(9, 2.2 * math.log10(n + 1)))  # log-scaled, small radii

legend_html = '''
<div style="position:fixed;bottom:20px;left:20px;z-index:9999;background:white;padding:10px;border-radius:6px;font:12px sans-serif;box-shadow:0 1px 4px rgba(0,0,0,.3)">
<b>Continent</b><br>
'''
for cname, col in CONT_COLOR.items():
    legend_html += '<span style="display:inline-block;width:12px;height:12px;background:%s;border-radius:50%%;margin-right:5px"></span>%s<br>' % (col, cname)
legend_html += '<hr><b>Circle size</b> = # proxy configs in city'
legend_html += '</div>'
m.get_root().html.add_child(folium.Element(legend_html))

for c in cities:
    cc, lat, lon = c['cc'], c['lat'], c['lon']
    if not lat or not lon:
        continue
    ct = continent(cc)
    col = CONT_COLOR[ct]
    tip = "%s, %s — %d configs / %d hosts" % (c['city'], c['country'], c['configs'], c['hosts'])
    radius = radius_for(c['configs'])
    m.add_child(folium.CircleMarker([lat, lon], radius=radius, color=col,
                                    fill=True, fillColor=col, fillOpacity=0.55,
                                    weight=1, popup=tip, tooltip=tip, base_r=radius))

m.save('proxies_map.html')

zoom_script = '''
<script>
(function() {
  function init() {
    var map = null;
    for (var k in window) {
      if (window[k] && window[k]._leaflet_id !== undefined && typeof window[k].getZoom === 'function') { map = window[k]; break; }
    }
    if (!map) return;
    function rescaleCircles() {
      var f = 0.5 + 0.16 * map.getZoom();
      map.eachLayer(function(layer) {
        if (layer && typeof layer.getRadius === 'function' && layer.options && layer.options.radius) {
          if (!layer._base_r) layer._base_r = layer.getRadius();
          layer.setRadius(Math.max(2, Math.min(18, layer._base_r * f)));
        }
      });
    }
    rescaleCircles();
    map.on('zoomend', rescaleCircles);
  }
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();
</script>
'''

html = open('proxies_map.html').read()
html = html.replace('</body>', zoom_script + '</body>')
open('proxies_map.html', 'w').write(html)
print('saved proxies_map.html with', len(cities), 'city markers (zoom-scaling radii)')