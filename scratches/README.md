# scratches — historical map pipeline (deprecated)

These files were generated/used while building the interactive proxy map.
They are kept for reference and are **not** part of the implementation (see
`../ARCHITECTURE.md`). The Python pipeline is superseded by the Go project.

## Pipeline (order)

```
download.py         # pull subs.txt links → nodes.json (unique hosts + configs)
geolocate.py / geolocate2.py / geolocate_final.py
                    # geolocate hosts → geo_final.json, cities.json
stats.py            # continent aggregation → continent_stats.json
map.py              # build proxies_map.html (leaflet)
```

## Artifacts

- `nodes.json`          — 49,704 unique hosts / 244,665 config lines
- `geo_final.json`      — per-host geolocation
- `cities.json`         — per-city aggregates (1,973 cities, zoom-scaling radii)
- `continent_stats.json`— per-continent configs/cities/countries
- `proxies_map.html`    — final map (leaflet, 1,973 city markers)
- `map_preview.png`     — static preview of the map
- `subs.txt`            — 179 subscription links used as sources
- `info.md`             — provider documentation & Update Log
- `candidates.txt`      — ancient scratch notes (ignored)

## venv

The Python virtualenv was removed (machine-specific). To re-run:

```
python3 -m venv .venv
.venv/bin/pip install requests pyyaml folium pycountry
```