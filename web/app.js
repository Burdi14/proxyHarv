/* ProxyFarm frontend: Leaflet map of verified proxy cities (§8). */
"use strict";

const state = { class: "rf", proto: "", cities: [], selectedCity: null };

const map = L.map("map", { worldCopyJump: true }).setView([30, 10], 2);
L.tileLayer("https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png", {
  attribution: '&copy; OpenStreetMap &copy; CARTO',
  maxZoom: 18,
}).addTo(map);

const cluster = L.markerClusterGroup({
  maxClusterRadius: 45,
  spiderfyOnMaxZoom: true,
  showCoverageOnHover: false,
});

function radiusFor(count) {
  // zoom-scaling radius, clamped (experience from proxies_map.html)
  const z = map.getZoom();
  const r = 4 + Math.min(12, Math.sqrt(count) * 1.6) * (z / 6);
  return Math.max(3, Math.min(16, r));
}

function classColor(cls) { return cls === "rf" ? "#2ecc71" : "#3498db"; }

function render() {
  cluster.clearLayers();
  const cls = state.class;
  for (const c of state.cities) {
    if (!c.lat || !c.lon) continue;
    const marker = L.circleMarker([c.lat, c.lon], {
      radius: radiusFor(c.count),
      color: classColor(cls),
      weight: 1.5,
      fillColor: classColor(cls),
      fillOpacity: 0.45,
    });
    marker.on("click", () => selectCity(c));
    marker.bindTooltip(`${c.city}, ${c.cc} — ${c.count} nodes, best ${c.best_latency_ms ?? "?"}ms`);
    cluster.addLayer(marker);
  }
  map.on("zoomend", () => {
    cluster.eachLayer((m) => m.setRadius && m.setRadius(radiusFor(m.options.countHint || 4)));
  });
  updateStats();
}

async function loadCities() {
  const q = new URLSearchParams({ class: state.class });
  try {
    const resp = await fetch(`/api/cities?` + q);
    const data = await resp.json();
    state.cities = data.cities || [];
    render();
  } catch (e) {
    document.getElementById("stats").textContent = "api unreachable";
  }
}

async function selectCity(city) {
  state.selectedCity = city;
  document.getElementById("city-panel").classList.remove("hidden");
  document.getElementById("city-name").textContent = `${city.city}, ${city.cc} — ${city.count} nodes`;
  const list = document.getElementById("node-list");
  list.innerHTML = "<div class='node'>loading…</div>";
  const q = new URLSearchParams({ city: city.city, class: state.class, limit: "50" });
  if (state.proto) q.set("proto", state.proto);
  const resp = await fetch(`/api/nodes?` + q);
  const data = await resp.json();
  list.innerHTML = "";
  for (const n of data.nodes || []) {
    list.appendChild(nodeCard(n));
  }
  if (!list.children.length) list.innerHTML = "<div class='node'>no nodes match the protocol filter</div>";
}

function nodeCard(n) {
  const el = document.createElement("div");
  el.className = "node";
  const lat = n.latency_ms != null ? n.latency_ms + "ms" : "?";
  const speed = n.speed_mbps != null ? n.speed_mbps.toFixed(1) + " Mbps" : "";
  const last = n.last_check ? new Date(n.last_check).toLocaleString() : "";
  el.innerHTML = `
    <div class="row1"><span class="proto">${n.protocol}/${n.transport}</span><span class="badge ${n.class}">${n.class}</span></div>
    <div class="row2"><span>${lat} ${speed}</span><span>${last}</span></div>`;
  el.onclick = () => openNode(n.id, el);
  return el;
}

async function openNode(id, cardEl) {
  const resp = await fetch(`/api/node/` + id);
  const n = await resp.json();
  const link = n.share_link;
  const btn = document.createElement("button");
  btn.textContent = "copy link";
  btn.onclick = async (ev) => {
    ev.stopPropagation();
    try {
      await navigator.clipboard.writeText(link);
      btn.textContent = "copied!";
      setTimeout(() => (btn.textContent = "copy link"), 1200);
    } catch { /* clipboard unavailable */ }
  };
  const pre = document.createElement("div");
  pre.className = "row2";
  pre.innerHTML = `<span style="word-break:break-all">${escapeHtml(link.slice(0, 80))}…</span>`;
  const old = cardEl.querySelector("button");
  if (old) old.remove();
  cardEl.appendChild(btn);
  if (!cardEl.querySelector(".linkline")) {
    pre.classList.add("linkline");
    cardEl.appendChild(pre);
  }
}

function updateStats() {
  const total = state.cities.reduce((s, c) => s + c.count, 0);
  document.getElementById("stats").textContent =
    `${state.cities.length} cities · ${total} nodes · class=${state.class}`;
}

document.querySelectorAll("input[name=class]").forEach((r) => {
  r.onchange = () => {
    state.class = r.value;
    document.getElementById("sub-url").textContent = `/api/sub?class=${state.class}`;
    loadCities();
    if (state.selectedCity) selectCity(state.selectedCity);
  };
});
document.getElementById("proto").onchange = (e) => {
  state.proto = e.target.value;
  if (state.selectedCity) selectCity(state.selectedCity);
};
document.getElementById("copy-sub").onclick = async () => {
  const url = location.origin + document.getElementById("sub-url").textContent;
  try {
    await navigator.clipboard.writeText(url);
  } catch { /* clipboard unavailable */ }
};

function escapeHtml(s) {
  return s.replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
}

// SSE: refresh the map when a new publish lands (§8 /api/events)
function watchPublishes() {
  if (!window.EventSource) return;
  const es = new EventSource("/api/events");
  es.addEventListener("publish", () => loadCities());
  es.onerror = () => { /* browser retries automatically */ };
}

loadCities();
watchPublishes();
