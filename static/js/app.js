/* Vietnam GIS Explorer – main application logic */

const APP = {
  map: null,
  choropleth: null,
  currentProvince: null,
  provinces: [],
  boundariesGeo: null,
};

/* ── Initialisation ──────────────────────────────────── */

document.addEventListener("DOMContentLoaded", async () => {
  APP.map = L.map("map", {
    center: [16.0, 106.5],
    zoom: 6,
    zoomControl: false,
    attributionControl: false,
  });

  L.control.zoom({ position: "topright" }).addTo(APP.map);
  L.control.attribution({ position: "bottomright", prefix: false })
    .addAttribution('GSO &middot; Copernicus &middot; ESA &middot; OSM')
    .addTo(APP.map);

  const osm = L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
    maxZoom: 19,
    attribution: "&copy; OpenStreetMap contributors",
  });
  const carto = L.tileLayer("https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png", {
    maxZoom: 19,
    subdomains: "abcd",
    attribution: "&copy; OpenStreetMap &copy; CARTO",
  });
  osm.addTo(APP.map);
  L.control.layers({ OpenStreetMap: osm, "CARTO Light": carto }, {}).addTo(APP.map);

  let provRes = [];
  let bndRes = { type: "FeatureCollection", features: [] };
  try {
    [provRes, bndRes] = await Promise.all([
      fetch("/api/provinces").then(r => r.json()),
      fetch("/api/provinces/boundaries").then(r => r.json()),
    ]);
  } catch (e) {
    console.error("Failed to load initial datasets", e);
    document.getElementById("layer-body").innerHTML =
      '<p class="muted">Failed to load API data. Check backend logs and refresh.</p>';
  }
  APP.provinces = provRes;
  APP.boundariesGeo = bndRes;

  _populateSelect(provRes);
  _renderChoropleth(bndRes, provRes);
  APP.map.invalidateSize();

  document.getElementById("province-select").addEventListener("change", e => {
    if (e.target.value) selectProvince(e.target.value);
  });
  document.getElementById("btn-back").addEventListener("click", backToNational);
  document.getElementById("btn-close-stats").addEventListener("click", () => {
    document.getElementById("stats-panel").classList.add("hidden");
    APP.map.invalidateSize();
  });
  document.getElementById("btn-collapse-layers").addEventListener("click", () => {
    document.getElementById("layer-panel").classList.toggle("collapsed");
    setTimeout(() => APP.map.invalidateSize(), 260);
  });
  document.getElementById("btn-cancel-modal").addEventListener("click", hideModal);
  document.getElementById("btn-process").addEventListener("click", () => {
    if (APP.currentProvince) processProvince(APP.currentProvince);
  });
});


/* ── National choropleth ─────────────────────────────── */

function _populateSelect(provinces) {
  const sel = document.getElementById("province-select");
  provinces.forEach(p => {
    const opt = document.createElement("option");
    opt.value = p.name;
    opt.textContent = p.name + (p.processed ? "" : " (not processed)");
    sel.appendChild(opt);
  });
}

function _renderChoropleth(geojson, provinces) {
  if (!geojson || !Array.isArray(geojson.features) || geojson.features.length === 0) {
    return;
  }
  const densities = provinces.map(p => p.density_per_km2).filter(d => d > 0);
  if (!densities.length) return;
  const dMin = Math.min(...densities);
  const dMax = Math.max(...densities);

  function getColor(d) {
    const t = Math.min(Math.max((d - dMin) / (dMax - dMin || 1), 0), 1);
    const r = Math.round(255 * Math.min(t * 2, 1));
    const g = Math.round(255 * (1 - t));
    const b = 60;
    return `rgb(${r},${g},${b})`;
  }

  APP.choropleth = L.geoJSON(geojson, {
    style: f => ({
      fillColor: getColor(f.properties.density_per_km2),
      weight: 1.2,
      color: "#555",
      fillOpacity: 0.65,
    }),
    onEachFeature: (f, layer) => {
      const p = f.properties;
      layer.bindTooltip(
        `<strong>${p.name}</strong><br>` +
        `Pop: ${p.population.toLocaleString()}<br>` +
        `Density: ${p.density_per_km2.toLocaleString()}/km²`,
        { sticky: true }
      );
      layer.on("click", () => selectProvince(p.name));
      layer.on("mouseover", () => layer.setStyle({ weight: 2.5, fillOpacity: 0.85 }));
      layer.on("mouseout", () => APP.choropleth.resetStyle(layer));
    },
  }).addTo(APP.map);
  APP.choropleth.bringToFront();
  const bounds = APP.choropleth.getBounds();
  if (bounds && bounds.isValid()) APP.map.fitBounds(bounds, { padding: [20, 20] });

  _addLegend(dMin, dMax, getColor);
}

function _addLegend(dMin, dMax, colorFn) {
  const legend = L.control({ position: "bottomleft" });
  legend.onAdd = function () {
    const div = L.DomUtil.create("div", "legend");
    div.innerHTML = "<strong>Density (per km²)</strong><br>";
    const steps = 6;
    for (let i = 0; i <= steps; i++) {
      const v = dMin + (dMax - dMin) * (i / steps);
      div.innerHTML +=
        `<i style="background:${colorFn(v)}"></i> ${Math.round(v).toLocaleString()}<br>`;
    }
    return div;
  };
  legend.addTo(APP.map);
  APP._nationalLegend = legend;
}


/* ── Province drill-down ─────────────────────────────── */

async function selectProvince(name) {
  APP.currentProvince = name;
  document.getElementById("province-select").value = name;
  document.getElementById("btn-back").style.display = "";

  if (APP.choropleth) APP.choropleth.setStyle({ fillOpacity: 0.15, weight: 0.5 });

  const info = APP.provinces.find(p => p.name === name);
  if (!info) return;

  let layersRes = { raster: [], vector: [] };
  let statsRes = info;
  try {
    const [layersData, statsData] = await Promise.all([
      fetch(`/api/provinces/${encodeURIComponent(name)}/layers`).then(r => r.json()),
      fetch(`/api/provinces/${encodeURIComponent(name)}/stats`).then(r => r.json()),
    ]);
    layersRes = layersData;
    statsRes = statsData;
  } catch (e) {
    console.warn("Failed loading province details", e);
  }

  StatsPanel.render(statsRes);
  document.getElementById("stats-panel").classList.remove("hidden");

  if (info.processed) {
    try {
      const bnd = await fetch(`/api/provinces/${encodeURIComponent(name)}/boundary`).then(r => r.json());
      _zoomToBoundary(bnd.boundary);
      LayerControl.init(name, layersRes, bnd);
      APP.map.invalidateSize();
    } catch (e) {
      LayerControl.showEmpty(name);
    }
  } else {
    LayerControl.showEmpty(name);
    _zoomToProvince(name);
    showModal(name);
  }
}

function _zoomToBoundary(geojson) {
  if (!geojson) return;
  const layer = L.geoJSON(geojson);
  APP.map.fitBounds(layer.getBounds(), { padding: [30, 30] });
}

function _zoomToProvince(name) {
  if (!APP.boundariesGeo) return;
  const feat = APP.boundariesGeo.features.find(f => f.properties.name === name);
  if (feat) {
    const layer = L.geoJSON(feat);
    APP.map.fitBounds(layer.getBounds(), { padding: [30, 30] });
  }
}


/* ── Back to national ────────────────────────────────── */

function backToNational() {
  APP.currentProvince = null;
  document.getElementById("province-select").value = "";
  document.getElementById("btn-back").style.display = "none";
  document.getElementById("stats-panel").classList.add("hidden");
  LayerControl.clearAll();
  if (APP.choropleth) APP.choropleth.setStyle(f => ({
    fillColor: APP.choropleth.options.style(f).fillColor,
    weight: 1.2, color: "#555", fillOpacity: 0.65,
  }));
  APP.map.setView([16.0, 106.5], 6);
  APP.map.invalidateSize();
  document.getElementById("layer-body").innerHTML =
    '<p class="muted">Select a processed province to see layers.</p>';
}


/* ── Processing modal ────────────────────────────────── */

function showModal(name) {
  document.getElementById("modal-title").textContent = `Process: ${name}`;
  document.getElementById("modal-message").classList.remove("hidden");
  document.getElementById("modal-actions").classList.remove("hidden");
  document.getElementById("modal-progress").classList.add("hidden");
  document.getElementById("process-modal").classList.remove("hidden");
}
window.showModal = showModal;

function hideModal() {
  document.getElementById("process-modal").classList.add("hidden");
}

async function processProvince(name) {
  document.getElementById("modal-message").classList.add("hidden");
  document.getElementById("modal-actions").classList.add("hidden");
  document.getElementById("modal-progress").classList.remove("hidden");
  document.getElementById("progress-text").textContent = "Starting pipeline...";
  document.getElementById("progress-fill").style.width = "5%";

  await fetch(`/api/provinces/${encodeURIComponent(name)}/process`, { method: "POST" });
  _pollProcessing(name);
}

async function _pollProcessing(name) {
  const res = await fetch(`/api/provinces/${encodeURIComponent(name)}/process/status`);
  const data = await res.json();

  if (data.status === "running") {
    const el = document.getElementById("progress-fill");
    const cur = parseFloat(el.style.width) || 5;
    el.style.width = Math.min(cur + 2, 90) + "%";
    document.getElementById("progress-text").textContent = data.message || "Processing...";
    setTimeout(() => _pollProcessing(name), 3000);
  } else if (data.status === "done") {
    document.getElementById("progress-fill").style.width = "100%";
    document.getElementById("progress-text").textContent = "Done! Reloading...";
    setTimeout(async () => {
      hideModal();
      const provRes = await fetch("/api/provinces").then(r => r.json());
      APP.provinces = provRes;
      await selectProvince(name);
    }, 1000);
  } else {
    document.getElementById("progress-text").textContent = "Error: " + (data.message || "Unknown");
    document.getElementById("modal-actions").classList.remove("hidden");
    document.getElementById("btn-process").textContent = "Retry";
  }
}
