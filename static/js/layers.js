/* Vietnam GIS Explorer – layer management */

const LayerControl = (() => {
  let _activeLayers = {};
  let _boundaryLayer = null;
  let _internalLayer = null;
  let _currentProvince = null;

  const RASTER_META = {
    dem:               { label: "DEM – Elevation",            group: "Terrain" },
    slope:             { label: "Slope",                      group: "Terrain" },
    aspect:            { label: "Aspect",                     group: "Terrain" },
    curvature:         { label: "Curvature",                  group: "Terrain" },
    flow_accumulation: { label: "Flow Accumulation",          group: "Terrain" },
    twi:               { label: "TWI",                        group: "Terrain" },
    landcover:         { label: "Land Cover (ESA)",           group: "Land Cover" },
    dist_road:         { label: "Distance to Road",           group: "Analysis" },
    dist_river:        { label: "Distance to River",          group: "Analysis" },
  };

  const VECTOR_META = {
    roads:          { label: "Roads (OSM)",           color: "#D62828", weight: 1.2, group: "Infrastructure" },
    rivers:         { label: "Rivers (OSM)",          color: "#1565C0", weight: 1.5, group: "Infrastructure" },
    infrastructure: { label: "Buildings (OSM)",       color: "#FF8F00", weight: 0.8, group: "Infrastructure" },
    contour:        { label: "Contour Lines",         color: "#8B6914", weight: 0.6, group: "Analysis" },
  };

  function init(province, layersInfo, boundaryData) {
    clearAll();
    _currentProvince = province;

    if (boundaryData.boundary) {
      _boundaryLayer = L.geoJSON(boundaryData.boundary, {
        style: { fill: false, color: "#222", weight: 2.5, dashArray: null },
      }).addTo(APP.map);
    }
    if (boundaryData.internal) {
      _internalLayer = L.geoJSON(boundaryData.internal, {
        style: { fill: false, color: "#888", weight: 1, dashArray: "5,4" },
        onEachFeature: (f, lyr) => {
          if (f.properties && f.properties.NAME_1) {
            lyr.bindTooltip(f.properties.NAME_1, { sticky: true });
          }
        },
      }).addTo(APP.map);
    }

    _buildPanel(layersInfo);
  }

  function _buildPanel(layersInfo) {
    const body = document.getElementById("layer-body");
    body.innerHTML = "";

    const groups = {};

    layersInfo.raster.forEach(name => {
      const meta = RASTER_META[name] || { label: name, group: "Other" };
      if (!groups[meta.group]) groups[meta.group] = [];
      groups[meta.group].push({ name, label: meta.label, type: "raster" });
    });

    layersInfo.vector.forEach(name => {
      const meta = VECTOR_META[name] || { label: name, group: "Other" };
      if (!groups[meta.group]) groups[meta.group] = [];
      groups[meta.group].push({ name, label: meta.label, type: "vector" });
    });

    const order = ["Terrain", "Land Cover", "Infrastructure", "Analysis", "Other"];
    order.forEach(groupName => {
      const items = groups[groupName];
      if (!items) return;
      const section = document.createElement("div");
      section.className = "layer-group";
      section.innerHTML = `<h3>${groupName}</h3>`;

      items.forEach(item => {
        const row = document.createElement("label");
        row.className = "layer-row";
        const inp = document.createElement("input");
        inp.type = "checkbox";
        inp.dataset.layer = item.name;
        inp.dataset.ltype = item.type;
        inp.addEventListener("change", e => _toggleLayer(item, e.target.checked));
        row.appendChild(inp);
        row.appendChild(document.createTextNode(" " + item.label));

        if (item.type === "raster") {
          const slider = document.createElement("input");
          slider.type = "range";
          slider.min = "0.1";
          slider.max = "1";
          slider.step = "0.1";
          slider.value = "0.8";
          slider.className = "opacity-slider";
          slider.addEventListener("input", e => _setOpacity(item.name, parseFloat(e.target.value)));
          row.appendChild(slider);
        }

        section.appendChild(row);
      });

      body.appendChild(section);
    });
  }

  function _toggleLayer(item, on) {
    if (on) {
      if (item.type === "raster") {
        // Keep only one raster visible at a time for predictable rendering.
        _deactivateOtherRasterToggles(item.name);
        _removeAllRasterLayersExcept(item.name);
        _addRaster(item.name);
      }
      else _addVector(item.name);
    } else {
      _removeLayer(item.name);
    }
  }

  function _deactivateOtherRasterToggles(activeName) {
    const toggles = document.querySelectorAll('#layer-body input[type="checkbox"][data-ltype="raster"]');
    toggles.forEach(t => {
      if (t.dataset.layer !== activeName) t.checked = false;
    });
  }

  function _removeAllRasterLayersExcept(activeName) {
    Object.keys(_activeLayers).forEach(name => {
      if (name !== activeName && RASTER_META[name]) _removeLayer(name);
    });
  }

  function _addRaster(name) {
    if (_activeLayers[name]) return;
    const url = `/api/provinces/${encodeURIComponent(_currentProvince)}/tiles/${name}/{z}/{x}/{y}.png`;
    const layer = L.tileLayer(url, { opacity: 0.8, maxZoom: 18, tileSize: 256 });
    layer.addTo(APP.map);
    _activeLayers[name] = layer;
  }

  async function _addVector(name) {
    if (_activeLayers[name]) return;
    const meta = VECTOR_META[name] || {};
    try {
      const data = await fetch(
        `/api/provinces/${encodeURIComponent(_currentProvince)}/vector/${name}`
      ).then(r => r.json());

      const layer = L.geoJSON(data, {
        style: {
          color: meta.color || "#333",
          weight: meta.weight || 1,
          opacity: 0.8,
        },
        pointToLayer: (f, ll) => L.circleMarker(ll, {
          radius: 3, color: meta.color || "#333",
          fillColor: meta.color || "#333", fillOpacity: 0.6,
        }),
        onEachFeature: (f, lyr) => {
          const props = f.properties || {};
          const label = props.name || props.highway || props.waterway ||
                        props.amenity || props.building || props.elevation || "";
          if (label) lyr.bindPopup(String(label));
        },
      });
      layer.addTo(APP.map);
      _activeLayers[name] = layer;
    } catch (e) {
      console.warn("Failed to load vector layer:", name, e);
    }
  }

  function _removeLayer(name) {
    if (_activeLayers[name]) {
      APP.map.removeLayer(_activeLayers[name]);
      delete _activeLayers[name];
    }
  }

  function _setOpacity(name, val) {
    if (_activeLayers[name] && _activeLayers[name].setOpacity) {
      _activeLayers[name].setOpacity(val);
    }
  }

  function clearAll() {
    Object.keys(_activeLayers).forEach(n => {
      APP.map.removeLayer(_activeLayers[n]);
    });
    _activeLayers = {};
    if (_boundaryLayer) { APP.map.removeLayer(_boundaryLayer); _boundaryLayer = null; }
    if (_internalLayer) { APP.map.removeLayer(_internalLayer); _internalLayer = null; }
    _currentProvince = null;
  }

  function showEmpty(name) {
    const body = document.getElementById("layer-body");
    body.innerHTML = `<p class="muted">Province <strong>${name}</strong> has not been processed yet.</p>
      <button id="btn-process-inline" class="btn btn-primary btn-sm">Process Now</button>`;
    const btn = document.getElementById("btn-process-inline");
    if (btn) {
      btn.addEventListener("click", () => {
        if (window.showModal) window.showModal(name);
      });
    }
  }

  return { init, clearAll, showEmpty };
})();
