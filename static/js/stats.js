/* Vietnam GIS Explorer – statistics panel */

const StatsPanel = (() => {
  let _chart = null;

  function render(data) {
    const body = document.getElementById("stats-body");
    document.getElementById("stats-title").textContent = data.name || "Statistics";
    body.innerHTML = "";

    _renderKeyStats(body, data);

    const socio = data.socioeconomic;
    if (socio) {
      if (socio.landcover) _renderLandUseChart(body, socio.landcover);
      if (socio.dem_mean != null) _renderTerrain(body, socio);
      if (socio.roads_total_km) _renderInfra(body, socio);
    }

    if (data.constituents && data.constituents.length > 1) {
      _renderConstituents(body, data.constituents);
    }

    _renderAttribution(body);
  }

  function _renderKeyStats(container, data) {
    const pop = data.population || 0;
    const area = data.area_km2 || 0;
    const density = data.density_per_km2 || 0;

    const card = _el("div", "stats-cards");
    card.innerHTML = `
      <div class="stat-card">
        <div class="stat-value">${pop.toLocaleString()}</div>
        <div class="stat-label">Population</div>
      </div>
      <div class="stat-card">
        <div class="stat-value">${area.toLocaleString()} km²</div>
        <div class="stat-label">Area</div>
      </div>
      <div class="stat-card">
        <div class="stat-value">${density.toLocaleString()}</div>
        <div class="stat-label">Density / km²</div>
      </div>
    `;

    const socio = data.socioeconomic;
    if (socio && socio.urban_percent != null) {
      card.innerHTML += `
        <div class="stat-card">
          <div class="stat-value">${socio.urban_percent.toFixed(1)}%</div>
          <div class="stat-label">Urban</div>
        </div>
        <div class="stat-card">
          <div class="stat-value">${(socio.forest_percent || 0).toFixed(1)}%</div>
          <div class="stat-label">Forest</div>
        </div>
      `;
    }
    container.appendChild(card);
  }

  function _renderLandUseChart(container, landcover) {
    const section = _el("div", "stats-section");
    section.innerHTML = "<h3>Land Use</h3>";
    const canvas = document.createElement("canvas");
    canvas.id = "landuse-chart";
    canvas.height = 200;
    section.appendChild(canvas);
    container.appendChild(section);

    if (_chart) { _chart.destroy(); _chart = null; }

    const labels = [];
    const values = [];
    const colors = [];
    const LC_COLORS = {
      Forest: "#006400", Shrubland: "#FFBB22", Grassland: "#FFFF4C",
      Agriculture: "#F096FF", Urban: "#FA0000", "Bare soil": "#B4B4B4",
      Water: "#0064C8", Wetland: "#0096A0", Mangroves: "#00CF75",
      "Snow/Ice": "#F0F0F0", "Moss/Lichen": "#FAE6A0",
    };

    for (const [key, val] of Object.entries(landcover)) {
      if (val.percent > 0.5) {
        labels.push(key);
        values.push(val.percent);
        colors.push(LC_COLORS[key] || "#999");
      }
    }

    _chart = new Chart(canvas, {
      type: "doughnut",
      data: {
        labels,
        datasets: [{ data: values, backgroundColor: colors, borderWidth: 1 }],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
          legend: { position: "bottom", labels: { font: { size: 11 } } },
          tooltip: {
            callbacks: {
              label: ctx => `${ctx.label}: ${ctx.parsed.toFixed(1)}%`,
            },
          },
        },
      },
    });
  }

  function _renderTerrain(container, socio) {
    const section = _el("div", "stats-section");
    section.innerHTML = `<h3>Terrain</h3>
      <table class="stats-table">
        <tr><td>Elevation (mean)</td><td>${socio.dem_mean.toFixed(0)} m</td></tr>
        <tr><td>Elevation (range)</td><td>${socio.dem_min.toFixed(0)} – ${socio.dem_max.toFixed(0)} m</td></tr>
        ${socio.slope_mean != null ? `<tr><td>Slope (mean)</td><td>${socio.slope_mean.toFixed(1)}°</td></tr>` : ""}
      </table>`;
    container.appendChild(section);
  }

  function _renderInfra(container, socio) {
    const section = _el("div", "stats-section");
    let html = "<h3>Infrastructure</h3><table class='stats-table'>";
    if (socio.roads_total_km)
      html += `<tr><td>Road network</td><td>${socio.roads_total_km.toLocaleString()} km</td></tr>
               <tr><td>Road density</td><td>${socio.roads_density_km_per_km2.toFixed(2)} km/km²</td></tr>`;
    if (socio.rivers_total_km)
      html += `<tr><td>River network</td><td>${socio.rivers_total_km.toLocaleString()} km</td></tr>`;
    html += "</table>";
    section.innerHTML = html;
    container.appendChild(section);
  }

  function _renderConstituents(container, constituents) {
    const section = _el("div", "stats-section");
    let html = "<h3>Constituent Provinces</h3><table class='stats-table'>";
    html += "<tr><th>Province</th><th>Pop.</th><th>Area</th><th>Density</th></tr>";
    constituents.forEach(c => {
      html += `<tr>
        <td>${c.name}</td>
        <td>${c.population.toLocaleString()}</td>
        <td>${c.area_km2.toLocaleString()} km²</td>
        <td>${c.density_per_km2.toLocaleString()}/km²</td>
      </tr>`;
    });
    html += "</table>";
    section.innerHTML = html;
    container.appendChild(section);
  }

  function _renderAttribution(container) {
    const div = _el("div", "stats-attribution");
    div.innerHTML = "Sources: GSO Vietnam 2023 &middot; Copernicus DEM &middot; ESA WorldCover &middot; OpenStreetMap";
    container.appendChild(div);
  }

  function _el(tag, cls) {
    const e = document.createElement(tag);
    if (cls) e.className = cls;
    return e;
  }

  return { render };
})();
