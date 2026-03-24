import { useEffect, useMemo, useRef, useState } from 'react'
import maplibregl from 'maplibre-gl'
import './App.css'

const RASTER_LAYERS = [
  { id: 'dem', label: 'DEM', group: 'Terrain' },
  { id: 'slope', label: 'Slope', group: 'Terrain' },
  { id: 'aspect', label: 'Aspect', group: 'Terrain' },
  { id: 'curvature', label: 'Curvature', group: 'Terrain' },
  { id: 'flow_accumulation', label: 'Flow Accumulation', group: 'Terrain' },
  { id: 'twi', label: 'TWI', group: 'Terrain' },
  { id: 'landcover', label: 'Land Cover', group: 'Landcover' },
  { id: 'dist_road', label: 'Distance to Road', group: 'Distance' },
  { id: 'dist_river', label: 'Distance to River', group: 'Distance' },
]

const VECTOR_LAYERS = [
  { id: 'roads', label: 'Roads', type: 'line', color: '#d62828' },
  { id: 'rivers', label: 'Rivers', type: 'line', color: '#1565c0' },
  { id: 'infrastructure', label: 'Infrastructure', type: 'circle', color: '#ff8f00' },
  { id: 'contour', label: 'Contours', type: 'line', color: '#7b5d2a' },
]

const vietnamStyle = {
  version: 8,
  sources: {
    osm: {
      type: 'raster',
      tiles: ['https://tile.openstreetmap.org/{z}/{x}/{y}.png'],
      tileSize: 256,
      attribution: '&copy; OpenStreetMap contributors',
    },
  },
  layers: [{ id: 'osm', type: 'raster', source: 'osm' }],
}

function normalizeFeatureCollection(fc) {
  if (!fc || !Array.isArray(fc.features)) return { type: 'FeatureCollection', features: [] }
  return fc
}

function bboxFromCoords(coords, box = [Infinity, Infinity, -Infinity, -Infinity]) {
  if (!Array.isArray(coords)) return box
  if (typeof coords[0] === 'number') {
    const [x, y] = coords
    box[0] = Math.min(box[0], x)
    box[1] = Math.min(box[1], y)
    box[2] = Math.max(box[2], x)
    box[3] = Math.max(box[3], y)
    return box
  }
  coords.forEach((c) => bboxFromCoords(c, box))
  return box
}

function fitGeoJSON(map, geojson) {
  if (!map || !geojson) return
  const box = [Infinity, Infinity, -Infinity, -Infinity]
  const features = geojson.type === 'FeatureCollection' ? geojson.features : [geojson]
  features.forEach((f) => {
    if (f?.geometry?.coordinates) bboxFromCoords(f.geometry.coordinates, box)
  })
  if (Number.isFinite(box[0])) {
    map.fitBounds(
      [
        [box[0], box[1]],
        [box[2], box[3]],
      ],
      { padding: 40, duration: 700 }
    )
  }
}

function number(v) {
  return Number(v || 0).toLocaleString()
}

export default function App() {
  const mapNode = useRef(null)
  const mapRef = useRef(null)
  const popupRef = useRef(null)

  const [provinces, setProvinces] = useState([])
  const [boundaryData, setBoundaryData] = useState({ type: 'FeatureCollection', features: [] })
  const [selectedProvince, setSelectedProvince] = useState('')
  const [available, setAvailable] = useState({ raster: [], vector: [] })
  const [activeRaster, setActiveRaster] = useState('')
  const [activeVectors, setActiveVectors] = useState({})
  const [stats, setStats] = useState(null)
  const [loadingMap, setLoadingMap] = useState(true)
  const [loadingProvince, setLoadingProvince] = useState(false)
  const [error, setError] = useState('')

  const selectedInfo = useMemo(
    () => provinces.find((p) => p.name === selectedProvince) || null,
    [provinces, selectedProvince]
  )

  useEffect(() => {
    if (!mapNode.current || mapRef.current) return

    const map = new maplibregl.Map({
      container: mapNode.current,
      style: vietnamStyle,
      center: [106.5, 16.0],
      zoom: 5.7,
      attributionControl: true,
    })
    map.addControl(new maplibregl.NavigationControl({ visualizePitch: true }), 'top-right')
    mapRef.current = map

    map.on('load', async () => {
      try {
        const [pRes, bRes] = await Promise.all([
          fetch('/api/provinces').then((r) => r.json()),
          fetch('/api/provinces/boundaries').then((r) => r.json()),
        ])
        setProvinces(pRes)
        const fc = normalizeFeatureCollection(bRes)
        setBoundaryData(fc)
        renderNational(fc)
        fitGeoJSON(map, fc)
      } catch {
        setError('Failed to load province data from API')
      } finally {
        setLoadingMap(false)
      }
    })

    return () => {
      map.remove()
      mapRef.current = null
    }
  }, [])

  useEffect(() => {
    const map = mapRef.current
    if (!map) return
    removeDynamicMapLayers(map)
    setActiveRaster('')
    setActiveVectors({})
  }, [selectedProvince])

  function renderNational(fc) {
    const map = mapRef.current
    if (!map) return

    ;['province-selected-fill', 'province-hover-line', 'province-fill', 'province-line'].forEach((id) => {
      if (map.getLayer(id)) map.removeLayer(id)
    })
    if (map.getSource('provinces')) map.removeSource('provinces')

    map.addSource('provinces', { type: 'geojson', data: fc })

    map.addLayer({
      id: 'province-fill',
      type: 'fill',
      source: 'provinces',
      paint: {
        'fill-color': [
          'interpolate',
          ['linear'],
          ['coalesce', ['get', 'density_per_km2'], 0],
          50, '#dcfce7',
          300, '#86efac',
          800, '#fbbf24',
          1600, '#fb7185',
          2600, '#e11d48',
        ],
        'fill-opacity': 0.30,
      },
    })

    map.addLayer({
      id: 'province-selected-fill',
      type: 'fill',
      source: 'provinces',
      filter: ['==', ['get', 'name'], ''],
      paint: {
        'fill-color': '#2563eb',
        'fill-opacity': 0.12,
      },
    })

    map.addLayer({
      id: 'province-line',
      type: 'line',
      source: 'provinces',
      paint: { 'line-color': '#1f2937', 'line-width': 1 },
    })

    map.addLayer({
      id: 'province-hover-line',
      type: 'line',
      source: 'provinces',
      filter: ['==', ['get', 'name'], ''],
      paint: { 'line-color': '#111827', 'line-width': 2.2 },
    })

    map.on('mousemove', 'province-fill', (e) => {
      const f = e.features?.[0]
      if (!f) return
      map.getCanvas().style.cursor = 'pointer'
      map.setFilter('province-hover-line', ['==', ['get', 'name'], f.properties.name])
      if (!popupRef.current) {
        popupRef.current = new maplibregl.Popup({ closeButton: false, closeOnClick: false })
      }
      popupRef.current
        .setLngLat(e.lngLat)
        .setHTML(
          `<b>${f.properties.name}</b><br/>Population: ${number(f.properties.population)}<br/>Density: ${number(f.properties.density_per_km2)}/km2`
        )
        .addTo(map)
    })

    map.on('mouseleave', 'province-fill', () => {
      map.getCanvas().style.cursor = ''
      map.setFilter('province-hover-line', ['==', ['get', 'name'], ''])
      popupRef.current?.remove()
    })

    map.on('click', 'province-fill', (e) => {
      const f = e.features?.[0]
      if (!f) return
      void chooseProvince(f.properties.name)
    })
  }

  async function chooseProvince(name) {
    setSelectedProvince(name)
    setLoadingProvince(true)
    setError('')
    const map = mapRef.current
    if (!map) return

    try {
      const [layerRes, statRes] = await Promise.all([
        fetch(`/api/provinces/${encodeURIComponent(name)}/layers`).then((r) => r.json()),
        fetch(`/api/provinces/${encodeURIComponent(name)}/stats`).then((r) => r.json()),
      ])
      setAvailable(layerRes)
      setStats(statRes)

      if (map.getLayer('province-selected-fill')) {
        map.setFilter('province-selected-fill', ['==', ['get', 'name'], name])
      }

      try {
        // Use national merged boundary source for stable topology (fewer slivers/holes).
        const feature = boundaryData.features.find((f) => f?.properties?.name === name)
        if (feature) {
          const selectedFc = { type: 'FeatureCollection', features: [feature] }
          map.addSource('selected-boundary', { type: 'geojson', data: selectedFc })
          map.addLayer({
            id: 'selected-boundary-line',
            type: 'line',
            source: 'selected-boundary',
            paint: { 'line-color': '#0f172a', 'line-width': 2.3 },
          })
          fitGeoJSON(map, selectedFc)
        }

        const bnd = await fetch(`/api/provinces/${encodeURIComponent(name)}/boundary`).then((r) => r.json())
        if (bnd.internal) {
          map.addSource('internal-boundary', { type: 'geojson', data: bnd.internal })
          map.addLayer({
            id: 'internal-boundary-line',
            type: 'line',
            source: 'internal-boundary',
            paint: { 'line-color': '#64748b', 'line-dasharray': [2, 2], 'line-width': 1 },
          })
        }
      } catch {
        // Not processed yet.
      }
    } catch {
      setError('Failed loading selected province')
    } finally {
      setLoadingProvince(false)
    }
  }

  function backToVietnam() {
    const map = mapRef.current
    if (!map) return
    setSelectedProvince('')
    setStats(null)
    setAvailable({ raster: [], vector: [] })
    setActiveRaster('')
    setActiveVectors({})
    removeDynamicMapLayers(map)
    if (map.getLayer('province-selected-fill')) {
      map.setFilter('province-selected-fill', ['==', ['get', 'name'], ''])
    }
    fitGeoJSON(map, boundaryData)
  }

  function removeDynamicMapLayers(map) {
    ;[
      'selected-boundary-line',
      'internal-boundary-line',
      'active-raster-layer',
      ...VECTOR_LAYERS.map((v) => `vec-layer-${v.id}`),
    ].forEach((id) => {
      if (map.getLayer(id)) map.removeLayer(id)
    })

    ;[
      'selected-boundary',
      'internal-boundary',
      'active-raster-source',
      ...VECTOR_LAYERS.map((v) => `vec-source-${v.id}`),
    ].forEach((id) => {
      if (map.getSource(id)) map.removeSource(id)
    })
  }

  function toggleRaster(layerId) {
    const map = mapRef.current
    if (!map || !selectedProvince) return

    if (activeRaster === layerId) {
      if (map.getLayer('active-raster-layer')) map.removeLayer('active-raster-layer')
      if (map.getSource('active-raster-source')) map.removeSource('active-raster-source')
      setActiveRaster('')
      return
    }

    if (map.getLayer('active-raster-layer')) map.removeLayer('active-raster-layer')
    if (map.getSource('active-raster-source')) map.removeSource('active-raster-source')

    map.addSource('active-raster-source', {
      type: 'raster',
      tiles: [`/api/provinces/${encodeURIComponent(selectedProvince)}/tiles/${layerId}/{z}/{x}/{y}.png`],
      tileSize: 256,
    })
    map.addLayer({
      id: 'active-raster-layer',
      type: 'raster',
      source: 'active-raster-source',
      paint: { 'raster-opacity': 0.84 },
    })
    setActiveRaster(layerId)
  }

  async function toggleVector(layerId) {
    const map = mapRef.current
    if (!map || !selectedProvince) return
    const sourceId = `vec-source-${layerId}`
    const layerMapId = `vec-layer-${layerId}`

    if (map.getLayer(layerMapId)) {
      map.removeLayer(layerMapId)
      if (map.getSource(sourceId)) map.removeSource(sourceId)
      setActiveVectors((s) => ({ ...s, [layerId]: false }))
      return
    }

    const cfg = VECTOR_LAYERS.find((v) => v.id === layerId)
    if (!cfg) return

    try {
      const geo = await fetch(`/api/provinces/${encodeURIComponent(selectedProvince)}/vector/${layerId}`).then((r) => r.json())
      map.addSource(sourceId, { type: 'geojson', data: geo })
      if (cfg.type === 'line') {
        map.addLayer({
          id: layerMapId,
          type: 'line',
          source: sourceId,
          paint: { 'line-color': cfg.color, 'line-width': 1.3, 'line-opacity': 0.95 },
        })
      } else {
        map.addLayer({
          id: layerMapId,
          type: 'circle',
          source: sourceId,
          paint: {
            'circle-color': cfg.color,
            'circle-radius': 2.5,
            'circle-opacity': 0.8,
            'circle-stroke-width': 0,
          },
        })
      }
      setActiveVectors((s) => ({ ...s, [layerId]: true }))
    } catch {
      setError(`Cannot load vector layer: ${layerId}`)
    }
  }

  async function refreshProvince() {
    if (!selectedProvince) return
    await chooseProvince(selectedProvince)
  }

  const groupedRaster = useMemo(() => {
    const g = {}
    RASTER_LAYERS.filter((l) => available.raster.includes(l.id)).forEach((l) => {
      g[l.group] = g[l.group] || []
      g[l.group].push(l)
    })
    return g
  }, [available])

  return (
    <div className="app-shell">
      <header className="topbar">
        <div>
          <h1 className="top-title">Vietnam GIS Explorer</h1>
          <p className="top-sub">NQ 202/2025 ? Terrain ? OSM ? GSO</p>
        </div>
        <div className="toolbar">
          <select
            className="province-select"
            value={selectedProvince}
            onChange={(e) => {
              const v = e.target.value
              if (v) void chooseProvince(v)
            }}
          >
            <option value="">Select province</option>
            {provinces.map((p) => (
              <option key={p.name} value={p.name}>
                {p.name}{p.processed ? '' : ' (not processed)'}
              </option>
            ))}
          </select>
          <button className="btn" onClick={refreshProvince} disabled={!selectedProvince}>Refresh</button>
          <button className="btn" onClick={backToVietnam} disabled={!selectedProvince}>National</button>
        </div>
      </header>

      <aside className="left-panel glass">
        <h3>Layers {selectedInfo && (<span className={selectedInfo.processed ? 'tag ok' : 'tag warn'}>{selectedInfo.processed ? 'Processed' : 'Not processed'}</span>)}</h3>

        {!selectedProvince && <p className="muted">Select a province</p>}

        {selectedProvince && (
          <>
            {!selectedInfo?.processed && (
              <div className="warn-box">
                <p>Province not processed yet.</p>
                <p className="muted">
                  Run locally: <code>python pipeline.py --province "{selectedProvince}"</code>
                </p>
              </div>
            )}

            <div className="panel-card">
              <h4>Raster layers</h4>
              {Object.keys(groupedRaster).length === 0 && <p className="muted">No raster layers</p>}
              {Object.entries(groupedRaster).map(([group, list]) => (
                <div key={group} className="group-block">
                  <p className="group-title">{group}</p>
                  {list.map((l) => (
                    <label key={l.id} className="row">
                      <input type="checkbox" checked={activeRaster === l.id} onChange={() => toggleRaster(l.id)} />
                      {l.label}
                    </label>
                  ))}
                </div>
              ))}
            </div>

            <div className="panel-card">
              <h4>Vector layers</h4>
              {VECTOR_LAYERS.filter((l) => available.vector.includes(l.id)).map((l) => (
                <label key={l.id} className="row">
                  <input type="checkbox" checked={!!activeVectors[l.id]} onChange={() => void toggleVector(l.id)} />
                  {l.label}
                </label>
              ))}
            </div>
          </>
        )}
      </aside>

      <main ref={mapNode} className="map-wrap" />

      <div className="legend-overlay glass">
        <b>Density / km?</b>
        <div className="legend-row"><span style={{ background: '#dcfce7' }} /> 50</div>
        <div className="legend-row"><span style={{ background: '#86efac' }} /> 300</div>
        <div className="legend-row"><span style={{ background: '#fbbf24' }} /> 800</div>
        <div className="legend-row"><span style={{ background: '#fb7185' }} /> 1,600</div>
        <div className="legend-row"><span style={{ background: '#e11d48' }} /> 2,600+</div>
      </div>

      <aside className="right-panel glass">
        <h3>Statistics</h3>
        {!stats && <p className="muted">Click a province</p>}
        {loadingProvince && <p className="muted">Loading...</p>}
        {stats && !loadingProvince && (
          <div className="stats">
            <div className="card"><span>Population</span><b>{number(stats.population)}</b></div>
            <div className="card"><span>Area</span><b>{number(stats.area_km2)} km?</b></div>
            <div className="card"><span>Density</span><b>{number(stats.density_per_km2)}/km?</b></div>
            {stats.socioeconomic?.urban_percent != null && (
              <div className="card"><span>Urban</span><b>{stats.socioeconomic.urban_percent.toFixed(1)}%</b></div>
            )}
            <h4>Constituents</h4>
            <div className="const-list">
              {(stats.constituents || []).map((c) => (
                <div key={c.name} className="const-row">
                  <span>{c.name}</span>
                  <span>{number(c.population)}</span>
                </div>
              ))}
            </div>
          </div>
        )}
      </aside>

      {loadingMap && (
        <div className="global-loading">
          <p>Loading map...</p>
        </div>
      )}

      {error && (
        <div className="toast">{error}</div>
      )}
    </div>
  )
}
