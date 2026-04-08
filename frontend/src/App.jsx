import { useEffect, useMemo, useRef, useState } from 'react'
import maplibregl from 'maplibre-gl'
import './App.css'

const RASTER_LAYERS = [
  { id: 'hillshade', label: 'Đổ bóng địa hình', group: 'Địa hình' },
  { id: 'dem', label: 'Độ cao (DEM)', group: 'Địa hình' },
  { id: 'slope', label: 'Độ dốc', group: 'Địa hình' },
  { id: 'aspect', label: 'Hướng dốc', group: 'Địa hình' },
  { id: 'landcover', label: 'Lớp phủ bề mặt', group: 'Lớp phủ' },
  { id: 'p_lsi', label: 'Xác suất sạt lở (p_lsi)', group: 'Nguy cơ' },
  { id: 'c_lsi', label: 'Phân loại nguy cơ (c_lsi)', group: 'Nguy cơ' },
]

const RASTER_LEGENDS = {
  hillshade: { title: 'Đổ bóng địa hình', items: [
    { color: '#222', label: 'Bóng' }, { color: '#888', label: 'Phẳng' }, { color: '#eee', label: 'Sáng' },
  ]},
  dem: { title: 'Độ cao (m)', items: [
    { color: '#234f1e', label: 'Thấp' }, { color: '#8cbc70', label: '' },
    { color: '#f5deb3', label: '' }, { color: '#a0522d', label: '' }, { color: '#fff', label: 'Cao' },
  ]},
  slope: { title: 'Độ dốc (độ)', items: [
    { color: '#ffffb2', label: '0' }, { color: '#fd8d3c', label: '15' }, { color: '#bd0026', label: '45+' },
  ]},
  aspect: { title: 'Hướng dốc', items: [
    { color: '#f00', label: 'N' }, { color: '#ff0', label: 'E' },
    { color: '#0f0', label: 'S' }, { color: '#00f', label: 'W' }, { color: '#f00', label: 'N' },
  ]},
  landcover: { title: 'Lớp phủ bề mặt', items: [
    { color: '#006400', label: 'Rừng' }, { color: '#F096FF', label: 'Nông nghiệp' },
    { color: '#FA0000', label: 'Đô thị' }, { color: '#0064C8', label: 'Nước' },
    { color: '#FFBB22', label: 'Cây bụi' }, { color: '#B4B4B4', label: 'Đất trống' },
  ]},
  p_lsi: { title: 'p_lsi (0–1)', items: [
    { color: '#ffffcc', label: '0.0' }, { color: '#fed976', label: '' },
    { color: '#fd8d3c', label: '' }, { color: '#e31a1c', label: '1.0' },
  ]},
  c_lsi: { title: 'c_lsi (1–5)', items: [
    { color: '#2c7bb6', label: '1 rất thấp' },
    { color: '#abd9e9', label: '2 thấp' },
    { color: '#ffffbf', label: '3 trung bình' },
    { color: '#fdae61', label: '4 cao' },
    { color: '#d7191c', label: '5 rất cao' },
  ]},
}

const VECTOR_LAYERS = [
  { id: 'infrastructure', label: 'Hạ tầng', type: 'circle', color: '#ff8f00' },
  { id: 'contour', label: 'Đường đồng mức', type: 'line', color: '#7b5d2a' },
  // Do not show coordinate details on-map for this research dataset.
  { id: 'real_ls_point', label: 'Điểm sạt lở thực tế', type: 'circle', color: '#22c55e' },
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
  const abortRef = useRef(null)
  const vecHandlersRef = useRef({})

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
    if (!error) return
    const t = setTimeout(() => setError(''), 5000)
    return () => clearTimeout(t)
  }, [error])

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
    if (abortRef.current) abortRef.current.abort()
    const ac = new AbortController()
    abortRef.current = ac

    const map = mapRef.current
    if (!map) return

    removeDynamicMapLayers(map)
    setSelectedProvince(name)
    setStats(null)
    setActiveRaster('')
    setActiveVectors({})
    setLoadingProvince(true)
    setError('')

    try {
      const [layerRes, statRes] = await Promise.all([
        fetch(`/api/provinces/${encodeURIComponent(name)}/layers`, { signal: ac.signal }).then((r) => r.json()),
        fetch(`/api/provinces/${encodeURIComponent(name)}/stats`, { signal: ac.signal }).then((r) => r.json()),
      ])
      if (ac.signal.aborted) return

      setAvailable(layerRes)
      setStats(statRes)

      if (map.getLayer('province-selected-fill')) {
        map.setFilter('province-selected-fill', ['==', ['get', 'name'], name])
      }

      try {
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

        const bnd = await fetch(
          `/api/provinces/${encodeURIComponent(name)}/boundary`, { signal: ac.signal }
        ).then((r) => r.json())
        if (ac.signal.aborted) return

        if (bnd.internal) {
          map.addSource('internal-boundary', { type: 'geojson', data: bnd.internal })
          map.addLayer({
            id: 'internal-boundary-line',
            type: 'line',
            source: 'internal-boundary',
            paint: { 'line-color': '#64748b', 'line-dasharray': [2, 2], 'line-width': 1 },
          })
        }
      } catch (e) {
        if (e?.name === 'AbortError') return
      }
    } catch (e) {
      if (e?.name === 'AbortError') return
      setError('Failed loading selected province')
    } finally {
      if (!ac.signal.aborted) setLoadingProvince(false)
    }
  }

  function backToVietnam() {
    if (abortRef.current) abortRef.current.abort()
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
      map.setPaintProperty('province-selected-fill', 'fill-opacity', 0.12)
    }
    if (map.getLayer('province-fill')) {
      map.setPaintProperty('province-fill', 'fill-opacity', 0.30)
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
      if (map.getLayer('province-fill')) {
        map.setPaintProperty('province-fill', 'fill-opacity', 0.30)
      }
      if (map.getLayer('province-selected-fill')) {
        map.setPaintProperty('province-selected-fill', 'fill-opacity', 0.12)
      }
      setActiveRaster('')
      return
    }

    if (map.getLayer('active-raster-layer')) map.removeLayer('active-raster-layer')
    if (map.getSource('active-raster-source')) map.removeSource('active-raster-source')

    map.addSource('active-raster-source', {
      type: 'raster',
      tiles: [`/api/provinces/${encodeURIComponent(selectedProvince)}/tiles/${layerId}/{z}/{x}/{y}.png`],
      tileSize: 256,
      // ~30m rasters gain little past z≈16; cap requests to avoid GDAL OOM on tile bursts
      maxzoom: 16,
    })
    map.addLayer({
      id: 'active-raster-layer',
      type: 'raster',
      source: 'active-raster-source',
      paint: { 'raster-opacity': 0.84 },
    })
    if (map.getLayer('province-fill')) {
      map.setPaintProperty('province-fill', 'fill-opacity', 0.12)
    }
    if (map.getLayer('province-selected-fill')) {
      map.setPaintProperty('province-selected-fill', 'fill-opacity', 0.06)
    }
    setActiveRaster(layerId)
  }

  async function toggleVector(layerId) {
    const map = mapRef.current
    if (!map || !selectedProvince) return
    const sourceId = `vec-source-${layerId}`
    const layerMapId = `vec-layer-${layerId}`

    if (map.getLayer(layerMapId)) {
      const hs = vecHandlersRef.current[layerMapId]
      if (hs) {
        try {
          map.off('mouseenter', layerMapId, hs.onEnter)
          map.off('mouseleave', layerMapId, hs.onLeave)
          map.off('click', layerMapId, hs.onClick)
        } catch {
          // ignore
        }
        delete vecHandlersRef.current[layerMapId]
      }
      map.removeLayer(layerMapId)
      if (map.getSource(sourceId)) map.removeSource(sourceId)
      setActiveVectors((s) => ({ ...s, [layerId]: false }))
      return
    }

    const cfg = VECTOR_LAYERS.find((v) => v.id === layerId)
    if (!cfg) return

    try {
      const vectorParams = {
        infrastructure: 'simplify=0.001&max_features=8000',
        contour: 'simplify=0.0012&max_features=12000',
      }
      const q = vectorParams[layerId] || 'simplify=0.0015&max_features=10000'
      const geo = await fetch(
        `/api/provinces/${encodeURIComponent(selectedProvince)}/vector/${layerId}?${q}`
      ).then((r) => r.json())
      map.addSource(sourceId, { type: 'geojson', data: geo })
      if (cfg.type === 'line') {
        map.addLayer({
          id: layerMapId,
          type: 'line',
          source: sourceId,
          minzoom: 8,
          paint: { 'line-color': cfg.color, 'line-width': 1.1, 'line-opacity': 0.9 },
        })
      } else {
        map.addLayer({
          id: layerMapId,
          type: 'circle',
          source: sourceId,
          minzoom: 10,
          paint: {
            'circle-color': cfg.color,
            'circle-radius': layerId === 'real_ls_point' ? 3 : 2,
            'circle-opacity': layerId === 'real_ls_point' ? 0.85 : 0.75,
            'circle-stroke-width': 0,
          },
        })
      }

      // For sensitive research points: allow inspection, but do NOT show coordinates.
      if (layerId === 'real_ls_point') {
        const onEnter = () => { map.getCanvas().style.cursor = 'pointer' }
        const onLeave = () => { map.getCanvas().style.cursor = '' }
        const onClick = (e) => {
          const f = e.features?.[0]
          if (!f) return
          const props = f.properties || {}
          const safePairs = Object.entries(props)
            .filter(([k, v]) => v != null && String(v).length > 0)
            .slice(0, 8)
            .map(([k, v]) => `<div><b>${k}</b>: ${String(v)}</div>`)
            .join('')
          const html = `<b>Điểm sạt lở thực tế</b>${safePairs ? `<div style="margin-top:6px">${safePairs}</div>` : ''}`

          if (!popupRef.current) {
            popupRef.current = new maplibregl.Popup({ closeButton: true, closeOnClick: true })
          }
          // Intentionally anchor to click location; do not print numeric coordinates.
          popupRef.current.setLngLat(e.lngLat).setHTML(html).addTo(map)
        }
        map.on('mouseenter', layerMapId, onEnter)
        map.on('mouseleave', layerMapId, onLeave)
        map.on('click', layerMapId, onClick)
        vecHandlersRef.current[layerMapId] = { onEnter, onLeave, onClick }
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
          <h1 className="top-title">Bản đồ GIS Việt Nam</h1>
          {/* <p className="top-sub">NQ 202/2025 ? Terrain ? OSM ? GSO</p> */}
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
            <option value="">Chọn tỉnh/thành</option>
            {provinces.map((p) => (
              <option key={p.name} value={p.name}>
                {p.name}{p.processed ? '' : ' (not processed)'}
              </option>
            ))}
          </select>
          <button className="btn" onClick={refreshProvince} disabled={!selectedProvince}>Tải lại</button>
          <button className="btn" onClick={backToVietnam} disabled={!selectedProvince}>Toàn quốc</button>
        </div>
      </header>

      <aside className="left-panel glass">
        <h3>Lớp dữ liệu {selectedInfo && (<span className={selectedInfo.processed ? 'tag ok' : 'tag warn'}>{selectedInfo.processed ? 'Đã xử lý' : 'Chưa xử lý'}</span>)}</h3>

        {!selectedProvince && <p className="muted">Chọn một tỉnh/thành</p>}

        {selectedProvince && (
          <>
            {!selectedInfo?.processed && (
              <div className="warn-box">
                <p>Tỉnh/thành chưa được xử lý.</p>
                <p className="muted">
                  Chạy trên máy: <code>python pipeline.py --province "{selectedProvince}"</code>
                </p>
              </div>
            )}

            <div className="panel-card">
              <h4>Lớp raster</h4>
              {Object.keys(groupedRaster).length === 0 && <p className="muted">Không có lớp raster</p>}
              {Object.entries(groupedRaster).map(([group, list]) => (
                <div key={group} className="group-block">
                  <p className="group-title">{group}</p>
                  {list.map((l) => (
                    <label key={l.id} className="row">
                      <input type="radio" name="raster-layer" checked={activeRaster === l.id} onClick={() => toggleRaster(l.id)} onChange={() => {}} />
                      {l.label}
                    </label>
                  ))}
                </div>
              ))}
            </div>

            <div className="panel-card">
              <h4>Lớp vector</h4>
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
        {activeRaster && RASTER_LEGENDS[activeRaster] ? (
          <>
            <b>{RASTER_LEGENDS[activeRaster].title}</b>
            {RASTER_LEGENDS[activeRaster].items.map((it, i) => (
              <div key={i} className="legend-row">
                <span style={{ background: it.color }} />
                {it.label}
              </div>
            ))}
          </>
        ) : (
          <>
            <b>Density / km&sup2;</b>
            <div className="legend-row"><span style={{ background: '#dcfce7' }} /> 50</div>
            <div className="legend-row"><span style={{ background: '#86efac' }} /> 300</div>
            <div className="legend-row"><span style={{ background: '#fbbf24' }} /> 800</div>
            <div className="legend-row"><span style={{ background: '#fb7185' }} /> 1,600</div>
            <div className="legend-row"><span style={{ background: '#e11d48' }} /> 2,600+</div>
          </>
        )}
      </div>

      <aside className="right-panel glass">
        <h3>Thống kê</h3>
        {!stats && !loadingProvince && <p className="muted">Bấm chọn một tỉnh/thành</p>}
        {loadingProvince && <p className="muted">Đang tải...</p>}
        {stats && !loadingProvince && (
          <div className="stats">
            <div className="card"><span>Population</span><b>{number(stats.population)}</b></div>
            <div className="card"><span>Area</span><b>{number(stats.area_km2)} km&sup2;</b></div>
            <div className="card"><span>Density</span><b>{number(stats.density_per_km2)}/km&sup2;</b></div>
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
          <p>Đang tải bản đồ...</p>
        </div>
      )}

      {error && (
        <div className="toast" onClick={() => setError('')}>{error}</div>
      )}
    </div>
  )
}
