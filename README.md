# Vietnam GIS Explorer

Interactive web-based GIS viewer for all 34 Vietnamese provinces under the 2025 administrative merger (NQ 202/2025/QH15). A Python pipeline downloads and processes geospatial data, and a FastAPI + React frontend lets you explore the results on a live map.

## Data Layers

- **Terrain** -- DEM, slope, aspect, hillshade (Copernicus GLO-30)
- **Land Cover** -- ESA WorldCover 10 m
- **Infrastructure** -- Roads, rivers, buildings (OpenStreetMap / Geofabrik)
- **Contours** -- Generated from DEM at configurable intervals
- **Demographics** -- GSO Vietnam official census data
- **Socioeconomic** -- Area, population density, urbanization, land use stats

## Project Structure

```
pipeline.py        Data pipeline -- downloads, processes, and writes rasters/vectors
app.py             FastAPI server -- serves tiles, GeoJSON, and stats to the frontend
requirements.txt   Python dependencies
frontend/          React + Vite web app (MapLibre GL JS)
data/              Generated province data (created by pipeline)
static/            Legacy static assets
```

## Setup

### Prerequisites

- Python 3.10+
- Node.js 18+

### Install

```bash
pip install -r requirements.txt
cd frontend && npm install
```

### Process province data

```bash
python pipeline.py --province "Hồ Chí Minh"
python pipeline.py --province "Quảng Ngãi"
python pipeline.py --list-provinces          # show all 34 provinces
```

### Run the web app

```bash
# Build frontend
cd frontend && npm run build && cd ..

# Start server
uvicorn app:app --host 0.0.0.0 --port 8000
```

Then open http://localhost:8000.

For development with hot reload:

```bash
# Terminal 1 -- backend
uvicorn app:app --reload --port 8000

# Terminal 2 -- frontend
cd frontend && npm run dev
```

## Tech Stack

| Layer    | Technology                                      |
|----------|-------------------------------------------------|
| Pipeline | Python, Rasterio, GeoPandas, NumPy, SciPy, OSMnx |
| Backend  | FastAPI, Uvicorn, Pillow                         |
| Frontend | React 19, Vite, MapLibre GL JS, Ant Design       |
