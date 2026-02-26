#!/usr/bin/env python3
"""
=========================================================
 Provincial Landslide Prediction GIS Pipeline (Vietnam)
=========================================================
Usage:
    python pipeline.py --province "Lao Cai"
    python pipeline.py --province "Ha Giang" --resolution 10

Downloads and processes all geospatial layers for landslide prediction.
All rasters normalized to same CRS, resolution, extent.
PNG maps generated for each layer overlaid on province boundary.
"""

import os
import sys
import io
import math

# Fix Windows terminal encoding + disable buffering
os.environ.setdefault('PYTHONIOENCODING', 'utf-8')
try:
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace', line_buffering=True)
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace', line_buffering=True)
except Exception:
    pass
import json
import time
import logging
import warnings
from pathlib import Path
from typing import Tuple, List, Optional, Dict

import click
import numpy as np
import geopandas as gpd
import rasterio
from rasterio.merge import merge
from rasterio.mask import mask as rasterio_mask
from rasterio.warp import calculate_default_transform, reproject, Resampling
from rasterio.features import rasterize
from rasterio.transform import from_origin
import requests
from shapely.geometry import box, mapping, LineString
from scipy.ndimage import distance_transform_edt
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
from matplotlib.patches import Patch
from matplotlib.lines import Line2D

warnings.filterwarnings('ignore')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S',
)
log = logging.getLogger(__name__)

# ════════════════════════════════════════════════════════
#  CONSTANTS
# ════════════════════════════════════════════════════════
GADM_URL = "https://geodata.ucdavis.edu/gadm/gadm4.1/json/gadm41_VNM_1.json"
COP_DEM_BASE = "https://copernicus-dem-30m.s3.amazonaws.com"
ESA_WC_BASE = "https://esa-worldcover.s3.eu-central-1.amazonaws.com/v200/2021/map"

LANDCOVER_CLASSES = {
    10: ("Tree cover", "Forest", "#006400"),
    20: ("Shrubland", "Shrubland", "#FFBB22"),
    30: ("Grassland", "Grassland", "#FFFF4C"),
    40: ("Cropland", "Agriculture", "#F096FF"),
    50: ("Built-up", "Urban", "#FA0000"),
    60: ("Bare/sparse", "Bare soil", "#B4B4B4"),
    70: ("Snow/ice", "Snow/Ice", "#F0F0F0"),
    80: ("Water", "Water", "#0064C8"),
    90: ("Herbaceous wetland", "Wetland", "#0096A0"),
    95: ("Mangroves", "Mangroves", "#00CF75"),
    100: ("Moss/lichen", "Moss/Lichen", "#FAE6A0"),
}


# ════════════════════════════════════════════════════════
#  UTILITY FUNCTIONS
# ════════════════════════════════════════════════════════
def download_file(url: str, dest: Path, desc: str = "", retries: int = 3) -> bool:
    """Download a file with retry logic and progress reporting."""
    for attempt in range(retries):
        try:
            label = desc or url.split('/')[-1]
            # HEAD request first to get file size
            try:
                head = requests.head(url, timeout=(10, 10))
                if head.status_code == 404:
                    log.warning(f"  [!] Not found (404): {label}")
                    return False
                total_size = int(head.headers.get('content-length', 0))
            except Exception:
                total_size = 0

            total_mb = total_size / (1024 * 1024) if total_size else 0
            log.info(f"  [v] Downloading {label} ({total_mb:.1f} MB)...")

            resp = requests.get(url, stream=True, timeout=(15, 60))
            if resp.status_code == 404:
                log.warning(f"  [!] Not found (404): {label}")
                return False
            resp.raise_for_status()
            dest.parent.mkdir(parents=True, exist_ok=True)

            downloaded = 0
            last_report = 0
            with open(dest, 'wb') as f:
                for chunk in resp.iter_content(chunk_size=131072):
                    f.write(chunk)
                    downloaded += len(chunk)
                    mb_now = downloaded / (1024 * 1024)
                    # Report progress every 5 MB
                    if mb_now - last_report >= 5:
                        if total_mb > 0:
                            pct = 100 * downloaded / total_size
                            log.info(f"      ... {mb_now:.0f}/{total_mb:.0f} MB ({pct:.0f}%)")
                        else:
                            log.info(f"      ... {mb_now:.0f} MB downloaded")
                        sys.stderr.flush()
                        sys.stdout.flush()
                        last_report = mb_now

            mb = dest.stat().st_size / (1024 * 1024)
            log.info(f"  [OK] Downloaded {label} ({mb:.1f} MB)")
            return True
        except Exception as e:
            log.warning(f"  Attempt {attempt+1}/{retries} failed: {e}")
            # Clean up partial download
            if dest.exists():
                dest.unlink()
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
    return False


def get_utm_epsg(lon: float, lat: float) -> int:
    """Get UTM EPSG code for a given lon/lat."""
    zone = int((lon + 180) / 6) + 1
    return 32600 + zone if lat >= 0 else 32700 + zone


def write_raster(data: np.ndarray, path: Path, crs, transform,
                 nodata: float = -9999.0, dtype='float32'):
    """Write a numpy array to GeoTIFF."""
    if data.ndim == 2:
        data = data[np.newaxis, :]
    profile = {
        'driver': 'GTiff', 'dtype': dtype,
        'width': data.shape[2], 'height': data.shape[1], 'count': data.shape[0],
        'crs': crs, 'transform': transform, 'nodata': nodata, 'compress': 'lzw',
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    with rasterio.open(path, 'w', **profile) as dst:
        dst.write(data)


def compute_target_grid(boundary: gpd.GeoDataFrame, target_crs: str, res: float) -> dict:
    """Define the standard target raster grid for all layers."""
    b = boundary.to_crs(target_crs).total_bounds
    minx = math.floor(b[0] / res) * res
    miny = math.floor(b[1] / res) * res
    maxx = math.ceil(b[2] / res) * res
    maxy = math.ceil(b[3] / res) * res
    width = int(round((maxx - minx) / res))
    height = int(round((maxy - miny) / res))
    transform = from_origin(minx, maxy, res, res)
    return {
        'bounds': (minx, miny, maxx, maxy),
        'width': width, 'height': height,
        'transform': transform, 'crs': target_crs, 'resolution': res,
    }


def resample_to_grid(src_path: Path, dst_path: Path, grid: dict,
                     resampling=Resampling.bilinear, nodata=-9999.0):
    """Reproject/resample a raster to the target grid."""
    with rasterio.open(src_path) as src:
        dst_data = np.full((1, grid['height'], grid['width']), nodata, dtype=np.float32)
        reproject(
            source=rasterio.band(src, 1), destination=dst_data[0],
            src_transform=src.transform, src_crs=src.crs,
            dst_transform=grid['transform'], dst_crs=grid['crs'],
            dst_nodata=nodata, resampling=resampling,
        )
    write_raster(dst_data, dst_path, grid['crs'], grid['transform'], nodata)


def clip_raster_to_boundary(src_path: Path, dst_path: Path,
                            boundary: gpd.GeoDataFrame, target_crs: str):
    """Clip a raster by the province boundary polygon."""
    boundary_reproj = boundary.to_crs(target_crs)
    geom = [mapping(boundary_reproj.geometry.values[0])]
    with rasterio.open(src_path) as src:
        # Use appropriate nodata for the dtype
        dtype = src.dtypes[0]
        if np.issubdtype(np.dtype(dtype), np.unsignedinteger):
            nd = 0
        elif np.issubdtype(np.dtype(dtype), np.integer):
            nd = -9999
        else:
            nd = -9999.0
        clipped, clipped_tf = rasterio_mask(src, geom, crop=True, nodata=nd)
        profile = src.profile.copy()
        profile.update({
            'height': clipped.shape[1], 'width': clipped.shape[2],
            'transform': clipped_tf, 'nodata': nd, 'compress': 'lzw',
        })
    with rasterio.open(dst_path, 'w', **profile) as dst:
        dst.write(clipped)


# ════════════════════════════════════════════════════════
#  STEP 1: PROVINCE BOUNDARY
# ════════════════════════════════════════════════════════
def step_boundary(province: str, output_dir: Path) -> gpd.GeoDataFrame:
    """Download GADM Vietnam level-1 boundary and filter."""
    log.info("═" * 60)
    log.info("📍 STEP 1: Province Boundary")
    log.info("═" * 60)

    cache = output_dir / "raw" / "gadm41_VNM_1.json"
    if not cache.exists():
        if not download_file(GADM_URL, cache, "GADM Vietnam"):
            raise RuntimeError("Failed to download GADM data")

    gdf = gpd.read_file(cache)
    q = province.lower().strip().replace(" ", "")
    # Match by removing spaces from both sides
    match = gdf[gdf['NAME_1'].str.lower().str.replace(" ", "", regex=False).str.contains(q)]

    if match.empty:
        avail = sorted(gdf['NAME_1'].tolist())
        log.error(f"Province '{province}' not found! Available:\n" + "\n".join(avail))
        raise ValueError(f"Province '{province}' not found")

    if len(match) > 1:
        log.warning(f"Multiple matches: {match['NAME_1'].tolist()}, using first.")

    boundary = match.iloc[[0]].copy()
    name = boundary.iloc[0]['NAME_1']
    log.info(f"  ✅ Province: {name}")

    out = output_dir / "boundary.geojson"
    boundary.to_file(out, driver='GeoJSON')
    log.info(f"  ✅ Saved: {out}")
    return boundary


# ════════════════════════════════════════════════════════
#  STEP 2: DEM DOWNLOAD
# ════════════════════════════════════════════════════════
def step_dem(boundary: gpd.GeoDataFrame, output_dir: Path) -> Path:
    """Download Copernicus GLO-30 DEM tiles, merge, clip."""
    log.info("═" * 60)
    log.info("🏔️  STEP 2: DEM (Copernicus GLO-30)")
    log.info("═" * 60)

    bounds = boundary.to_crs("EPSG:4326").total_bounds
    buf = 0.15  # buffer for hydrology
    bb = (bounds[0] - buf, bounds[1] - buf, bounds[2] + buf, bounds[3] + buf)

    # Determine tiles
    tiles = []
    for lat in range(int(math.floor(bb[1])), int(math.ceil(bb[3]))):
        for lon in range(int(math.floor(bb[0])), int(math.ceil(bb[2]))):
            ns = "N" if lat >= 0 else "S"
            ew = "E" if lon >= 0 else "W"
            name = f"Copernicus_DSM_COG_10_{ns}{abs(lat):02d}_00_{ew}{abs(lon):03d}_00_DEM"
            url = f"{COP_DEM_BASE}/{name}/{name}.tif"
            tiles.append((url, name))

    log.info(f"  Need {len(tiles)} DEM tile(s)")
    raw = output_dir / "raw" / "dem"
    raw.mkdir(parents=True, exist_ok=True)

    paths = []
    for url, name in tiles:
        p = raw / f"{name}.tif"
        if p.exists():
            log.info(f"  ✅ Cached: {name}")
            paths.append(p)
        elif download_file(url, p, name):
            paths.append(p)
        else:
            log.warning(f"  ⚠ Missing tile: {name}")

    if not paths:
        raise RuntimeError("No DEM tiles downloaded!")

    # Merge
    log.info("  Merging DEM tiles...")
    datasets = [rasterio.open(p) for p in paths]
    merged, merged_tf = merge(datasets)
    profile = datasets[0].profile.copy()
    for ds in datasets:
        ds.close()
    profile.update({
        'height': merged.shape[1], 'width': merged.shape[2],
        'transform': merged_tf, 'compress': 'lzw',
    })
    merged_path = output_dir / "raw" / "dem_merged.tif"
    with rasterio.open(merged_path, 'w', **profile) as dst:
        dst.write(merged)

    # Clip to boundary
    log.info("  Clipping to province...")
    dem_path = output_dir / "dem_clipped.tif"
    clip_raster_to_boundary(merged_path, dem_path, boundary, "EPSG:4326")
    log.info(f"  ✅ DEM saved: {dem_path}")
    return dem_path


# ════════════════════════════════════════════════════════
#  STEP 3: TERRAIN DERIVATIVES
# ════════════════════════════════════════════════════════
def _slope_aspect(dem: np.ndarray, cs: float):
    """Slope (degrees) and aspect (degrees)."""
    dy, dx = np.gradient(dem, cs)
    slope = np.degrees(np.arctan(np.sqrt(dx**2 + dy**2)))
    aspect = np.degrees(np.arctan2(-dy, dx))
    aspect = (90.0 - aspect) % 360.0
    return slope.astype(np.float32), aspect.astype(np.float32)


def _curvature(dem: np.ndarray, cs: float):
    """Profile curvature."""
    dy, dx = np.gradient(dem, cs)
    dyy, _ = np.gradient(dy, cs)
    _, dxx = np.gradient(dx, cs)
    return (-2.0 * (dxx + dyy)).astype(np.float32)


def _flow_accumulation(dem: np.ndarray):
    """D8 flow accumulation (vectorized where possible)."""
    log.info("    Computing flow accumulation...")
    rows, cols = dem.shape
    dr = np.array([-1, -1, -1, 0, 0, 1, 1, 1])
    dc = np.array([-1, 0, 1, -1, 1, -1, 0, 1])
    dist = np.array([1.414, 1, 1.414, 1, 1, 1.414, 1, 1.414])

    pad = np.pad(dem, 1, constant_values=np.inf)
    slopes = np.empty((8, rows, cols), dtype=np.float32)
    for i in range(8):
        nb = pad[1+dr[i]:rows+1+dr[i], 1+dc[i]:cols+1+dc[i]]
        slopes[i] = (dem - nb) / dist[i]

    flow_dir = np.argmax(slopes, axis=0).astype(np.int8)
    flow_dir[np.max(slopes, axis=0) <= 0] = -1

    # Sort by elevation descending
    order = np.argsort(dem.ravel())[::-1]
    acc = np.ones(rows * cols, dtype=np.float64)
    fd = flow_dir.ravel()

    total = len(order)
    for count, idx in enumerate(order):
        if count % 5000000 == 0 and count > 0:
            log.info(f"    ... {count}/{total} ({100*count/total:.0f}%)")
        d = fd[idx]
        if d < 0:
            continue
        r, c = divmod(int(idx), cols)
        nr, nc = r + dr[d], c + dc[d]
        if 0 <= nr < rows and 0 <= nc < cols:
            acc[nr * cols + nc] += acc[idx]

    log.info(f"    ✅ Flow accumulation done (max={acc.max():.0f})")
    return acc.reshape(rows, cols).astype(np.float32)


def step_terrain(dem_path: Path, output_dir: Path,
                 boundary: gpd.GeoDataFrame, target_crs: str) -> Dict[str, Path]:
    """Compute slope, aspect, curvature, flow accumulation, TWI."""
    log.info("═" * 60)
    log.info("⛰️  STEP 3: Terrain Derivatives")
    log.info("═" * 60)

    # Reproject DEM to UTM at native resolution
    log.info("  Reprojecting DEM to UTM...")
    with rasterio.open(dem_path) as src:
        tf, w, h = calculate_default_transform(
            src.crs, target_crs, src.width, src.height, *src.bounds)
        dem_utm = np.full((h, w), -9999, dtype=np.float32)
        reproject(
            source=rasterio.band(src, 1), destination=dem_utm,
            src_transform=src.transform, src_crs=src.crs,
            dst_transform=tf, dst_crs=target_crs,
            dst_nodata=-9999, resampling=Resampling.bilinear,
        )

    cs = abs(tf[0])
    log.info(f"  DEM UTM: {w}x{h}, cellsize={cs:.1f}m")

    # Replace nodata with NaN
    dem_data = dem_utm.copy()
    dem_data[dem_data <= -9999] = np.nan

    # Fill NaN with nearest neighbor
    valid = ~np.isnan(dem_data)
    if not valid.all():
        log.info("  Filling NoData gaps...")
        from scipy.interpolate import NearestNDInterpolator
        coords = np.argwhere(valid)
        vals = dem_data[valid]
        if len(coords) > 0:
            interp = NearestNDInterpolator(coords, vals)
            nan_idx = np.argwhere(~valid)
            if len(nan_idx) > 0:
                dem_data[~valid] = interp(nan_idx)

    nodata_mask = dem_utm <= -9999

    log.info("  Computing slope & aspect...")
    slope, aspect = _slope_aspect(dem_data, cs)

    log.info("  Computing curvature...")
    curvature = _curvature(dem_data, cs)

    flow_acc = _flow_accumulation(dem_data)

    log.info("  Computing TWI...")
    a = flow_acc * cs
    slope_rad = np.radians(np.maximum(slope, 0.001))
    twi = np.log(a / np.tan(slope_rad)).astype(np.float32)
    twi = np.clip(twi, -5, 30)

    # Apply nodata mask
    for arr in [dem_utm, slope, aspect, curvature, flow_acc, twi]:
        arr[nodata_mask] = -9999

    # Save native-resolution rasters
    native = output_dir / "native"
    native.mkdir(exist_ok=True)
    results = {}
    for name, data in [('dem', dem_utm), ('slope', slope), ('aspect', aspect),
                       ('curvature', curvature), ('flow_accumulation', flow_acc), ('twi', twi)]:
        p = native / f"{name}.tif"
        write_raster(data, p, target_crs, tf)
        results[name] = p
        log.info(f"  ✅ {name}: {p}")

    return results


# ════════════════════════════════════════════════════════
#  STEP 4: CONTOUR LINES
# ════════════════════════════════════════════════════════
def step_contour(dem_native_path: Path, output_dir: Path, interval: float = 10.0) -> Optional[Path]:
    """Generate contour lines from DEM."""
    log.info("═" * 60)
    log.info(f"📐 STEP 4: Contour Lines (interval={interval}m)")
    log.info("═" * 60)

    with rasterio.open(dem_native_path) as src:
        dem = src.read(1)
        tf = src.transform
        crs = src.crs
        nd = src.nodata

    dem_plot = dem.copy()
    if nd is not None:
        dem_plot[dem == nd] = np.nan

    # Build coordinate arrays
    rows, cols = dem_plot.shape
    xs = np.array([tf[2] + (c + 0.5) * tf[0] for c in range(cols)])
    ys = np.array([tf[5] + (r + 0.5) * tf[4] for r in range(rows)])
    X, Y = np.meshgrid(xs, ys)

    vmin, vmax = np.nanmin(dem_plot), np.nanmax(dem_plot)
    levels = np.arange(math.floor(vmin / interval) * interval, vmax, interval)

    fig, ax = plt.subplots()
    cs = ax.contour(X, Y, dem_plot, levels=levels)
    plt.close(fig)

    features = []
    for i, lev in enumerate(cs.levels):
        segs = cs.allsegs[i]
        for seg in segs:
            if len(seg) >= 2:
                features.append({'geometry': LineString(seg), 'elevation': float(lev)})

    if not features:
        log.warning("  ⚠ No contour lines generated")
        return None

    gdf = gpd.GeoDataFrame(features, crs=crs)
    out = output_dir / "contour.shp"
    gdf.to_file(out, driver='ESRI Shapefile')
    log.info(f"  ✅ Contour: {out} ({len(features)} lines)")
    return out


# ════════════════════════════════════════════════════════
#  STEP 5: OSM DATA
# ════════════════════════════════════════════════════════
def step_osm(boundary: gpd.GeoDataFrame, output_dir: Path) -> Dict[str, Optional[Path]]:
    """Download roads, rivers, infrastructure from OSM."""
    log.info("═" * 60)
    log.info("🛣️  STEP 5: OSM Data (Roads, Rivers, Infrastructure)")
    log.info("═" * 60)

    import osmnx as ox
    polygon = boundary.to_crs("EPSG:4326").geometry.values[0]
    results = {}

    layers = [
        ('roads', {"highway": True}),
        ('rivers', {"waterway": True}),
        ('infrastructure', {"amenity": True, "building": True}),
    ]

    for name, tags in layers:
        try:
            log.info(f"  Downloading {name}...")
            gdf = ox.features_from_polygon(polygon, tags=tags)
            if gdf.empty:
                log.warning(f"  ⚠ No {name} found")
                results[name] = None
                continue

            # Keep only relevant columns
            keep = [c for c in gdf.columns if c in
                    ['geometry', 'name', 'highway', 'waterway', 'amenity', 'building']]
            gdf = gdf[keep].copy()
            out = output_dir / f"{name}.gpkg"
            gdf.to_file(out, driver='GPKG')
            log.info(f"  ✅ {name}: {out} ({len(gdf)} features)")
            results[name] = out
        except Exception as e:
            log.error(f"  ❌ Failed to download {name}: {e}")
            results[name] = None

    return results


# ════════════════════════════════════════════════════════
#  STEP 6: ESA WORLDCOVER
# ════════════════════════════════════════════════════════
def step_landcover(boundary: gpd.GeoDataFrame, output_dir: Path) -> Optional[Path]:
    """Download ESA WorldCover 10m tiles, merge, clip."""
    log.info("═" * 60)
    log.info("🌿 STEP 6: Land Cover (ESA WorldCover 10m)")
    log.info("═" * 60)

    bounds = boundary.to_crs("EPSG:4326").total_bounds
    tiles = []
    # WorldCover tiles are 3°x3°, named by SW corner (multiples of 3)
    for lat in range(int(math.floor(bounds[1] / 3) * 3),
                     int(math.ceil(bounds[3] / 3) * 3), 3):
        for lon in range(int(math.floor(bounds[0] / 3) * 3),
                         int(math.ceil(bounds[2] / 3) * 3), 3):
            ns = "N" if lat >= 0 else "S"
            ew = "E" if lon >= 0 else "W"
            name = f"ESA_WorldCover_10m_2021_v200_{ns}{abs(lat):02d}{ew}{abs(lon):03d}_Map"
            url = f"{ESA_WC_BASE}/{name}.tif"
            tiles.append((url, name))

    log.info(f"  Need {len(tiles)} WorldCover tile(s)")
    raw = output_dir / "raw" / "worldcover"
    raw.mkdir(parents=True, exist_ok=True)

    paths = []
    for url, name in tiles:
        p = raw / f"{name}.tif"
        if p.exists():
            log.info(f"  ✅ Cached: {name}")
            paths.append(p)
        elif download_file(url, p, name):
            paths.append(p)

    if not paths:
        log.error("  ❌ No WorldCover tiles downloaded")
        return None

    # Merge
    log.info("  Merging WorldCover tiles...")
    datasets = [rasterio.open(p) for p in paths]
    merged, merged_tf = merge(datasets)
    profile = datasets[0].profile.copy()
    for ds in datasets:
        ds.close()
    profile.update({
        'height': merged.shape[1], 'width': merged.shape[2],
        'transform': merged_tf, 'compress': 'lzw',
    })
    merged_path = output_dir / "raw" / "wc_merged.tif"
    with rasterio.open(merged_path, 'w', **profile) as dst:
        dst.write(merged)

    # Clip
    lc_path = output_dir / "native" / "landcover.tif"
    lc_path.parent.mkdir(exist_ok=True)
    clip_raster_to_boundary(merged_path, lc_path, boundary, "EPSG:4326")
    log.info(f"  ✅ Landcover: {lc_path}")
    return lc_path


# ════════════════════════════════════════════════════════
#  STEP 7: DISTANCE RASTERS
# ════════════════════════════════════════════════════════
def step_distance(osm_results: dict, grid: dict, boundary: gpd.GeoDataFrame,
                  output_dir: Path) -> Dict[str, Optional[Path]]:
    """Generate distance-to-road and distance-to-river rasters."""
    log.info("═" * 60)
    log.info("📏 STEP 7: Distance Rasters")
    log.info("═" * 60)

    results = {}
    target_crs = grid['crs']

    for layer in ['roads', 'rivers']:
        src_path = osm_results.get(layer)
        if src_path is None:
            log.warning(f"  ⚠ Skipping dist_{layer} (no data)")
            results[f'dist_{layer.rstrip("s")}'] = None
            continue

        log.info(f"  Computing distance to {layer}...")
        gdf = gpd.read_file(src_path).to_crs(target_crs)

        # Filter to lines and polygons only
        gdf = gdf[gdf.geometry.type.isin(['LineString', 'MultiLineString',
                                           'Polygon', 'MultiPolygon', 'Point'])]
        if gdf.empty:
            log.warning(f"  ⚠ No valid geometries for {layer}")
            results[f'dist_{layer.rstrip("s")}'] = None
            continue

        # Rasterize features
        shapes_iter = [(geom, 1) for geom in gdf.geometry]
        rasterized = rasterize(
            shapes_iter,
            out_shape=(grid['height'], grid['width']),
            transform=grid['transform'],
            fill=0, dtype='uint8',
        )

        # Distance transform (in pixels, then multiply by resolution)
        binary = (rasterized == 0).astype(np.float32)
        dist = distance_transform_edt(binary) * grid['resolution']
        dist = dist.astype(np.float32)

        # Mask outside boundary
        boundary_utm = boundary.to_crs(target_crs)
        bmask = rasterize(
            [(mapping(boundary_utm.geometry.values[0]), 1)],
            out_shape=(grid['height'], grid['width']),
            transform=grid['transform'],
            fill=0, dtype='uint8',
        )
        dist[bmask == 0] = -9999

        name = f"dist_{layer.rstrip('s')}"
        out = output_dir / f"{name}.tif"
        write_raster(dist, out, target_crs, grid['transform'])
        results[name] = out
        log.info(f"  ✅ {name}: {out}")

    return results


# ════════════════════════════════════════════════════════
#  STEP 8: NORMALIZE ALL RASTERS TO TARGET GRID
# ════════════════════════════════════════════════════════
def step_normalize(terrain_paths: Dict[str, Path], lc_path: Optional[Path],
                   grid: dict, boundary: gpd.GeoDataFrame,
                   output_dir: Path) -> Dict[str, Path]:
    """Resample all rasters to the target 5m grid."""
    log.info("═" * 60)
    log.info(f"🔄 STEP 8: Normalize to {grid['resolution']}m grid")
    log.info("═" * 60)

    # Province mask for clipping
    boundary_utm = boundary.to_crs(grid['crs'])
    bmask = rasterize(
        [(mapping(boundary_utm.geometry.values[0]), 1)],
        out_shape=(grid['height'], grid['width']),
        transform=grid['transform'],
        fill=0, dtype='uint8',
    )

    results = {}

    # Terrain layers (bilinear)
    for name, src in terrain_paths.items():
        dst = output_dir / f"{name}.tif"
        log.info(f"  Resampling {name}...")
        rs = Resampling.nearest if name == 'flow_accumulation' else Resampling.bilinear
        resample_to_grid(src, dst, grid, resampling=rs)

        # Apply province mask
        with rasterio.open(dst, 'r+') as ds:
            data = ds.read(1)
            data[bmask == 0] = -9999
            ds.write(data, 1)

        results[name] = dst
        log.info(f"  ✅ {name}: {dst}")

    # Landcover (nearest neighbor)
    if lc_path:
        dst = output_dir / "landcover.tif"
        log.info("  Resampling landcover...")
        resample_to_grid(lc_path, dst, grid, resampling=Resampling.nearest, nodata=0)

        with rasterio.open(dst, 'r+') as ds:
            data = ds.read(1)
            data[bmask == 0] = 0
            ds.write(data, 1)

        results['landcover'] = dst
        log.info(f"  ✅ landcover: {dst}")

    return results


# ════════════════════════════════════════════════════════
#  STEP 9: PNG MAP GENERATION
# ════════════════════════════════════════════════════════
def _make_fig(boundary_utm: gpd.GeoDataFrame, title: str):
    """Create a figure with province boundary."""
    fig, ax = plt.subplots(1, 1, figsize=(12, 10))
    boundary_utm.boundary.plot(ax=ax, color='#333333', linewidth=1.5, zorder=10)
    ax.set_title(title, fontsize=16, fontweight='bold', pad=15)
    ax.set_xlabel('Easting (m)', fontsize=10)
    ax.set_ylabel('Northing (m)', fontsize=10)
    ax.ticklabel_format(style='plain')
    ax.tick_params(labelsize=8)
    return fig, ax


def _save_raster_map(raster_path: Path, boundary: gpd.GeoDataFrame,
                     target_crs: str, title: str, out_png: Path,
                     cmap='terrain', vmin=None, vmax=None, label=''):
    """Generate PNG for a raster layer."""
    boundary_utm = boundary.to_crs(target_crs)
    fig, ax = _make_fig(boundary_utm, title)

    with rasterio.open(raster_path) as src:
        data = src.read(1)
        tf = src.transform
        nd = src.nodata

    data_plot = data.astype(float)
    if nd is not None:
        data_plot[data == nd] = np.nan

    extent = [tf[2], tf[2] + tf[0] * data.shape[1],
              tf[5] + tf[4] * data.shape[0], tf[5]]

    if vmin is None:
        vmin = np.nanpercentile(data_plot, 2)
    if vmax is None:
        vmax = np.nanpercentile(data_plot, 98)

    im = ax.imshow(data_plot, extent=extent, cmap=cmap, vmin=vmin, vmax=vmax,
                   origin='upper', alpha=0.9, zorder=1)
    cbar = fig.colorbar(im, ax=ax, fraction=0.03, pad=0.04)
    if label:
        cbar.set_label(label, fontsize=10)

    ax.set_xlim(extent[0], extent[1])
    ax.set_ylim(extent[2], extent[3])
    fig.tight_layout()
    out_png.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_png, dpi=150, bbox_inches='tight', facecolor='white')
    plt.close(fig)
    log.info(f"  🗺️  {out_png.name}")


def _save_landcover_map(raster_path: Path, boundary: gpd.GeoDataFrame,
                        target_crs: str, out_png: Path):
    """Generate PNG for landcover (categorical)."""
    boundary_utm = boundary.to_crs(target_crs)
    fig, ax = _make_fig(boundary_utm, "Land Cover (ESA WorldCover)")

    with rasterio.open(raster_path) as src:
        data = src.read(1)
        tf = src.transform

    extent = [tf[2], tf[2] + tf[0] * data.shape[1],
              tf[5] + tf[4] * data.shape[0], tf[5]]

    # Build colormap
    classes = sorted(LANDCOVER_CLASSES.keys())
    colors_list = [LANDCOVER_CLASSES[c][2] for c in classes]
    cmap = mcolors.ListedColormap(colors_list)
    bounds_cm = classes + [max(classes) + 10]
    norm = mcolors.BoundaryNorm(bounds_cm, cmap.N)

    data_plot = data.astype(float)
    data_plot[data == 0] = np.nan

    ax.imshow(data_plot, extent=extent, cmap=cmap, norm=norm,
              origin='upper', alpha=0.9, zorder=1, interpolation='nearest')

    # Legend
    patches = [Patch(facecolor=LANDCOVER_CLASSES[c][2], label=LANDCOVER_CLASSES[c][1])
               for c in classes if c in np.unique(data)]
    ax.legend(handles=patches, loc='lower right', fontsize=8, framealpha=0.9)

    ax.set_xlim(extent[0], extent[1])
    ax.set_ylim(extent[2], extent[3])
    fig.tight_layout()
    out_png.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_png, dpi=150, bbox_inches='tight', facecolor='white')
    plt.close(fig)
    log.info(f"  🗺️  {out_png.name}")


def _save_vector_map(gpkg_path: Path, boundary: gpd.GeoDataFrame,
                     target_crs: str, title: str, out_png: Path,
                     color='red', linewidth=0.5):
    """Generate PNG for a vector layer."""
    boundary_utm = boundary.to_crs(target_crs)
    fig, ax = _make_fig(boundary_utm, title)

    gdf = gpd.read_file(gpkg_path).to_crs(target_crs)

    # Clip to boundary
    from shapely.ops import unary_union
    clip_geom = unary_union(boundary_utm.geometry)
    gdf = gpd.clip(gdf, clip_geom)

    if not gdf.empty:
        geom_types = gdf.geometry.type.unique()
        if any(t in ['Point', 'MultiPoint'] for t in geom_types):
            gdf.plot(ax=ax, color=color, markersize=1, alpha=0.7, zorder=5)
        else:
            gdf.plot(ax=ax, color=color, linewidth=linewidth, alpha=0.8, zorder=5)

    b = boundary_utm.total_bounds
    ax.set_xlim(b[0], b[2])
    ax.set_ylim(b[1], b[3])
    fig.tight_layout()
    out_png.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_png, dpi=150, bbox_inches='tight', facecolor='white')
    plt.close(fig)
    log.info(f"  🗺️  {out_png.name}")


def _save_contour_map(shp_path: Path, boundary: gpd.GeoDataFrame,
                      target_crs: str, out_png: Path):
    """Generate PNG for contour lines."""
    boundary_utm = boundary.to_crs(target_crs)
    fig, ax = _make_fig(boundary_utm, "Contour Lines (10m interval)")

    gdf = gpd.read_file(shp_path).to_crs(target_crs)
    if not gdf.empty:
        gdf.plot(ax=ax, column='elevation', cmap='terrain', linewidth=0.3,
                 alpha=0.8, zorder=5, legend=True,
                 legend_kwds={'label': 'Elevation (m)', 'shrink': 0.6})

    b = boundary_utm.total_bounds
    ax.set_xlim(b[0], b[2])
    ax.set_ylim(b[1], b[3])
    fig.tight_layout()
    out_png.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_png, dpi=150, bbox_inches='tight', facecolor='white')
    plt.close(fig)
    log.info(f"  🗺️  {out_png.name}")


def step_maps(output_dir: Path, boundary: gpd.GeoDataFrame, target_crs: str,
              raster_paths: dict, osm_paths: dict,
              dist_paths: dict, contour_path: Optional[Path]):
    """Generate PNG maps for all layers."""
    log.info("═" * 60)
    log.info("🗺️  STEP 9: PNG Map Generation")
    log.info("═" * 60)

    maps_dir = output_dir / "maps"

    # Raster maps
    raster_configs = {
        'dem':               ('DEM (Elevation)',        'terrain',   'Elevation (m)'),
        'slope':             ('Slope',                  'Reds',      'Degrees'),
        'aspect':            ('Aspect',                 'hsv',       'Degrees'),
        'curvature':         ('Curvature',              'RdBu_r',   'Curvature'),
        'flow_accumulation': ('Flow Accumulation',      'Blues',     'Cells'),
        'twi':               ('Topographic Wetness Index', 'YlGnBu', 'TWI'),
    }
    for name, (title, cmap, label) in raster_configs.items():
        if name in raster_paths and raster_paths[name].exists():
            vmin, vmax = None, None
            if name == 'flow_accumulation':
                vmin, vmax = 1, None  # log scale would be better
            _save_raster_map(
                raster_paths[name], boundary, target_crs,
                title, maps_dir / f"{name}.png", cmap=cmap, label=label,
                vmin=vmin, vmax=vmax,
            )

    # Landcover
    if 'landcover' in raster_paths and raster_paths['landcover'].exists():
        _save_landcover_map(raster_paths['landcover'], boundary, target_crs,
                            maps_dir / "landcover.png")

    # Distance rasters
    for name, path in dist_paths.items():
        if path and path.exists():
            title = name.replace('_', ' ').title()
            _save_raster_map(path, boundary, target_crs, title,
                             maps_dir / f"{name}.png",
                             cmap='YlOrRd_r', label='Distance (m)')

    # Vector maps
    vec_configs = {
        'roads': ('Roads (OpenStreetMap)', '#E63946', 0.3),
        'rivers': ('Rivers (OpenStreetMap)', '#1D3557', 0.8),
        'infrastructure': ('Infrastructure (OpenStreetMap)', '#E9C46A', 0.5),
    }
    for name, (title, color, lw) in vec_configs.items():
        if name in osm_paths and osm_paths[name] and osm_paths[name].exists():
            _save_vector_map(osm_paths[name], boundary, target_crs,
                             title, maps_dir / f"{name}.png", color=color, linewidth=lw)

    # Contour
    if contour_path and contour_path.exists():
        _save_contour_map(contour_path, boundary, target_crs,
                          maps_dir / "contour.png")


# ════════════════════════════════════════════════════════
#  STEP 10: RASTER STACK
# ════════════════════════════════════════════════════════
def step_stack(raster_paths: dict, dist_paths: dict, output_dir: Path, grid: dict):
    """Build a VRT stack of all rasters."""
    log.info("═" * 60)
    log.info("📦 STEP 10: Raster Stack")
    log.info("═" * 60)

    all_paths = []
    band_names = []
    for name in ['dem', 'slope', 'aspect', 'curvature', 'flow_accumulation',
                 'twi', 'landcover']:
        p = raster_paths.get(name)
        if p and p.exists():
            all_paths.append(str(p))
            band_names.append(name)

    for name, p in dist_paths.items():
        if p and p.exists():
            all_paths.append(str(p))
            band_names.append(name)

    if not all_paths:
        log.warning("  ⚠ No rasters to stack")
        return

    # Write VRT
    vrt_path = output_dir / "stack.vrt"
    vrt_lines = ['<VRTDataset>']
    for i, (path, bname) in enumerate(zip(all_paths, band_names), 1):
        with rasterio.open(path) as src:
            w, h = src.width, src.height
            if i == 1:
                vrt_lines[0] = f'<VRTDataset rasterXSize="{w}" rasterYSize="{h}">'
        vrt_lines.append(f'  <VRTRasterBand dataType="Float32" band="{i}">')
        vrt_lines.append(f'    <Description>{bname}</Description>')
        vrt_lines.append(f'    <SimpleSource>')
        vrt_lines.append(f'      <SourceFilename relativeToVRT="1">{Path(path).name}</SourceFilename>')
        vrt_lines.append(f'      <SourceBand>1</SourceBand>')
        vrt_lines.append(f'    </SimpleSource>')
        vrt_lines.append(f'  </VRTRasterBand>')
    vrt_lines.append('</VRTDataset>')

    vrt_path.write_text('\n'.join(vrt_lines), encoding='utf-8')
    log.info(f"  ✅ Stack VRT: {vrt_path} ({len(band_names)} bands)")
    log.info(f"  Bands: {', '.join(band_names)}")


# ════════════════════════════════════════════════════════
#  MAIN CLI
# ════════════════════════════════════════════════════════
@click.command()
@click.option('--province', required=True, help='Province name (Vietnamese)')
@click.option('--resolution', default=5.0, type=float, help='Target resolution (m)')
@click.option('--contour-interval', default=10.0, type=float, help='Contour interval (m)')
@click.option('--output-dir', default=None, type=str, help='Output directory')
def main(province, resolution, contour_interval, output_dir):
    """
    Provincial Landslide Prediction GIS Pipeline (Vietnam).

    Downloads and processes all geospatial layers for a province.
    """
    log.info("[*] LANDSLIDE PREDICTION GIS PIPELINE")
    log.info(f"   Province: {province}")
    log.info(f"   Resolution: {resolution}m")
    log.info("")

    # Setup output directory
    safe_name = province.replace(" ", "_").replace(".", "")
    if output_dir is None:
        output_dir = Path("data") / "province" / safe_name
    else:
        output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    log.info(f"   Output: {output_dir.resolve()}")
    log.info("")

    # ── Step 1: Boundary ──
    boundary = step_boundary(province, output_dir)

    # Determine UTM CRS
    centroid = boundary.to_crs("EPSG:4326").geometry.centroid.iloc[0]
    target_crs = f"EPSG:{get_utm_epsg(centroid.x, centroid.y)}"
    log.info(f"  Target CRS: {target_crs}")

    # ── Step 2: DEM ──
    dem_path = step_dem(boundary, output_dir)

    # ── Step 3: Terrain ──
    terrain_paths = step_terrain(dem_path, output_dir, boundary, target_crs)

    # ── Step 4: Contour ──
    contour_path = step_contour(terrain_paths['dem'], output_dir, contour_interval)

    # ── Step 5: OSM ──
    osm_paths = step_osm(boundary, output_dir)

    # ── Step 6: WorldCover ──
    lc_path = step_landcover(boundary, output_dir)

    # ── Target grid ──
    grid = compute_target_grid(boundary, target_crs, resolution)
    log.info(f"\n  Target grid: {grid['width']}x{grid['height']} @ {resolution}m")

    # ── Step 7: Distance rasters ──
    dist_paths = step_distance(osm_paths, grid, boundary, output_dir)

    # ── Step 8: Normalize ──
    all_rasters = step_normalize(terrain_paths, lc_path, grid, boundary, output_dir)

    # ── Step 9: Maps ──
    step_maps(output_dir, boundary, target_crs, all_rasters, osm_paths,
              dist_paths, contour_path)

    # ── Step 10: Stack ──
    step_stack(all_rasters, dist_paths, output_dir, grid)

    # ── Summary ──
    log.info("")
    log.info("═" * 60)
    log.info("🎉 PIPELINE COMPLETE!")
    log.info("═" * 60)
    log.info(f"  Output directory: {output_dir.resolve()}")
    log.info("")
    log.info("  📁 Files generated:")
    for f in sorted(output_dir.rglob("*")):
        if f.is_file() and 'raw' not in str(f) and 'native' not in str(f):
            size = f.stat().st_size / (1024 * 1024)
            log.info(f"     {f.relative_to(output_dir)}  ({size:.1f} MB)")


if __name__ == '__main__':
    main()
