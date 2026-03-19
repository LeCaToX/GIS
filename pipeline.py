#!/usr/bin/env python3
"""
=========================================================
 Provincial GIS Pipeline (Vietnam)
 Supports 2025 Province Merger (NQ 202/2025/QH15)
=========================================================
Usage:
    python pipeline.py --province "Quảng Ngãi"                       # 2025 merged
    python pipeline.py --province "Hồ Chí Minh" --resolution 10     # 2025 merged
    python pipeline.py --province "Lào Cai" --legacy-boundaries      # Old GADM boundary
    python pipeline.py --list-provinces                              # Show all provinces

Downloads and processes geospatial + socioeconomic layers:
  - Terrain: DEM, slope, aspect, curvature, flow accumulation, TWI
  - Landcover: ESA WorldCover 10m
  - Infrastructure: OSM roads, rivers, buildings
  - Demographics: WorldPop population density
  - Socioeconomic report: area, population, urbanization, land use

2025 Merger: Automatically dissolves old province boundaries into the
new 34-unit administrative structure per Nghị quyết 202/2025/QH15.
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
import pandas as pd
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

# WorldPop population raster (Vietnam 2020, 1km UN-adjusted)
WORLDPOP_URL = (
    "https://data.worldpop.org/GIS/Population/"
    "Global_2000_2020_1km_UNadj/2020/VNM/"
    "vnm_ppp_2020_1km_Aggregated_UNadj.tif"
)
WORLDPOP_FALLBACK_URL = (
    "https://data.worldpop.org/GIS/Population/"
    "Global_2000_2020_1km/2020/VNM/"
    "vnm_ppp_2020_1km_Aggregated.tif"
)

# ════════════════════════════════════════════════════════
#  PROVINCE MERGER 2025 (Nghị quyết 202/2025/QH15)
#  63 tỉnh/thành → 34 đơn vị (28 tỉnh + 6 TP trực thuộc TW)
#  Effective: 01/07/2025
# ════════════════════════════════════════════════════════
PROVINCE_MERGER_2025 = {
    # --- 23 tỉnh/thành mới hình thành (sáp nhập) ---
    "Tuyên Quang":  ["Hà Giang", "Tuyên Quang"],
    "Lào Cai":      ["Yên Bái", "Lào Cai"],
    "Thái Nguyên":  ["Bắc Kạn", "Thái Nguyên"],
    "Phú Thọ":      ["Vĩnh Phúc", "Hòa Bình", "Phú Thọ"],
    "Bắc Ninh":     ["Bắc Giang", "Bắc Ninh"],
    "Hải Phòng":    ["Hải Dương", "Hải Phòng"],
    "Hưng Yên":     ["Thái Bình", "Hưng Yên"],
    "Ninh Bình":    ["Hà Nam", "Nam Định", "Ninh Bình"],
    "Quảng Trị":    ["Quảng Bình", "Quảng Trị"],
    "Đà Nẵng":      ["Quảng Nam", "Đà Nẵng"],
    "Quảng Ngãi":   ["Kon Tum", "Quảng Ngãi"],
    "Gia Lai":      ["Bình Định", "Gia Lai"],
    "Khánh Hòa":    ["Ninh Thuận", "Khánh Hòa"],
    "Lâm Đồng":     ["Đắk Nông", "Bình Thuận", "Lâm Đồng"],
    "Đắk Lắk":      ["Phú Yên", "Đắk Lắk"],
    "Hồ Chí Minh":  ["Bình Dương", "Bà Rịa - Vũng Tàu", "Hồ Chí Minh"],
    "Đồng Nai":     ["Bình Phước", "Đồng Nai"],
    "Tây Ninh":     ["Long An", "Tây Ninh"],
    "Cần Thơ":      ["Sóc Trăng", "Hậu Giang", "Cần Thơ"],
    "Vĩnh Long":    ["Bến Tre", "Trà Vinh", "Vĩnh Long"],
    "Đồng Tháp":    ["Tiền Giang", "Đồng Tháp"],
    "Cà Mau":       ["Bạc Liêu", "Cà Mau"],
    "An Giang":     ["Kiên Giang", "An Giang"],
    # --- 11 tỉnh/thành giữ nguyên ---
    "Cao Bằng":     ["Cao Bằng"],
    "Điện Biên":    ["Điện Biên"],
    "Hà Tĩnh":     ["Hà Tĩnh"],
    "Lai Châu":     ["Lai Châu"],
    "Lạng Sơn":     ["Lạng Sơn"],
    "Nghệ An":      ["Nghệ An"],
    "Quảng Ninh":   ["Quảng Ninh"],
    "Thanh Hóa":    ["Thanh Hóa"],
    "Sơn La":       ["Sơn La"],
    "Hà Nội":       ["Hà Nội"],
    "Huế":          ["Thừa Thiên Huế"],
}


# ════════════════════════════════════════════════════════
#  UTILITY FUNCTIONS
# ════════════════════════════════════════════════════════
def _normalize_vn(name: str) -> str:
    """Normalize Vietnamese province name for fuzzy matching.
    Strips spaces, dashes, dots, and lowercases. Keeps diacritics."""
    return name.lower().replace(" ", "").replace("-", "").replace(".", "")


def _find_merger_match(province: str) -> Optional[Tuple[str, List[str]]]:
    """Check if province matches any entry in PROVINCE_MERGER_2025.
    Returns (new_name, [old_names]) or None."""
    q = _normalize_vn(province)
    for new_name, old_names in PROVINCE_MERGER_2025.items():
        if _normalize_vn(new_name) == q or q in _normalize_vn(new_name):
            return new_name, old_names
    for new_name, old_names in PROVINCE_MERGER_2025.items():
        for old in old_names:
            if _normalize_vn(old) == q or q in _normalize_vn(old):
                return new_name, old_names
    return None


def _match_gadm_province(gdf: gpd.GeoDataFrame, name: str) -> gpd.GeoDataFrame:
    """Find GADM features matching a province name (flexible)."""
    q = _normalize_vn(name)
    normed = gdf['NAME_1'].apply(_normalize_vn)
    exact = gdf[normed == q]
    if not exact.empty:
        return exact
    partial = gdf[normed.str.contains(q)]
    if not partial.empty:
        return partial.iloc[[0]]
    return gpd.GeoDataFrame()


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
def step_boundary(province: str, output_dir: Path,
                  legacy: bool = False) -> Tuple[gpd.GeoDataFrame, Optional[gpd.GeoDataFrame]]:
    """Download GADM Vietnam level-1 boundary and filter.

    With 2025 merger support: dissolves constituent province boundaries.
    Returns (merged_boundary, internal_boundaries_or_None).
    """
    log.info("═" * 60)
    log.info("📍 STEP 1: Province Boundary")
    log.info("═" * 60)

    cache = output_dir / "raw" / "gadm41_VNM_1.json"
    if not cache.exists():
        if not download_file(GADM_URL, cache, "GADM Vietnam"):
            raise RuntimeError("Failed to download GADM data")

    gdf = gpd.read_file(cache)

    # --- Try 2025 merger match first ---
    merger_match = None if legacy else _find_merger_match(province)
    internal_boundaries = None

    if merger_match:
        new_name, old_names = merger_match
        log.info(f"  🔄 Merger 2025: {new_name} ← {', '.join(old_names)}")

        parts = []
        for old in old_names:
            m = _match_gadm_province(gdf, old)
            if m.empty:
                log.warning(f"  ⚠ GADM match not found for '{old}', skipping")
            else:
                log.info(f"    ✅ Found: {m.iloc[0]['NAME_1']}")
                parts.append(m)

        if not parts:
            raise ValueError(f"No GADM provinces found for merger '{new_name}'")

        constituents = gpd.GeoDataFrame(
            pd.concat(parts, ignore_index=True), crs=gdf.crs)

        if len(constituents) > 1:
            internal_boundaries = constituents[['NAME_1', 'geometry']].copy()

        from shapely.ops import unary_union
        merged_geom = unary_union(constituents.geometry)
        boundary = gpd.GeoDataFrame(
            [{'NAME_1': new_name, 'geometry': merged_geom}],
            crs=gdf.crs,
        )

        area_km2 = boundary.to_crs(epsg=3405).geometry.area.sum() / 1e6
        log.info(f"  ✅ Merged province: {new_name}")
        log.info(f"     Constituents: {len(constituents)} old provinces")
        log.info(f"     Total area: {area_km2:,.0f} km²")

    else:
        # --- Legacy single-province match ---
        q = province.lower().strip().replace(" ", "")
        match = gdf[gdf['NAME_1'].str.lower().str.replace(
            " ", "", regex=False).str.contains(q)]

        if match.empty:
            merged_names = sorted(PROVINCE_MERGER_2025.keys())
            old_names = sorted(gdf['NAME_1'].tolist())
            log.error(
                f"Province '{province}' not found!\n"
                f"  2025 provinces (34): {', '.join(merged_names)}\n"
                f"  GADM provinces (63): {', '.join(old_names)}")
            raise ValueError(f"Province '{province}' not found")

        if len(match) > 1:
            log.warning(f"Multiple matches: {match['NAME_1'].tolist()}, using first.")

        boundary = match.iloc[[0]].copy()
        name = boundary.iloc[0]['NAME_1']
        log.info(f"  ✅ Province (legacy): {name}")

    out = output_dir / "boundary.geojson"
    boundary.to_file(out, driver='GeoJSON')
    log.info(f"  ✅ Saved: {out}")

    if internal_boundaries is not None and len(internal_boundaries) > 1:
        ib_out = output_dir / "internal_boundaries.geojson"
        internal_boundaries.to_file(ib_out, driver='GeoJSON')
        log.info(f"  ✅ Internal boundaries: {ib_out}")

    return boundary, internal_boundaries


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
#  STEP 6b: POPULATION (WorldPop)
# ════════════════════════════════════════════════════════
def step_population(boundary: gpd.GeoDataFrame, output_dir: Path) -> Optional[Path]:
    """Download WorldPop population raster for Vietnam, clip to province."""
    log.info("═" * 60)
    log.info("👥 STEP 6b: Population (WorldPop 2020 1km)")
    log.info("═" * 60)

    raw = output_dir / "raw" / "worldpop"
    raw.mkdir(parents=True, exist_ok=True)
    wp_file = raw / "vnm_ppp_2020_1km.tif"

    if not wp_file.exists():
        ok = download_file(WORLDPOP_URL, wp_file, "WorldPop Vietnam 1km")
        if not ok:
            log.info("  Trying fallback URL...")
            ok = download_file(WORLDPOP_FALLBACK_URL, wp_file, "WorldPop Vietnam 1km (fallback)")
        if not ok:
            log.warning(
                "  ⚠ WorldPop download failed. You can manually download from:\n"
                f"    {WORLDPOP_URL}\n"
                f"    and place it at: {wp_file}")
            return None
    else:
        log.info(f"  ✅ Cached: {wp_file.name}")

    pop_path = output_dir / "native" / "population.tif"
    pop_path.parent.mkdir(exist_ok=True)
    clip_raster_to_boundary(wp_file, pop_path, boundary, "EPSG:4326")
    log.info(f"  ✅ Population: {pop_path}")
    return pop_path


# ════════════════════════════════════════════════════════
#  STEP 6c: SOCIOECONOMIC STATISTICS
# ════════════════════════════════════════════════════════
def step_socioeconomic(boundary: gpd.GeoDataFrame,
                       internal_boundaries: Optional[gpd.GeoDataFrame],
                       pop_path: Optional[Path],
                       lc_path: Optional[Path],
                       terrain_paths: Dict[str, Path],
                       osm_paths: Dict[str, Optional[Path]],
                       target_crs: str,
                       output_dir: Path) -> Dict:
    """Compute socioeconomic statistics for the province."""
    log.info("═" * 60)
    log.info("📊 STEP 6c: Socioeconomic Statistics")
    log.info("═" * 60)

    stats = {}
    boundary_4326 = boundary.to_crs("EPSG:4326")
    boundary_proj = boundary.to_crs(epsg=3405)

    # --- Area ---
    area_m2 = boundary_proj.geometry.area.sum()
    area_km2 = area_m2 / 1e6
    stats['area_km2'] = round(area_km2, 2)
    log.info(f"  Area: {area_km2:,.1f} km²")

    # --- Population from WorldPop ---
    if pop_path and pop_path.exists():
        with rasterio.open(pop_path) as src:
            pop_data = src.read(1).astype(float)
            nd = src.nodata
        if nd is not None:
            pop_data[pop_data == nd] = 0
        pop_data[pop_data < 0] = 0
        total_pop = float(np.nansum(pop_data))
        stats['population'] = round(total_pop)
        stats['population_density_per_km2'] = round(total_pop / area_km2, 1) if area_km2 > 0 else 0
        log.info(f"  Population: {total_pop:,.0f}")
        log.info(f"  Density: {stats['population_density_per_km2']:,.1f} /km²")
    else:
        log.warning("  ⚠ No population data available")

    # --- Land cover breakdown ---
    if lc_path and lc_path.exists():
        with rasterio.open(lc_path) as src:
            lc_data = src.read(1)
            pixel_area_m2 = abs(src.transform[0] * src.transform[4])

        lc_stats = {}
        for cls_id, (full_name, short_name, _color) in LANDCOVER_CLASSES.items():
            count = int(np.sum(lc_data == cls_id))
            area = count * pixel_area_m2 / 1e6
            pct = 100 * area / area_km2 if area_km2 > 0 else 0
            if count > 0:
                lc_stats[short_name] = {
                    'area_km2': round(area, 2),
                    'percent': round(pct, 2),
                }
        stats['landcover'] = lc_stats

        urban_area = lc_stats.get('Urban', {}).get('area_km2', 0)
        forest_area = lc_stats.get('Forest', {}).get('area_km2', 0)
        agri_area = lc_stats.get('Agriculture', {}).get('area_km2', 0)
        stats['urban_area_km2'] = urban_area
        stats['urban_percent'] = round(100 * urban_area / area_km2, 2) if area_km2 > 0 else 0
        stats['forest_area_km2'] = forest_area
        stats['forest_percent'] = round(100 * forest_area / area_km2, 2) if area_km2 > 0 else 0
        stats['agriculture_area_km2'] = agri_area
        stats['agriculture_percent'] = round(100 * agri_area / area_km2, 2) if area_km2 > 0 else 0

        log.info(f"  Urban area: {urban_area:,.1f} km² ({stats['urban_percent']:.1f}%)")
        log.info(f"  Forest area: {forest_area:,.1f} km² ({stats['forest_percent']:.1f}%)")
        log.info(f"  Agriculture: {agri_area:,.1f} km² ({stats['agriculture_percent']:.1f}%)")

    # --- Terrain summary ---
    for layer in ['dem', 'slope']:
        p = terrain_paths.get(layer)
        if p and p.exists():
            with rasterio.open(p) as src:
                data = src.read(1).astype(float)
                nd = src.nodata
            if nd is not None:
                data[data == nd] = np.nan
            stats[f'{layer}_mean'] = round(float(np.nanmean(data)), 2)
            stats[f'{layer}_min'] = round(float(np.nanmin(data)), 2)
            stats[f'{layer}_max'] = round(float(np.nanmax(data)), 2)
            stats[f'{layer}_std'] = round(float(np.nanstd(data)), 2)
            log.info(f"  {layer.upper()}: mean={stats[f'{layer}_mean']:.1f}, "
                     f"range=[{stats[f'{layer}_min']:.1f}, {stats[f'{layer}_max']:.1f}]")

    # --- OSM infrastructure density ---
    for layer_name in ['roads', 'rivers']:
        src_path = osm_paths.get(layer_name)
        if src_path and Path(src_path).exists():
            gdf = gpd.read_file(src_path).to_crs(target_crs)
            line_types = ['LineString', 'MultiLineString']
            lines = gdf[gdf.geometry.type.isin(line_types)]
            if not lines.empty:
                total_length_km = lines.geometry.length.sum() / 1000
                density = total_length_km / area_km2 if area_km2 > 0 else 0
                stats[f'{layer_name}_total_km'] = round(total_length_km, 1)
                stats[f'{layer_name}_density_km_per_km2'] = round(density, 3)
                log.info(f"  {layer_name.title()}: {total_length_km:,.0f} km "
                         f"(density: {density:.2f} km/km²)")

    # --- Per-constituent-province stats (for merged provinces) ---
    if internal_boundaries is not None and pop_path and pop_path.exists():
        constituent_stats = []
        for _, row in internal_boundaries.iterrows():
            sub_name = row['NAME_1']
            sub_gdf = gpd.GeoDataFrame([row], crs=internal_boundaries.crs)
            sub_proj = sub_gdf.to_crs(epsg=3405)
            sub_area = sub_proj.geometry.area.sum() / 1e6

            sub_pop = 0
            try:
                from shapely.geometry import mapping as shp_mapping
                geom_4326 = sub_gdf.to_crs("EPSG:4326").geometry.values[0]
                with rasterio.open(pop_path) as src:
                    clipped, _ = rasterio_mask(src, [shp_mapping(geom_4326)],
                                               crop=True, nodata=0)
                    sub_pop = float(np.sum(np.maximum(clipped, 0)))
            except Exception:
                pass

            constituent_stats.append({
                'name': sub_name,
                'area_km2': round(sub_area, 2),
                'population': round(sub_pop),
                'density_per_km2': round(sub_pop / sub_area, 1) if sub_area > 0 else 0,
            })
            log.info(f"    {sub_name}: {sub_area:,.0f} km², "
                     f"pop ~{sub_pop:,.0f}, density ~{sub_pop/sub_area:.0f}/km²")

        stats['constituents'] = constituent_stats

    # --- Save report ---
    report_path = output_dir / "socioeconomic_report.json"
    with open(report_path, 'w', encoding='utf-8') as f:
        json.dump(stats, f, ensure_ascii=False, indent=2)
    log.info(f"  ✅ Report saved: {report_path}")

    # --- Readable text report ---
    txt_path = output_dir / "socioeconomic_report.txt"
    with open(txt_path, 'w', encoding='utf-8') as f:
        name = boundary.iloc[0].get('NAME_1', 'Province')
        f.write(f"{'═' * 60}\n")
        f.write(f"  SOCIOECONOMIC PROFILE: {name}\n")
        f.write(f"{'═' * 60}\n\n")
        f.write(f"  Area:               {stats.get('area_km2', 'N/A'):>12,} km²\n")
        if 'population' in stats:
            f.write(f"  Population (2020):  {stats['population']:>12,}\n")
            f.write(f"  Pop. density:       {stats['population_density_per_km2']:>12,.1f} /km²\n")
        f.write(f"\n  LAND USE:\n")
        f.write(f"  {'─' * 40}\n")
        if 'urban_area_km2' in stats:
            f.write(f"  Urban:              {stats['urban_area_km2']:>10,.1f} km²  "
                    f"({stats['urban_percent']:.1f}%)\n")
        if 'forest_area_km2' in stats:
            f.write(f"  Forest:             {stats['forest_area_km2']:>10,.1f} km²  "
                    f"({stats['forest_percent']:.1f}%)\n")
        if 'agriculture_area_km2' in stats:
            f.write(f"  Agriculture:        {stats['agriculture_area_km2']:>10,.1f} km²  "
                    f"({stats['agriculture_percent']:.1f}%)\n")
        if 'dem_mean' in stats:
            f.write(f"\n  TERRAIN:\n")
            f.write(f"  {'─' * 40}\n")
            f.write(f"  Elevation (mean):   {stats['dem_mean']:>10,.1f} m\n")
            f.write(f"  Elevation (range):  {stats['dem_min']:>10,.1f} – "
                    f"{stats['dem_max']:,.1f} m\n")
        if 'slope_mean' in stats:
            f.write(f"  Slope (mean):       {stats['slope_mean']:>10,.1f}°\n")
        if 'roads_total_km' in stats:
            f.write(f"\n  INFRASTRUCTURE:\n")
            f.write(f"  {'─' * 40}\n")
            f.write(f"  Road network:       {stats['roads_total_km']:>10,.0f} km\n")
            f.write(f"  Road density:       {stats['roads_density_km_per_km2']:>10,.2f} km/km²\n")
        if 'rivers_total_km' in stats:
            f.write(f"  River network:      {stats['rivers_total_km']:>10,.0f} km\n")

        if 'constituents' in stats:
            f.write(f"\n  CONSTITUENT PROVINCES (pre-merger):\n")
            f.write(f"  {'─' * 54}\n")
            f.write(f"  {'Name':<20} {'Area (km²)':>12} {'Pop.':>12} {'Density':>10}\n")
            f.write(f"  {'─' * 54}\n")
            for c in stats['constituents']:
                f.write(f"  {c['name']:<20} {c['area_km2']:>12,.0f} "
                        f"{c['population']:>12,} {c['density_per_km2']:>10,.0f}\n")
        f.write(f"\n{'═' * 60}\n")
    log.info(f"  ✅ Text report: {txt_path}")

    return stats


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
#  STEP 9: PNG MAP GENERATION (ArcGIS-Style)
# ════════════════════════════════════════════════════════

# --- ArcGIS-style theme constants ---
_ARCGIS_BG = '#F5F5F0'          # Canvas beige
_ARCGIS_FRAME = '#2B2B2B'       # Dark frame
_ARCGIS_GRID = '#C0C0C0'        # Subtle grid
_ARCGIS_LABEL = '#333333'       # Axis labels
_ARCGIS_TITLE_BG = '#2B2B2B'    # Title banner
_ARCGIS_TITLE_FG = '#FFFFFF'    # Title text
_ARCGIS_DPI = 200


def _arcgis_theme():
    """Apply ArcGIS-like matplotlib rcParams."""
    plt.rcParams.update({
        'font.family': 'sans-serif',
        'font.sans-serif': ['Arial', 'Helvetica', 'DejaVu Sans'],
        'font.size': 9,
        'axes.facecolor': _ARCGIS_BG,
        'figure.facecolor': _ARCGIS_BG,
        'axes.edgecolor': _ARCGIS_FRAME,
        'axes.linewidth': 1.2,
        'axes.labelcolor': _ARCGIS_LABEL,
        'xtick.color': _ARCGIS_LABEL,
        'ytick.color': _ARCGIS_LABEL,
        'axes.grid': False,
    })


def _add_north_arrow(ax, x=0.95, y=0.95, size=0.06):
    """Draw a north arrow on the axes (in axes-fraction coordinates)."""
    ax.annotate('N', xy=(x, y), xycoords='axes fraction',
                ha='center', va='center', fontsize=11, fontweight='bold',
                color=_ARCGIS_FRAME,
                bbox=dict(boxstyle='round,pad=0.15', fc='white', ec=_ARCGIS_FRAME,
                          lw=0.8, alpha=0.9))
    ax.annotate('', xy=(x, y + size), xycoords='axes fraction',
                xytext=(x, y + size * 0.35), textcoords='axes fraction',
                arrowprops=dict(arrowstyle='->', color=_ARCGIS_FRAME, lw=1.8))


def _add_scale_bar(ax, transform, length_m=None, y_frac=0.04, x_frac=0.05):
    """Draw a scale bar in map coordinates (bottom-left)."""
    xlim = ax.get_xlim()
    ylim = ax.get_ylim()
    map_width = xlim[1] - xlim[0]

    if length_m is None:
        # Auto-pick a nice round number ~20% of map width
        raw = map_width * 0.2
        mag = 10 ** int(math.floor(math.log10(raw)))
        length_m = round(raw / mag) * mag
        if length_m == 0:
            length_m = mag

    x0 = xlim[0] + map_width * x_frac
    y0 = ylim[0] + (ylim[1] - ylim[0]) * y_frac
    bar_h = (ylim[1] - ylim[0]) * 0.006

    # Draw bar segments (alternating black/white like ArcGIS)
    n_seg = 4
    seg_len = length_m / n_seg
    for i in range(n_seg):
        color = _ARCGIS_FRAME if i % 2 == 0 else 'white'
        rect = plt.Rectangle((x0 + i * seg_len, y0), seg_len, bar_h,
                              facecolor=color, edgecolor=_ARCGIS_FRAME, linewidth=0.6,
                              zorder=50, clip_on=False)
        ax.add_patch(rect)

    # Labels
    if length_m >= 1000:
        label = f'{length_m / 1000:.0f} km'
    else:
        label = f'{length_m:.0f} m'

    ax.text(x0 + length_m / 2, y0 + bar_h * 2.5, label,
            ha='center', va='bottom', fontsize=7, fontweight='bold',
            color=_ARCGIS_FRAME, zorder=51,
            bbox=dict(boxstyle='round,pad=0.1', fc='white', ec='none', alpha=0.7))
    ax.text(x0, y0 - bar_h * 1.5, '0', ha='center', va='top',
            fontsize=5.5, color=_ARCGIS_LABEL, zorder=51)
    ax.text(x0 + length_m, y0 - bar_h * 1.5,
            f'{length_m / 1000:.0f} km' if length_m >= 1000 else f'{length_m:.0f} m',
            ha='center', va='top', fontsize=5.5, color=_ARCGIS_LABEL, zorder=51)


def _compute_hillshade(dem: np.ndarray, cell_size: float,
                       azimuth: float = 315, altitude: float = 45):
    """Compute ArcGIS-style analytical hillshade (0-255)."""
    az_rad = math.radians(360 - azimuth + 90)
    alt_rad = math.radians(altitude)
    dy, dx = np.gradient(dem, cell_size)
    slope = np.arctan(np.sqrt(dx**2 + dy**2))
    aspect = np.arctan2(-dy, dx)
    hs = (np.sin(alt_rad) * np.cos(slope) +
          np.cos(alt_rad) * np.sin(slope) * np.cos(az_rad - aspect))
    hs = np.clip(hs * 255, 0, 255).astype(np.uint8)
    return hs


def _make_fig(boundary_utm: gpd.GeoDataFrame, title: str, figsize=(14, 11),
              internal_boundaries_utm: Optional[gpd.GeoDataFrame] = None):
    """Create an ArcGIS-style figure with professional cartographic elements."""
    _arcgis_theme()
    fig, ax = plt.subplots(1, 1, figsize=figsize)

    # Internal boundaries (old province borders, dashed)
    if internal_boundaries_utm is not None and len(internal_boundaries_utm) > 1:
        internal_boundaries_utm.boundary.plot(
            ax=ax, color='#888888', linewidth=0.8, linestyle='--',
            zorder=8, alpha=0.6)
        for _, row in internal_boundaries_utm.iterrows():
            centroid = row.geometry.centroid
            ax.text(centroid.x, centroid.y, row['NAME_1'],
                    fontsize=6, ha='center', va='center', color='#666666',
                    fontweight='bold', alpha=0.7, zorder=8,
                    bbox=dict(boxstyle='round,pad=0.2', fc='white',
                              ec='none', alpha=0.5))

    # Province boundary with shadow effect
    boundary_utm.boundary.plot(ax=ax, color='#555555', linewidth=3.0, zorder=9, alpha=0.3)
    boundary_utm.boundary.plot(ax=ax, color=_ARCGIS_FRAME, linewidth=1.8, zorder=10)

    # Title banner (ArcGIS-style dark bar at top)
    ax.set_title(title, fontsize=15, fontweight='bold', color=_ARCGIS_TITLE_FG,
                 pad=14,
                 bbox=dict(boxstyle='round,pad=0.4', facecolor=_ARCGIS_TITLE_BG,
                           edgecolor='none', alpha=0.92))

    # Axis formatting
    ax.set_xlabel('Easting (m)', fontsize=9, labelpad=8, color=_ARCGIS_LABEL)
    ax.set_ylabel('Northing (m)', fontsize=9, labelpad=8, color=_ARCGIS_LABEL)
    ax.ticklabel_format(style='plain')
    ax.tick_params(labelsize=7, length=4, width=0.8, colors=_ARCGIS_LABEL)

    # Subtle coordinate grid
    ax.grid(True, linestyle=':', linewidth=0.4, color=_ARCGIS_GRID,
            alpha=0.6, zorder=0)

    # Frame styling
    for spine in ax.spines.values():
        spine.set_edgecolor(_ARCGIS_FRAME)
        spine.set_linewidth(1.2)

    return fig, ax


def _make_arcgis_colorbar(fig, ax, im, label='', orientation='vertical'):
    """Create an ArcGIS-style colorbar with professional formatting."""
    cbar = fig.colorbar(im, ax=ax, fraction=0.028, pad=0.02,
                        orientation=orientation, shrink=0.85)
    cbar.ax.tick_params(labelsize=7, length=3, width=0.6, colors=_ARCGIS_LABEL)
    if label:
        cbar.set_label(label, fontsize=8, color=_ARCGIS_LABEL, labelpad=8)
    cbar.outline.set_edgecolor(_ARCGIS_FRAME)
    cbar.outline.set_linewidth(0.8)
    return cbar


def _finalize_map(fig, ax, out_png, transform=None, attribution=None):
    """Add north arrow, scale bar, attribution, and save."""
    _add_north_arrow(ax)
    if transform is not None:
        _add_scale_bar(ax, transform)

    if attribution:
        ax.text(0.99, 0.01, attribution, transform=ax.transAxes,
                ha='right', va='bottom', fontsize=5, color='#888888',
                style='italic', zorder=51,
                bbox=dict(boxstyle='round,pad=0.15', fc='white', ec='none', alpha=0.5))

    fig.tight_layout(pad=1.5)
    out_png.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_png, dpi=_ARCGIS_DPI, bbox_inches='tight',
                facecolor=_ARCGIS_BG, edgecolor='none')
    plt.close(fig)
    log.info(f"  🗺️  {out_png.name}")


def _save_raster_map(raster_path: Path, boundary: gpd.GeoDataFrame,
                     target_crs: str, title: str, out_png: Path,
                     cmap='terrain', vmin=None, vmax=None, label='',
                     hillshade: bool = False, dem_path: Optional[Path] = None,
                     internal_boundaries: Optional[gpd.GeoDataFrame] = None):
    """Generate ArcGIS-style PNG for a raster layer."""
    boundary_utm = boundary.to_crs(target_crs)
    ib_utm = internal_boundaries.to_crs(target_crs) if internal_boundaries is not None else None
    fig, ax = _make_fig(boundary_utm, title, internal_boundaries_utm=ib_utm)

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

    # Hillshade basemap (ArcGIS-style shaded relief)
    if hillshade:
        hs_data = None
        if dem_path and dem_path.exists():
            with rasterio.open(dem_path) as dem_src:
                hs_data = dem_src.read(1).astype(float)
                if dem_src.nodata is not None:
                    hs_data[dem_src.read(1) == dem_src.nodata] = np.nan
        else:
            hs_data = data_plot.copy()

        if hs_data is not None:
            cs = abs(tf[0])
            hs_valid = hs_data.copy()
            hs_valid[np.isnan(hs_valid)] = 0
            hs = _compute_hillshade(hs_valid, cs)
            ax.imshow(hs, extent=extent, cmap='gray', vmin=0, vmax=255,
                      origin='upper', alpha=0.35, zorder=0)

    im = ax.imshow(data_plot, extent=extent, cmap=cmap, vmin=vmin, vmax=vmax,
                   origin='upper', alpha=0.85 if hillshade else 0.92,
                   zorder=1, interpolation='bilinear')

    _make_arcgis_colorbar(fig, ax, im, label=label)

    ax.set_xlim(extent[0], extent[1])
    ax.set_ylim(extent[2], extent[3])
    _finalize_map(fig, ax, out_png, transform=tf,
                  attribution='Data: Copernicus DEM / ESA WorldCover')


def _save_landcover_map(raster_path: Path, boundary: gpd.GeoDataFrame,
                        target_crs: str, out_png: Path,
                        internal_boundaries: Optional[gpd.GeoDataFrame] = None):
    """Generate ArcGIS-style PNG for landcover (categorical)."""
    boundary_utm = boundary.to_crs(target_crs)
    ib_utm = internal_boundaries.to_crs(target_crs) if internal_boundaries is not None else None
    fig, ax = _make_fig(boundary_utm, "Land Cover (ESA WorldCover 10m)",
                        internal_boundaries_utm=ib_utm)

    with rasterio.open(raster_path) as src:
        data = src.read(1)
        tf = src.transform

    extent = [tf[2], tf[2] + tf[0] * data.shape[1],
              tf[5] + tf[4] * data.shape[0], tf[5]]

    # Build colormap
    classes = sorted(LANDCOVER_CLASSES.keys())
    colors_list = [LANDCOVER_CLASSES[c][2] for c in classes]
    lc_cmap = mcolors.ListedColormap(colors_list)
    bounds_cm = classes + [max(classes) + 10]
    norm = mcolors.BoundaryNorm(bounds_cm, lc_cmap.N)

    data_plot = data.astype(float)
    data_plot[data == 0] = np.nan

    ax.imshow(data_plot, extent=extent, cmap=lc_cmap, norm=norm,
              origin='upper', alpha=0.92, zorder=1, interpolation='nearest')

    # ArcGIS-style legend: framed box with larger patches
    present = np.unique(data)
    patches = [Patch(facecolor=LANDCOVER_CLASSES[c][2],
                     edgecolor='#555555', linewidth=0.5,
                     label=f'  {LANDCOVER_CLASSES[c][1]}')
               for c in classes if c in present]
    legend = ax.legend(handles=patches, loc='lower right', fontsize=7.5,
                       framealpha=0.95, edgecolor=_ARCGIS_FRAME,
                       fancybox=False, shadow=False,
                       title='Land Cover Class', title_fontsize=8,
                       labelspacing=0.6, handlelength=1.8, handleheight=1.2,
                       borderpad=0.8)
    legend.get_frame().set_linewidth(0.8)
    legend.get_title().set_fontweight('bold')

    ax.set_xlim(extent[0], extent[1])
    ax.set_ylim(extent[2], extent[3])
    _finalize_map(fig, ax, out_png, transform=tf,
                  attribution='Data: ESA WorldCover v200 2021')


def _save_vector_map(gpkg_path: Path, boundary: gpd.GeoDataFrame,
                     target_crs: str, title: str, out_png: Path,
                     color='red', linewidth=0.5,
                     dem_path: Optional[Path] = None,
                     internal_boundaries: Optional[gpd.GeoDataFrame] = None):
    """Generate ArcGIS-style PNG for a vector layer with optional hillshade basemap."""
    boundary_utm = boundary.to_crs(target_crs)
    ib_utm = internal_boundaries.to_crs(target_crs) if internal_boundaries is not None else None
    fig, ax = _make_fig(boundary_utm, title, internal_boundaries_utm=ib_utm)

    # Optional hillshade basemap for context
    if dem_path and dem_path.exists():
        with rasterio.open(dem_path) as src:
            dem_data = src.read(1).astype(float)
            tf = src.transform
            if src.nodata is not None:
                dem_data[src.read(1) == src.nodata] = np.nan
        dem_valid = dem_data.copy()
        dem_valid[np.isnan(dem_valid)] = 0
        cs = abs(tf[0])
        hs = _compute_hillshade(dem_valid, cs)
        extent = [tf[2], tf[2] + tf[0] * dem_data.shape[1],
                  tf[5] + tf[4] * dem_data.shape[0], tf[5]]
        ax.imshow(hs, extent=extent, cmap='gray', vmin=0, vmax=255,
                  origin='upper', alpha=0.25, zorder=0)

    gdf = gpd.read_file(gpkg_path).to_crs(target_crs)

    # Clip to boundary
    from shapely.ops import unary_union
    clip_geom = unary_union(boundary_utm.geometry)
    gdf = gpd.clip(gdf, clip_geom)

    if not gdf.empty:
        geom_types = gdf.geometry.type.unique()
        if any(t in ['Point', 'MultiPoint'] for t in geom_types):
            gdf.plot(ax=ax, color=color, markersize=2, alpha=0.8, zorder=5,
                     edgecolor='white', linewidth=0.2)
        else:
            # Outer glow effect
            gdf.plot(ax=ax, color='white', linewidth=linewidth + 1.2,
                     alpha=0.4, zorder=4)
            gdf.plot(ax=ax, color=color, linewidth=linewidth,
                     alpha=0.9, zorder=5)

    b = boundary_utm.total_bounds
    ax.set_xlim(b[0], b[2])
    ax.set_ylim(b[1], b[3])
    _finalize_map(fig, ax, out_png, attribution='Data: © OpenStreetMap contributors')


def _save_contour_map(shp_path: Path, boundary: gpd.GeoDataFrame,
                      target_crs: str, out_png: Path,
                      dem_path: Optional[Path] = None,
                      interval: float = 10.0,
                      internal_boundaries: Optional[gpd.GeoDataFrame] = None):
    """Generate ArcGIS-style contour map with major/minor line differentiation."""
    boundary_utm = boundary.to_crs(target_crs)
    ib_utm = internal_boundaries.to_crs(target_crs) if internal_boundaries is not None else None
    fig, ax = _make_fig(boundary_utm, f"Contour Lines ({interval:.0f}m interval)",
                        internal_boundaries_utm=ib_utm)

    # Hillshade basemap
    if dem_path and dem_path.exists():
        with rasterio.open(dem_path) as src:
            dem_data = src.read(1).astype(float)
            tf = src.transform
            if src.nodata is not None:
                dem_data[src.read(1) == src.nodata] = np.nan
        dem_valid = dem_data.copy()
        dem_valid[np.isnan(dem_valid)] = 0
        cs = abs(tf[0])
        hs = _compute_hillshade(dem_valid, cs)
        extent = [tf[2], tf[2] + tf[0] * dem_data.shape[1],
                  tf[5] + tf[4] * dem_data.shape[0], tf[5]]
        ax.imshow(hs, extent=extent, cmap='gray', vmin=0, vmax=255,
                  origin='upper', alpha=0.3, zorder=0)

    gdf = gpd.read_file(shp_path).to_crs(target_crs)
    if not gdf.empty:
        # Determine major interval (every 5th contour)
        major_interval = interval * 5
        gdf['is_major'] = (gdf['elevation'] % major_interval) < 0.01

        # Minor contours
        minor = gdf[~gdf['is_major']]
        if not minor.empty:
            minor.plot(ax=ax, color='#8B6914', linewidth=0.2, alpha=0.55, zorder=4)

        # Major contours (bold, darker)
        major = gdf[gdf['is_major']]
        if not major.empty:
            major.plot(ax=ax, color='#654321', linewidth=0.7, alpha=0.85, zorder=5)

        # Legend
        legend_handles = [
            Line2D([0], [0], color='#654321', linewidth=1.5,
                   label=f'Major ({major_interval:.0f}m)'),
            Line2D([0], [0], color='#8B6914', linewidth=0.5, alpha=0.6,
                   label=f'Minor ({interval:.0f}m)'),
        ]
        leg = ax.legend(handles=legend_handles, loc='lower right', fontsize=7.5,
                        framealpha=0.95, edgecolor=_ARCGIS_FRAME,
                        fancybox=False, title='Contour Lines', title_fontsize=8,
                        borderpad=0.8)
        leg.get_frame().set_linewidth(0.8)
        leg.get_title().set_fontweight('bold')

    b = boundary_utm.total_bounds
    ax.set_xlim(b[0], b[2])
    ax.set_ylim(b[1], b[3])
    _finalize_map(fig, ax, out_png, attribution='Data: Copernicus DEM 30m')


def _save_population_map(pop_path: Path, boundary: gpd.GeoDataFrame,
                         target_crs: str, out_png: Path,
                         internal_boundaries: Optional[gpd.GeoDataFrame] = None):
    """Generate population density map."""
    boundary_utm = boundary.to_crs(target_crs)
    ib_utm = internal_boundaries.to_crs(target_crs) if internal_boundaries is not None else None
    fig, ax = _make_fig(boundary_utm, "Population Density (WorldPop 2020)",
                        internal_boundaries_utm=ib_utm)

    with rasterio.open(pop_path) as src:
        dst_crs = rasterio.crs.CRS.from_user_input(target_crs)
        if src.crs != dst_crs:
            tf, w, h = calculate_default_transform(
                src.crs, dst_crs, src.width, src.height, *src.bounds)
            data = np.empty((h, w), dtype=np.float32)
            reproject(
                source=src.read(1), destination=data,
                src_transform=src.transform, src_crs=src.crs,
                dst_transform=tf, dst_crs=dst_crs,
                resampling=Resampling.bilinear,
                src_nodata=src.nodata, dst_nodata=np.nan,
            )
            data = data.astype(float)
        else:
            data = src.read(1).astype(float)
            tf = src.transform
            nd = src.nodata
            if nd is not None:
                data[data == nd] = np.nan

    data[data < 0] = np.nan

    extent = [tf[2], tf[2] + tf[0] * data.shape[1],
              tf[5] + tf[4] * data.shape[0], tf[5]]

    pop_cmap = plt.cm.YlOrRd.copy()
    pop_cmap.set_bad(color=_ARCGIS_BG)

    vmax = np.nanpercentile(data[data > 0], 95) if np.any(data > 0) else 1
    im = ax.imshow(data, extent=extent, cmap=pop_cmap, vmin=0, vmax=vmax,
                   origin='upper', alpha=0.9, zorder=1, interpolation='bilinear')

    _make_arcgis_colorbar(fig, ax, im, label='Population per pixel')

    ax.set_xlim(extent[0], extent[1])
    ax.set_ylim(extent[2], extent[3])
    _finalize_map(fig, ax, out_png, transform=tf,
                  attribution='Data: WorldPop 2020 UN-adjusted')


def _save_socioeconomic_summary_map(boundary: gpd.GeoDataFrame,
                                    internal_boundaries: Optional[gpd.GeoDataFrame],
                                    stats: Dict, target_crs: str, out_png: Path):
    """Generate a summary infographic map showing key socioeconomic indicators."""
    boundary_utm = boundary.to_crs(target_crs)
    ib_utm = internal_boundaries.to_crs(target_crs) if internal_boundaries is not None else None
    name = boundary.iloc[0].get('NAME_1', 'Province')
    fig, ax = _make_fig(boundary_utm, f"Socioeconomic Profile: {name}",
                        internal_boundaries_utm=ib_utm)

    boundary_utm.plot(ax=ax, facecolor='#E8F5E9', edgecolor=_ARCGIS_FRAME,
                      linewidth=1.5, zorder=1)

    if ib_utm is not None and len(ib_utm) > 1:
        import matplotlib.cm as cm
        colors = cm.Set3(np.linspace(0, 1, len(ib_utm)))
        for idx, (_, row) in enumerate(ib_utm.iterrows()):
            gpd.GeoDataFrame([row], crs=ib_utm.crs).plot(
                ax=ax, facecolor=colors[idx], edgecolor='#666666',
                linewidth=0.8, alpha=0.4, zorder=2)

    b = boundary_utm.total_bounds
    ax.set_xlim(b[0], b[2])
    ax.set_ylim(b[1], b[3])

    info_lines = []
    if 'area_km2' in stats:
        info_lines.append(f"Area: {stats['area_km2']:,.0f} km²")
    if 'population' in stats:
        info_lines.append(f"Population: {stats['population']:,.0f}")
        info_lines.append(f"Density: {stats['population_density_per_km2']:,.0f} /km²")
    if 'urban_percent' in stats:
        info_lines.append(f"Urban: {stats['urban_percent']:.1f}%")
    if 'forest_percent' in stats:
        info_lines.append(f"Forest: {stats['forest_percent']:.1f}%")
    if 'agriculture_percent' in stats:
        info_lines.append(f"Agriculture: {stats['agriculture_percent']:.1f}%")
    if 'dem_mean' in stats:
        info_lines.append(f"Elevation: {stats['dem_mean']:.0f}m (mean)")
    if 'roads_total_km' in stats:
        info_lines.append(f"Roads: {stats['roads_total_km']:,.0f} km")

    info_text = '\n'.join(info_lines)
    ax.text(0.03, 0.97, info_text, transform=ax.transAxes,
            fontsize=8, fontfamily='monospace', verticalalignment='top',
            bbox=dict(boxstyle='round,pad=0.6', facecolor='white',
                      edgecolor=_ARCGIS_FRAME, alpha=0.92, linewidth=0.8),
            zorder=20)

    _finalize_map(fig, ax, out_png,
                  attribution='NQ 202/2025/QH15 | WorldPop | ESA | OSM')


def step_maps(output_dir: Path, boundary: gpd.GeoDataFrame, target_crs: str,
              raster_paths: dict, osm_paths: dict,
              dist_paths: dict, contour_path: Optional[Path],
              pop_path: Optional[Path] = None,
              internal_boundaries: Optional[gpd.GeoDataFrame] = None,
              socio_stats: Optional[Dict] = None):
    """Generate ArcGIS-style PNG maps for all layers."""
    log.info("═" * 60)
    log.info("🗺️  STEP 9: PNG Map Generation (ArcGIS-Style)")
    log.info("═" * 60)

    maps_dir = output_dir / "maps"
    ib = internal_boundaries

    dem_path = raster_paths.get('dem')

    raster_configs = {
        'dem':               ('DEM — Elevation',          'gist_earth', 'Elevation (m)',   True),
        'slope':             ('Slope Angle',              'YlOrRd',     'Degrees (°)',     True),
        'aspect':            ('Aspect (Compass Direction)', 'twilight',  'Degrees (°)',     False),
        'curvature':         ('Profile Curvature',        'RdBu_r',     'Curvature',       True),
        'flow_accumulation': ('Flow Accumulation (D8)',    'Blues',      'Upstream Cells',  True),
        'twi':               ('Topographic Wetness Index', 'YlGnBu',    'TWI',             True),
    }
    for name, (title, cmap, label, use_hs) in raster_configs.items():
        if name in raster_paths and raster_paths[name].exists():
            vmin, vmax = None, None
            if name == 'flow_accumulation':
                vmin, vmax = 1, None
            _save_raster_map(
                raster_paths[name], boundary, target_crs,
                title, maps_dir / f"{name}.png", cmap=cmap, label=label,
                vmin=vmin, vmax=vmax,
                hillshade=use_hs,
                dem_path=dem_path if use_hs and name != 'dem' else None,
                internal_boundaries=ib,
            )

    # Landcover map
    if 'landcover' in raster_paths and raster_paths['landcover'].exists():
        _save_landcover_map(raster_paths['landcover'], boundary, target_crs,
                            maps_dir / "landcover.png", internal_boundaries=ib)

    # Distance rasters
    for name, path in dist_paths.items():
        if path and path.exists():
            title = name.replace('_', ' ').title()
            _save_raster_map(path, boundary, target_crs, title,
                             maps_dir / f"{name}.png",
                             cmap='YlOrRd_r', label='Distance (m)',
                             hillshade=True, dem_path=dem_path,
                             internal_boundaries=ib)

    # Vector maps (with hillshade basemap)
    vec_configs = {
        'roads': ('Road Network (OSM)', '#D62828', 0.4),
        'rivers': ('Hydrographic Network (OSM)', '#1565C0', 0.9),
        'infrastructure': ('Infrastructure (OSM)', '#FF8F00', 0.5),
    }
    for name, (title, color, lw) in vec_configs.items():
        if name in osm_paths and osm_paths[name] and osm_paths[name].exists():
            _save_vector_map(osm_paths[name], boundary, target_crs,
                             title, maps_dir / f"{name}.png",
                             color=color, linewidth=lw, dem_path=dem_path,
                             internal_boundaries=ib)

    # Contour map (with hillshade basemap)
    if contour_path and contour_path.exists():
        _save_contour_map(contour_path, boundary, target_crs,
                          maps_dir / "contour.png", dem_path=dem_path,
                          internal_boundaries=ib)

    # Population density map
    if pop_path and pop_path.exists():
        _save_population_map(pop_path, boundary, target_crs,
                             maps_dir / "population.png",
                             internal_boundaries=ib)

    # Socioeconomic summary map
    if socio_stats:
        _save_socioeconomic_summary_map(
            boundary, ib, socio_stats, target_crs,
            maps_dir / "socioeconomic_summary.png")


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
@click.option('--province', default=None, help='Province name (Vietnamese, 2025 or legacy)')
@click.option('--resolution', default=5.0, type=float, help='Target resolution (m)')
@click.option('--contour-interval', default=10.0, type=float, help='Contour interval (m)')
@click.option('--output-dir', default=None, type=str, help='Output directory')
@click.option('--legacy-boundaries', is_flag=True, default=False,
              help='Use old 63-province boundaries (skip 2025 merger)')
@click.option('--list-provinces', is_flag=True, default=False,
              help='List all available province names and exit')
def main(province, resolution, contour_interval, output_dir,
         legacy_boundaries, list_provinces):
    """
    Provincial GIS Pipeline (Vietnam).

    Downloads and processes geospatial, demographic, and socioeconomic
    layers for a province. Supports 2025 merger (NQ 202/2025/QH15):
    63 provinces → 34 units.

    \b
    Examples:
        python pipeline.py --province "Quảng Ngãi"           # 2025: Kon Tum + Quảng Ngãi
        python pipeline.py --province "Hồ Chí Minh"          # 2025: BD + BRVT + HCM
        python pipeline.py --province "Lào Cai" --legacy-boundaries  # Old single province
        python pipeline.py --list-provinces
    """
    if list_provinces:
        log.info("═" * 60)
        log.info("  AVAILABLE PROVINCES")
        log.info("═" * 60)
        log.info("\n  ── 2025 Provinces (34 units, NQ 202/2025/QH15) ──")
        for new_name, old_names in sorted(PROVINCE_MERGER_2025.items()):
            if len(old_names) > 1:
                log.info(f"    {new_name:<16} ← {', '.join(old_names)}")
            else:
                log.info(f"    {new_name:<16}   (unchanged)")
        log.info(f"\n  Use --legacy-boundaries to access old 63-province GADM names.")
        return

    if not province:
        raise click.UsageError("--province is required (or use --list-provinces)")

    log.info("[*] PROVINCIAL GIS PIPELINE (Vietnam)")
    log.info(f"   Province:   {province}")
    log.info(f"   Resolution: {resolution}m")
    log.info(f"   Boundaries: {'Legacy (63 provinces)' if legacy_boundaries else '2025 Merger (34 units)'}")
    log.info("")

    safe_name = province.replace(" ", "_").replace(".", "")
    if output_dir is None:
        output_dir = Path("data") / "province" / safe_name
    else:
        output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    log.info(f"   Output: {output_dir.resolve()}")
    log.info("")

    # ── Step 1: Boundary (with merger support) ──
    boundary, internal_boundaries = step_boundary(
        province, output_dir, legacy=legacy_boundaries)

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

    # ── Step 6a: WorldCover ──
    lc_path = step_landcover(boundary, output_dir)

    # ── Step 6b: Population ──
    pop_path = step_population(boundary, output_dir)

    # ── Target grid ──
    grid = compute_target_grid(boundary, target_crs, resolution)
    log.info(f"\n  Target grid: {grid['width']}x{grid['height']} @ {resolution}m")

    # ── Step 7: Distance rasters ──
    dist_paths = step_distance(osm_paths, grid, boundary, output_dir)

    # ── Step 8: Normalize ──
    all_rasters = step_normalize(terrain_paths, lc_path, grid, boundary, output_dir)

    # ── Step 6c: Socioeconomic statistics ──
    socio_stats = step_socioeconomic(
        boundary, internal_boundaries, pop_path, lc_path,
        terrain_paths, osm_paths, target_crs, output_dir)

    # ── Step 9: Maps (including population + socioeconomic) ──
    step_maps(output_dir, boundary, target_crs, all_rasters, osm_paths,
              dist_paths, contour_path,
              pop_path=pop_path,
              internal_boundaries=internal_boundaries,
              socio_stats=socio_stats)

    # ── Step 10: Stack ──
    step_stack(all_rasters, dist_paths, output_dir, grid)

    # ── Summary ──
    log.info("")
    log.info("═" * 60)
    log.info("🎉 PIPELINE COMPLETE!")
    log.info("═" * 60)
    prov_name = boundary.iloc[0].get('NAME_1', province)
    log.info(f"  Province: {prov_name}")
    if internal_boundaries is not None:
        old_names = internal_boundaries['NAME_1'].tolist()
        log.info(f"  Merged from: {', '.join(old_names)}")
    log.info(f"  Output directory: {output_dir.resolve()}")

    if socio_stats:
        log.info("")
        log.info("  📊 Key Statistics:")
        if 'area_km2' in socio_stats:
            log.info(f"     Area:        {socio_stats['area_km2']:>10,.0f} km²")
        if 'population' in socio_stats:
            log.info(f"     Population:  {socio_stats['population']:>10,.0f}")
            log.info(f"     Density:     {socio_stats['population_density_per_km2']:>10,.0f} /km²")
        if 'urban_percent' in socio_stats:
            log.info(f"     Urban:       {socio_stats['urban_percent']:>10.1f}%")
        if 'forest_percent' in socio_stats:
            log.info(f"     Forest:      {socio_stats['forest_percent']:>10.1f}%")

    log.info("")
    log.info("  📁 Files generated:")
    for f in sorted(output_dir.rglob("*")):
        if f.is_file() and 'raw' not in str(f) and 'native' not in str(f):
            size = f.stat().st_size / (1024 * 1024)
            log.info(f"     {f.relative_to(output_dir)}  ({size:.1f} MB)")


if __name__ == '__main__':
    main()
