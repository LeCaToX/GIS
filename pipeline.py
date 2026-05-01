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
  - Demographics: GSO Vietnam (Tổng cục Thống kê) official census data
  - Socioeconomic report: area, population, urbanization, land use

Data sources prioritise Vietnamese/regional datasets (GSO, OD Mekong)
over global ones (WorldPop) wherever possible.

2025 Merger: Automatically dissolves old province boundaries into the
new 34-unit administrative structure per Nghị quyết 202/2025/QH15.
"""

import os
import sys
import io
import math
import shutil
import zipfile
from email.utils import parsedate_to_datetime

# Ensure Matplotlib can write its cache even on locked-down systems
# (must be set before importing matplotlib).
try:
    _mpl_dir = os.environ.get("MPLCONFIGDIR")
    if not _mpl_dir:
        # Prefer a project-local cache directory; fallback to /tmp.
        _local = Path("cache") / "matplotlib"
        _local.mkdir(parents=True, exist_ok=True)
        os.environ["MPLCONFIGDIR"] = str(_local)
except Exception:
    # As a last resort, let Matplotlib pick a temporary directory.
    pass

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
OD_MEKONG_BOUNDARY_URL = (
    "https://data.opendevelopmentmekong.net/dataset/999c96d8-fae0-4b82-9a2b-e481f6f50e12/"
    "resource/234169fb-ae73-4f23-bbd4-ff20a4fca401/download/diaphantinh.geojson"
)
COP_DEM_BASE = "https://copernicus-dem-30m.s3.amazonaws.com"
ESA_WC_BASE = "https://esa-worldcover.s3.eu-central-1.amazonaws.com/v200/2021/map"
GEOFABRIK_VN_SHP_URL = "https://download.geofabrik.de/asia/vietnam-latest-free.shp.zip"
GEOFABRIK_VN_PBF_URL = "https://download.geofabrik.de/asia/vietnam-latest.osm.pbf"

# Public global thematic layers (no-auth, open access)
# - NDVI: NASA Earth Observations (NEO) MOD_NDVI_16 (approximate values, visualization-grade)
# - Geology (lithology classes): OpenLandMap (USGS EcoTapestry derived)
# - Forest type: MODIS MCD12Q1 IGBP land cover type 1 (via OpenLandMap)
NEO_VIEW_MOD_NDVI_16 = "https://neo.gsfc.nasa.gov/view.php?datasetId=MOD_NDVI_16"
OPENLANDMAP_LITHOLOGY_COG = (
    "https://s3.openlandmap.org/arco/lithology_usgs.ecotapestry_c_250m_s_20140101_20141231_go_epsg.4326_v1.0.tif"
)
OPENLANDMAP_MCD12Q1_T1_2021_COG = (
    "https://s3.openlandmap.org/arco/lc_mcd12q1v061.t1_c_500m_s_20210101_20211231_go_epsg.4326_v20230818.tif"
)

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
#  GSO PROVINCE DATA (Tổng cục Thống kê – 2023 estimates)
#  Source: gso.gov.vn  /  Statistical Yearbook of Vietnam 2023
#  Keys match GADM NAME_1 field for direct lookup
# ════════════════════════════════════════════════════════
GSO_PROVINCE_DATA = {
    # Northeast (Đông Bắc)
    "Hà Giang":     {"population": 910_000,   "area_km2": 7_929.48},
    "Cao Bằng":     {"population": 576_000,   "area_km2": 6_700.26},
    "Lạng Sơn":     {"population": 828_000,   "area_km2": 8_310.09},
    "Tuyên Quang":  {"population": 1_056_000, "area_km2": 5_867.90},
    "Bắc Kạn":     {"population": 338_000,   "area_km2": 4_859.96},
    "Thái Nguyên":  {"population": 1_356_000, "area_km2": 3_526.64},
    "Phú Thọ":     {"population": 1_515_000, "area_km2": 3_534.56},
    "Bắc Giang":   {"population": 1_884_000, "area_km2": 3_895.59},
    "Quảng Ninh":   {"population": 1_387_000, "area_km2": 6_178.21},
    # Northwest (Tây Bắc)
    "Lào Cai":      {"population": 898_000,   "area_km2": 6_364.03},
    "Yên Bái":     {"population": 950_000,   "area_km2": 6_887.46},
    "Điện Biên":   {"population": 705_000,   "area_km2": 9_541.25},
    "Hòa Bình":    {"population": 979_000,   "area_km2": 4_590.57},
    "Lai Châu":     {"population": 527_000,   "area_km2": 9_068.79},
    "Sơn La":       {"population": 1_356_000, "area_km2": 14_123.49},
    # Red River Delta (Đồng bằng sông Hồng)
    "Hà Nội":      {"population": 8_146_000, "area_km2": 3_358.60},
    "Vĩnh Phúc":   {"population": 1_296_000, "area_km2": 1_235.87},
    "Bắc Ninh":    {"population": 1_447_000, "area_km2": 822.71},
    "Hải Dương":   {"population": 2_003_000, "area_km2": 1_668.24},
    "Hải Phòng":   {"population": 2_138_000, "area_km2": 1_561.76},
    "Hưng Yên":    {"population": 1_340_000, "area_km2": 930.22},
    "Thái Bình":   {"population": 1_934_000, "area_km2": 1_586.35},
    "Hà Nam":       {"population": 945_000,   "area_km2": 861.93},
    "Nam Định":    {"population": 1_852_000, "area_km2": 1_668.57},
    "Ninh Bình":    {"population": 1_102_000, "area_km2": 1_386.79},
    # North Central Coast (Bắc Trung Bộ)
    "Thanh Hóa":   {"population": 3_689_000, "area_km2": 11_114.65},
    "Nghệ An":     {"population": 3_399_000, "area_km2": 16_481.41},
    "Hà Tĩnh":    {"population": 1_402_000, "area_km2": 5_990.67},
    "Quảng Bình":  {"population": 992_000,   "area_km2": 8_065.30},
    "Quảng Trị":   {"population": 687_000,   "area_km2": 4_621.72},
    "Thừa Thiên Huế": {"population": 1_257_000, "area_km2": 4_902.44},
    # South Central Coast + Central Highlands
    "Đà Nẵng":    {"population": 1_294_000, "area_km2": 1_284.88},
    "Quảng Nam":    {"population": 1_697_000, "area_km2": 10_574.74},
    "Quảng Ngãi":  {"population": 1_355_000, "area_km2": 5_155.78},
    "Bình Định":   {"population": 1_679_000, "area_km2": 6_066.21},
    "Phú Yên":     {"population": 1_056_000, "area_km2": 5_023.42},
    "Khánh Hòa":   {"population": 1_371_000, "area_km2": 5_137.79},
    "Ninh Thuận":  {"population": 720_000,   "area_km2": 3_355.34},
    "Bình Thuận":  {"population": 1_498_000, "area_km2": 7_943.93},
    "Kon Tum":      {"population": 589_000,   "area_km2": 9_674.18},
    "Gia Lai":      {"population": 1_586_000, "area_km2": 15_510.99},
    "Đắk Lắk":    {"population": 2_016_000, "area_km2": 13_030.50},
    "Đắk Nông":   {"population": 670_000,   "area_km2": 6_509.27},
    "Lâm Đồng":   {"population": 1_390_000, "area_km2": 9_783.34},
    # Southeast (Đông Nam Bộ)
    "Bình Phước":  {"population": 1_313_000, "area_km2": 6_876.76},
    "Tây Ninh":     {"population": 1_207_000, "area_km2": 4_041.25},
    "Bình Dương":  {"population": 2_564_000, "area_km2": 2_694.64},
    "Đồng Nai":    {"population": 3_227_000, "area_km2": 5_863.60},
    "Bà Rịa - Vũng Tàu": {"population": 1_303_000, "area_km2": 1_980.98},
    "Hồ Chí Minh":  {"population": 9_125_000, "area_km2": 2_061.41},
    # Mekong Delta (Đồng bằng sông Cửu Long)
    "Long An":      {"population": 1_730_000, "area_km2": 4_494.94},
    "Tiền Giang":  {"population": 1_898_000, "area_km2": 2_510.61},
    "Bến Tre":     {"population": 1_331_000, "area_km2": 2_394.81},
    "Trà Vinh":     {"population": 1_067_000, "area_km2": 2_358.26},
    "Vĩnh Long":    {"population": 1_100_000, "area_km2": 1_525.73},
    "Đồng Tháp":  {"population": 1_838_000, "area_km2": 3_383.85},
    "An Giang":     {"population": 2_057_000, "area_km2": 3_536.68},
    "Kiên Giang":   {"population": 1_789_000, "area_km2": 6_348.78},
    "Cần Thơ":     {"population": 1_456_000, "area_km2": 1_438.96},
    "Hậu Giang":   {"population": 853_000,   "area_km2": 1_621.70},
    "Sóc Trăng":   {"population": 1_256_000, "area_km2": 3_311.88},
    "Bạc Liêu":   {"population": 1_105_000, "area_km2": 2_669.01},
    "Cà Mau":       {"population": 1_369_000, "area_km2": 5_221.19},
}

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


def download_http_stream(url: str, dest: Path, desc: str = "", retries: int = 3, timeout_s: int = 60) -> bool:
    """Download via streaming GET (for endpoints that do not support HEAD well)."""
    for attempt in range(retries):
        try:
            label = desc or url
            log.info(f"  [v] Downloading {label} ...")
            dest.parent.mkdir(parents=True, exist_ok=True)
            with requests.get(url, stream=True, timeout=(15, timeout_s)) as r:
                r.raise_for_status()
                with open(dest, "wb") as f:
                    for chunk in r.iter_content(chunk_size=1024 * 1024):
                        if chunk:
                            f.write(chunk)
            mb = dest.stat().st_size / (1024 * 1024)
            log.info(f"  [OK] Downloaded {label} ({mb:.1f} MB)")
            return True
        except Exception as e:
            log.warning(f"  Attempt {attempt+1}/{retries} failed: {e}")
            if dest.exists():
                try:
                    dest.unlink()
                except Exception:
                    pass
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
    return False


def _remote_last_modified_epoch(url: str) -> Optional[float]:
    """Return HTTP Last-Modified as epoch seconds, or None."""
    try:
        r = requests.head(url, timeout=(10, 20), allow_redirects=True)
        r.raise_for_status()
        lm = r.headers.get("last-modified")
        if not lm:
            return None
        return parsedate_to_datetime(lm).timestamp()
    except Exception:
        return None


def _download_if_newer(url: str, dest: Path, desc: str) -> bool:
    """Download only when remote appears newer than local file."""
    remote_ts = _remote_last_modified_epoch(url)
    if dest.exists() and remote_ts is not None:
        # 1-minute tolerance for filesystem/network clock differences
        if dest.stat().st_mtime >= (remote_ts - 60):
            log.info(f"  ✅ Up-to-date: {desc}")
            return True
    return download_file(url, dest, desc)


def _extract_zip_if_changed(zip_path: Path, extract_dir: Path) -> bool:
    """Extract zip only when content changed since last extraction."""
    marker = extract_dir / ".extracted_from_mtime"
    zip_mtime = str(int(zip_path.stat().st_mtime))
    if extract_dir.exists() and marker.exists():
        if marker.read_text(encoding="utf-8").strip() == zip_mtime:
            log.info("  ✅ Geofabrik archive already extracted")
            return True

    if extract_dir.exists():
        shutil.rmtree(extract_dir, ignore_errors=True)
    extract_dir.mkdir(parents=True, exist_ok=True)

    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(extract_dir)
    marker.write_text(zip_mtime, encoding="utf-8")
    log.info(f"  ✅ Extracted: {extract_dir}")
    return True


def crawl_vietnam_data(force_refresh: bool = False) -> Dict[str, Optional[Path]]:
    """Crawl/update Vietnam-only source datasets used by the pipeline."""
    log.info("═" * 60)
    log.info("🛰️  CRAWL: Vietnam-only updated data sources")
    log.info("═" * 60)

    out = {}
    base = Path("data") / "sources" / "vietnam"
    boundary_dir = base / "boundary"
    osm_dir = base / "osm"
    boundary_dir.mkdir(parents=True, exist_ok=True)
    osm_dir.mkdir(parents=True, exist_ok=True)

    gadm_path = boundary_dir / "gadm41_VNM_1.json"
    od_path = boundary_dir / "od_mekong_provinces.geojson"
    shp_zip = osm_dir / "vietnam-latest-free.shp.zip"
    pbf_path = osm_dir / "vietnam-latest.osm.pbf"
    shp_extract = osm_dir / "vietnam-latest-free-shp"

    # Boundaries (Vietnam only)
    if force_refresh:
        download_file(GADM_URL, gadm_path, "GADM Vietnam")
        download_file(OD_MEKONG_BOUNDARY_URL, od_path, "OD Mekong Vietnam provinces")
    else:
        _download_if_newer(GADM_URL, gadm_path, "GADM Vietnam")
        _download_if_newer(OD_MEKONG_BOUNDARY_URL, od_path, "OD Mekong Vietnam provinces")

    # OSM Vietnam (daily Geofabrik)
    if force_refresh:
        ok_shp = download_file(GEOFABRIK_VN_SHP_URL, shp_zip, "Geofabrik Vietnam shapefiles")
    else:
        ok_shp = _download_if_newer(GEOFABRIK_VN_SHP_URL, shp_zip, "Geofabrik Vietnam shapefiles")
    if ok_shp and shp_zip.exists():
        _extract_zip_if_changed(shp_zip, shp_extract)

    # Keep latest PBF too (optional for downstream tooling)
    if force_refresh:
        download_file(GEOFABRIK_VN_PBF_URL, pbf_path, "Geofabrik Vietnam PBF")
    else:
        _download_if_newer(GEOFABRIK_VN_PBF_URL, pbf_path, "Geofabrik Vietnam PBF")

    out["gadm"] = gadm_path if gadm_path.exists() else None
    out["od_mekong"] = od_path if od_path.exists() else None
    out["osm_shp_zip"] = shp_zip if shp_zip.exists() else None
    out["osm_shp_dir"] = shp_extract if shp_extract.exists() else None
    out["osm_pbf"] = pbf_path if pbf_path.exists() else None

    log.info("  ✅ Crawl done")
    for k, v in out.items():
        if v is not None:
            mb = v.stat().st_size / (1024 * 1024) if v.is_file() else 0
            if v.is_file():
                log.info(f"     {k:<12}: {v} ({mb:.1f} MB)")
            else:
                log.info(f"     {k:<12}: {v}")
    return out


def get_utm_epsg(lon: float, lat: float) -> int:
    """Get UTM EPSG code for a given lon/lat."""
    zone = int((lon + 180) / 6) + 1
    return 32600 + zone if lat >= 0 else 32700 + zone


def _build_overviews(path: Path):
    """Add GDAL overview pyramids to an existing GeoTIFF for fast low-zoom reads."""
    with rasterio.open(path, 'r+') as ds:
        factors = [f for f in [2, 4, 8, 16]
                   if ds.width // f >= 1 and ds.height // f >= 1]
        if factors:
            ds.build_overviews(factors, Resampling.average)
            ds.update_tags(ns='rio_overview', resampling='average')


def _write_layer_stats(tif_path: Path):
    """Compute 2nd/98th percentile stats and write a sidecar JSON for the tile server."""
    try:
        with rasterio.open(tif_path) as src:
            data = src.read(1).astype(float)
            nd = src.nodata
        if nd is not None:
            data[data == nd] = np.nan
        valid = data[~np.isnan(data)]
        if len(valid) == 0:
            return
        vmin = float(np.percentile(valid, 2))
        vmax = float(np.percentile(valid, 98))
        if vmax - vmin < 1e-6:
            return
        stats_path = tif_path.with_suffix('.stats.json')
        stats_path.write_text(json.dumps({"vmin": vmin, "vmax": vmax}))
    except Exception as e:
        log.warning(f"  Could not write stats for {tif_path.name}: {e}")


def write_raster(data: np.ndarray, path: Path, crs, transform,
                 nodata: float = -9999.0, dtype='float32'):
    """Write a numpy array as a tiled GeoTIFF with GDAL overviews."""
    if data.ndim == 2:
        data = data[np.newaxis, :]
    profile = {
        'driver': 'GTiff', 'dtype': dtype,
        'width': data.shape[2], 'height': data.shape[1], 'count': data.shape[0],
        'crs': crs, 'transform': transform, 'nodata': nodata,
        'compress': 'deflate', 'tiled': True,
        'blockxsize': 256, 'blockysize': 256,
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    with rasterio.open(path, 'w', **profile) as dst:
        dst.write(data)
    _build_overviews(path)


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
            'transform': clipped_tf, 'nodata': nd,
            'compress': 'deflate', 'tiled': True,
            'blockxsize': 256, 'blockysize': 256,
        })
    dst_path.parent.mkdir(parents=True, exist_ok=True)
    with rasterio.open(dst_path, 'w', **profile) as dst:
        dst.write(clipped)
    _build_overviews(dst_path)


def _clip_cog_url_to_boundary(url: str, out_path: Path,
                              boundary: gpd.GeoDataFrame,
                              dst_crs: str,
                              resampling: Resampling = Resampling.nearest,
                              nodata=None):
    """Read a remote/local raster (COG recommended), window-read by boundary bbox, then clip + (optionally) reproject.

    This is designed for global public layers (OpenLandMap, etc.) so we don't download the whole world.
    """
    boundary_ll = boundary.to_crs("EPSG:4326")
    b = boundary_ll.total_bounds  # lon/lat
    with rasterio.open(url) as src:
        src_crs = src.crs
        if src_crs is None:
            src_crs = "EPSG:4326"
        # Window in source CRS (assume EPSG:4326 for these global layers)
        win = rasterio.windows.from_bounds(b[0], b[1], b[2], b[3], transform=src.transform)
        win = win.round_offsets().round_lengths()
        data = src.read(1, window=win)
        tf = src.window_transform(win)
        nd = src.nodata if nodata is None else nodata

        # Write a temp subset in source CRS, then use existing clip util (which handles crop + nodata)
        tmp = out_path.with_suffix(".subset.tif")
        write_raster(data, tmp, src.crs, tf, nodata=nd, dtype=str(data.dtype))

    # Reproject subset to desired CRS/grid if needed
    if str(src_crs) != str(dst_crs):
        tmp2 = out_path.with_suffix(".reproj.tif")
        # Use rasterio reproject to new CRS at approx same resolution
        with rasterio.open(tmp) as s:
            dst_tf, dst_w, dst_h = calculate_default_transform(s.crs, dst_crs, s.width, s.height, *s.bounds)
            dst_nd = s.nodata
            dst = np.full((dst_h, dst_w), dst_nd if dst_nd is not None else 0, dtype=s.dtypes[0])
            reproject(
                source=rasterio.band(s, 1),
                destination=dst,
                src_transform=s.transform,
                src_crs=s.crs,
                dst_transform=dst_tf,
                dst_crs=dst_crs,
                src_nodata=s.nodata,
                dst_nodata=dst_nd,
                resampling=resampling,
            )
        write_raster(dst, tmp2, dst_crs, dst_tf, nodata=dst_nd if dst_nd is not None else 0, dtype=str(dst.dtype))
        tmp.unlink(missing_ok=True)
        tmp = tmp2

    # Final clip to boundary in dst CRS
    clip_raster_to_boundary(tmp, out_path, boundary, dst_crs)
    try:
        tmp.unlink()
    except Exception:
        pass
    _write_layer_stats(out_path)
    return out_path


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
            log.info("  Trying OD Mekong (Vietnamese admin boundary)...")
            od_cache = output_dir / "raw" / "od_mekong_provinces.geojson"
            if not download_file(OD_MEKONG_BOUNDARY_URL, od_cache,
                                 "OD Mekong Vietnam Provinces"):
                raise RuntimeError("Failed to download boundary data from GADM and OD Mekong")
            cache = od_cache

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
    _write_layer_stats(dem_path)
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

    log.info("  Computing flow accumulation (D8)...")
    # Use a filled DEM for stable routing (still masked later)
    flow_acc = _flow_accumulation(dem_data.astype(np.float32))

    log.info("  Computing TWI / SPI / STI...")
    # Convert slope to radians for tan/sin; guard against zeros.
    slope_rad = np.radians(slope.astype(np.float32))
    tan_slope = np.tan(slope_rad)
    sin_slope = np.sin(slope_rad)
    tan_slope = np.maximum(tan_slope, 1e-6)
    sin_slope = np.maximum(sin_slope, 1e-6)

    # Use contributing area proxy (cells) -> approximate specific catchment area.
    # (This is sufficient for relative-index mapping at province scale.)
    a = np.maximum(flow_acc, 1.0)
    twi = np.log(a / tan_slope).astype(np.float32)
    spi = np.log(a * tan_slope).astype(np.float32)

    # Sediment Transport Index (Moore & Burch style; common LS mapping defaults)
    # STI = ( (a * cs / 22.13)^m ) * ( (sin(slope) / 0.0896)^n )
    m, n = 0.4, 1.3
    sti = ((a * cs / 22.13) ** m) * ((sin_slope / 0.0896) ** n)
    sti = sti.astype(np.float32)

    log.info("  Computing TRI (terrain ruggedness index)...")
    # TRI = std-dev of elevation in a 3x3 neighborhood (Riley et al. variant).
    try:
        from scipy.ndimage import generic_filter

        def _nanstd(w):
            w = np.asarray(w, dtype=np.float32)
            w = w[~np.isnan(w)]
            return float(np.std(w)) if w.size else np.nan

        tri = generic_filter(dem_data.astype(np.float32), _nanstd, size=3, mode="nearest")
        tri = tri.astype(np.float32)
    except Exception:
        # Fallback: rough proxy using gradients.
        dy, dx = np.gradient(dem_data.astype(np.float32), cs)
        tri = np.sqrt(dx * dx + dy * dy).astype(np.float32)

    log.info("  Computing SDC (slope degree classes)...")
    # SDC = categorical slope class raster (useful for mapping + modelling).
    # Classes: 1:0–5, 2:5–15, 3:15–30, 4:30–45, 5:>45 (degrees)
    sdc = np.zeros_like(slope, dtype=np.uint8)
    sdc[(slope >= 0) & (slope < 5)] = 1
    sdc[(slope >= 5) & (slope < 15)] = 2
    sdc[(slope >= 15) & (slope < 30)] = 3
    sdc[(slope >= 30) & (slope < 45)] = 4
    sdc[(slope >= 45)] = 5

    log.info("  Computing hillshade...")
    dem_valid = dem_data.copy()
    dem_valid[np.isnan(dem_valid)] = 0
    hillshade = _compute_hillshade(dem_valid, cs).astype(np.float32)
    hillshade[nodata_mask] = -9999

    for arr in [dem_utm, slope, aspect, curvature, flow_acc, twi, spi, sti, tri]:
        arr[nodata_mask] = -9999
    sdc[nodata_mask] = 0

    native = output_dir / "native"
    native.mkdir(exist_ok=True)
    results = {}
    for name, data in [
        ('hillshade', hillshade),
        ('dem', dem_utm),
        ('slope', slope),
        ('aspect', aspect),
        ('curvature', curvature),
        ('flow_accumulation', flow_acc),
        ('twi', twi),
        ('spi', spi),
        ('sti', sti),
        ('tri', tri),
    ]:
        p = native / f"{name}.tif"
        write_raster(data, p, target_crs, tf)
        _write_layer_stats(p)
        results[name] = p
        log.info(f"  {name}: {p}")

    # Categorical outputs
    sdc_path = native / "sdc.tif"
    write_raster(sdc, sdc_path, target_crs, tf, nodata=0, dtype="uint8")
    results["sdc"] = sdc_path
    log.info(f"  sdc: {sdc_path}")

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
def _step_osm_overpass(boundary: gpd.GeoDataFrame, output_dir: Path) -> Dict[str, Optional[Path]]:
    """Download roads, rivers, infrastructure from OSM Overpass."""
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


def _step_osm_geofabrik(boundary: gpd.GeoDataFrame, output_dir: Path,
                        refresh: bool = False) -> Dict[str, Optional[Path]]:
    """Build OSM layers from Geofabrik Vietnam shapefile package."""
    log.info("═" * 60)
    log.info("🛣️  STEP 5: OSM Data (Geofabrik Vietnam daily extract)")
    log.info("═" * 60)

    local_shp_dir = Path("data") / "sources" / "vietnam" / "osm" / "vietnam-latest-free-shp"
    if refresh or not local_shp_dir.exists():
        crawled = crawl_vietnam_data(force_refresh=refresh)
        shp_dir = crawled.get("osm_shp_dir")
        if shp_dir is None:
            log.error("  ❌ Geofabrik shapefile directory unavailable")
            return {"roads": None, "rivers": None, "infrastructure": None}
    else:
        shp_dir = local_shp_dir
        log.info(f"  ✅ Using local Geofabrik extract: {shp_dir}")

    boundary_ll = boundary.to_crs("EPSG:4326")
    polygon = boundary_ll.geometry.values[0]
    minx, miny, maxx, maxy = boundary_ll.total_bounds
    bbox = (minx, miny, maxx, maxy)

    def _load_clip(shp_name: str) -> Optional[gpd.GeoDataFrame]:
        shp = Path(shp_dir) / shp_name
        if not shp.exists():
            log.warning(f"  ⚠ Missing Geofabrik layer: {shp_name}")
            return None
        try:
            gdf = gpd.read_file(shp, bbox=bbox)
            if gdf.empty:
                return gdf
            if gdf.crs is None:
                gdf = gdf.set_crs("EPSG:4326", allow_override=True)
            elif str(gdf.crs) != "EPSG:4326":
                gdf = gdf.to_crs("EPSG:4326")
            gdf = gdf[gdf.geometry.notnull()].copy()
            if gdf.empty:
                return gdf
            gdf = gdf[gdf.geometry.intersects(polygon)].copy()
            return gdf
        except Exception as e:
            log.warning(f"  ⚠ Failed reading {shp_name}: {e}")
            return None

    results = {"roads": None, "rivers": None, "infrastructure": None}
    layer_map = {
        "roads": ["gis_osm_roads_free_1.shp"],
        "rivers": ["gis_osm_waterways_free_1.shp"],
        "infrastructure": ["gis_osm_pois_free_1.shp", "gis_osm_buildings_a_free_1.shp"],
    }

    keep_cols = ["geometry", "name", "highway", "waterway", "amenity", "building", "fclass"]
    for layer_name, shp_list in layer_map.items():
        frames = []
        for shp in shp_list:
            gdf = _load_clip(shp)
            if gdf is not None and not gdf.empty:
                cols = [c for c in keep_cols if c in gdf.columns]
                if "geometry" not in cols:
                    cols = ["geometry"] + cols
                gdf = gdf[cols].copy()
                frames.append(gdf)
        if not frames:
            log.warning(f"  ⚠ No {layer_name} found from Geofabrik package")
            continue

        merged = gpd.GeoDataFrame(pd.concat(frames, ignore_index=True), crs="EPSG:4326")
        out = output_dir / f"{layer_name}.gpkg"
        merged.to_file(out, driver="GPKG")
        results[layer_name] = out
        log.info(f"  ✅ {layer_name}: {out} ({len(merged)} features)")

    return results


def step_osm(boundary: gpd.GeoDataFrame, output_dir: Path,
             source: str = "auto", refresh_vn_data: bool = False) -> Dict[str, Optional[Path]]:
    """Fetch OSM-derived layers using selected source."""
    source = (source or "auto").lower().strip()
    if source in {"geofabrik", "auto"}:
        geo = _step_osm_geofabrik(boundary, output_dir, refresh=refresh_vn_data)
        # Keep auto-fallback behavior if Geofabrik did not produce enough data.
        if source == "auto" and (geo.get("roads") is None or geo.get("rivers") is None):
            log.info("  Geofabrik incomplete, fallback to Overpass...")
            ov = _step_osm_overpass(boundary, output_dir)
            for k, v in ov.items():
                if geo.get(k) is None and v is not None:
                    geo[k] = v
        return geo
    return _step_osm_overpass(boundary, output_dir)


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
    _write_layer_stats(lc_path)
    log.info(f"  ✅ Landcover: {lc_path}")
    return lc_path


# ════════════════════════════════════════════════════════
#  STEP 6a: NDVI (NASA NEO MOD_NDVI_16)
# ════════════════════════════════════════════════════════
def step_ndvi(boundary: gpd.GeoDataFrame, output_dir: Path,
              target_crs: str) -> Optional[Path]:
    """Fetch latest MODIS NDVI (16-day) from NASA NEO as floating GeoTIFF and clip to province.

    Note: NEO floating GeoTIFF values are visualization-grade approximations.
    """
    log.info("═" * 60)
    log.info("🌱 STEP 6a: NDVI (NASA NEO MOD_NDVI_16, 16-day)")
    log.info("═" * 60)

    raw_dir = output_dir / "raw" / "ndvi"
    raw_dir.mkdir(parents=True, exist_ok=True)
    ndvi_out = output_dir / "native" / "ndvi.tif"
    ndvi_out.parent.mkdir(exist_ok=True)

    # Grab the latest scene id (si=...) from the NEO dataset page.
    try:
        html = requests.get(NEO_VIEW_MOD_NDVI_16, timeout=(10, 30)).text
        import re
        m = re.search(r"RenderData\\?si=(\\d+)", html)
        if not m:
            log.warning("  ⚠ Could not parse NEO scene id for NDVI; skipping")
            return None
        si = m.group(1)
        # Floating point GeoTIFF. NEO uses the same RenderData endpoint; format names vary slightly.
        # We try a couple of known variants.
        candidates = [
            f"https://neo.gsfc.nasa.gov/servlet/RenderData?si={si}&cs=gs&format=GeoTIFF_Float&width=3600&height=1800",
            f"https://neo.gsfc.nasa.gov/servlet/RenderData?si={si}&cs=gs&format=GEOTIFF_FLOAT&width=3600&height=1800",
        ]
        ndvi_raw = raw_dir / f"MOD_NDVI_16_si{si}.tif"
        if not ndvi_raw.exists():
            ok = False
            for u in candidates:
                if download_http_stream(u, ndvi_raw, desc="NEO NDVI (float GeoTIFF)", retries=2, timeout_s=120):
                    ok = True
                    break
            if not ok:
                log.warning("  ⚠ NDVI download failed; skipping")
                return None
    except Exception as e:
        log.warning(f"  ⚠ NDVI fetch failed: {e}")
        return None

    # Clip/reproject to map CRS
    try:
        _clip_cog_url_to_boundary(str(ndvi_raw), ndvi_out, boundary, target_crs,
                                  resampling=Resampling.bilinear, nodata=-9999.0)
        log.info(f"  ✅ NDVI: {ndvi_out}")
        return ndvi_out
    except Exception as e:
        log.warning(f"  ⚠ NDVI processing failed: {e}")
        return None


# ════════════════════════════════════════════════════════
#  STEP 6d: Geology (Lithology classes - OpenLandMap)
# ════════════════════════════════════════════════════════
def step_geology(boundary: gpd.GeoDataFrame, output_dir: Path,
                 target_crs: str) -> Optional[Path]:
    """Fetch lithology classes (proxy for geology) from OpenLandMap and clip to province."""
    log.info("═" * 60)
    log.info("🪨 STEP 6d: Geology (Lithology classes - OpenLandMap)")
    log.info("═" * 60)

    out = output_dir / "native" / "geology_lithology.tif"
    out.parent.mkdir(exist_ok=True)
    try:
        _clip_cog_url_to_boundary(OPENLANDMAP_LITHOLOGY_COG, out, boundary, target_crs,
                                  resampling=Resampling.nearest, nodata=0)
        log.info(f"  ✅ Geology (lithology): {out}")
        return out
    except Exception as e:
        log.warning(f"  ⚠ Geology fetch failed: {e}")
        return None


# ════════════════════════════════════════════════════════
#  STEP 6e: Forest type (MODIS MCD12Q1 IGBP type1 - OpenLandMap)
# ════════════════════════════════════════════════════════
def step_forest_type(boundary: gpd.GeoDataFrame, output_dir: Path,
                     target_crs: str) -> Optional[Path]:
    """Fetch MODIS land cover type 1 (IGBP) and clip to province (forest types are a subset of classes)."""
    log.info("═" * 60)
    log.info("🌲 STEP 6e: Forest Type (MODIS MCD12Q1 IGBP - OpenLandMap)")
    log.info("═" * 60)

    out = output_dir / "native" / "forest_type_igbp.tif"
    out.parent.mkdir(exist_ok=True)
    try:
        _clip_cog_url_to_boundary(OPENLANDMAP_MCD12Q1_T1_2021_COG, out, boundary, target_crs,
                                  resampling=Resampling.nearest, nodata=0)
        log.info(f"  ✅ Forest type (IGBP): {out}")
        return out
    except Exception as e:
        log.warning(f"  ⚠ Forest type fetch failed: {e}")
        return None

# ════════════════════════════════════════════════════════
#  STEP 6b: POPULATION (GSO Vietnam – Tổng cục Thống kê)
# ════════════════════════════════════════════════════════
def _lookup_gso_population(province_names: List[str]) -> List[Dict]:
    """Return per-province GSO stats for the given GADM NAME_1 list."""
    # Build a normalized lookup to handle input variants such as:
    # "BìnhDương", "BàRịa-VũngTàu", "HồChíMinh", etc.
    norm_gso = {_normalize_vn(k): (k, v) for k, v in GSO_PROVINCE_DATA.items()}
    results = []
    for name in province_names:
        entry = GSO_PROVINCE_DATA.get(name)
        used_name = name
        if entry is None:
            norm = _normalize_vn(name)
            hit = norm_gso.get(norm)
            if hit is not None:
                used_name, entry = hit
        if entry:
            pop = entry['population']
            area = entry['area_km2']
            results.append({
                'name': used_name,
                'population': pop,
                'area_km2': area,
                'density_per_km2': round(pop / area, 1) if area > 0 else 0,
            })
        else:
            log.warning(f"  ⚠ No GSO data for '{name}'")
    return results


def step_population(boundary: gpd.GeoDataFrame,
                    internal_boundaries: Optional[gpd.GeoDataFrame],
                    output_dir: Path) -> Optional[Dict]:
    """Look up official GSO population statistics for the province."""
    log.info("═" * 60)
    log.info("👥 STEP 6b: Population (GSO Vietnam 2023)")
    log.info("═" * 60)

    if internal_boundaries is not None and len(internal_boundaries) > 1:
        names = internal_boundaries['NAME_1'].tolist()
    else:
        name = boundary.iloc[0].get('NAME_1', '')
        names = [name] if name else []

    constituents = _lookup_gso_population(names)
    if not constituents:
        log.warning("  ⚠ Province not found in GSO dataset")
        return None

    total_pop = sum(c['population'] for c in constituents)
    total_area = sum(c['area_km2'] for c in constituents)
    density = round(total_pop / total_area, 1) if total_area > 0 else 0

    result = {
        'population': total_pop,
        'area_km2': round(total_area, 2),
        'density_per_km2': density,
        'source': 'GSO Vietnam (Tổng cục Thống kê) 2023',
        'constituents': constituents,
    }

    log.info(f"  Population: {total_pop:,.0f}  (source: GSO 2023)")
    log.info(f"  Area (GSO): {total_area:,.1f} km²")
    log.info(f"  Density:    {density:,.1f} /km²")
    for c in constituents:
        log.info(f"    {c['name']:<20} pop {c['population']:>10,}  "
                 f"({c['density_per_km2']:,.0f}/km²)")

    pop_json = output_dir / "gso_population.json"
    with open(pop_json, 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    log.info(f"  ✅ GSO data: {pop_json}")

    return result


# ════════════════════════════════════════════════════════
#  STEP 6c: SOCIOECONOMIC STATISTICS
# ════════════════════════════════════════════════════════
def step_socioeconomic(boundary: gpd.GeoDataFrame,
                       internal_boundaries: Optional[gpd.GeoDataFrame],
                       gso_pop: Optional[Dict],
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
    boundary_proj = boundary.to_crs(epsg=3405)

    # --- Area (computed from geometry; GSO value kept for reference) ---
    area_m2 = boundary_proj.geometry.area.sum()
    area_km2 = area_m2 / 1e6
    stats['area_km2'] = round(area_km2, 2)
    log.info(f"  Area: {area_km2:,.1f} km²")

    # --- Population from GSO (Tổng cục Thống kê) ---
    if gso_pop:
        total_pop = gso_pop['population']
        stats['population'] = total_pop
        stats['population_density_per_km2'] = round(total_pop / area_km2, 1) if area_km2 > 0 else 0
        stats['population_source'] = gso_pop.get('source', 'GSO Vietnam 2023')
        log.info(f"  Population: {total_pop:,.0f}  (GSO 2023)")
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

    # --- Per-constituent-province stats (GSO data for merged provinces) ---
    if gso_pop and gso_pop.get('constituents') and len(gso_pop['constituents']) > 1:
        constituent_stats = []
        for c in gso_pop['constituents']:
            sub_name = c['name']
            sub_pop = c['population']
            sub_area = c['area_km2']
            constituent_stats.append({
                'name': sub_name,
                'area_km2': sub_area,
                'population': sub_pop,
                'density_per_km2': c['density_per_km2'],
            })
            log.info(f"    {sub_name}: {sub_area:,.0f} km², "
                     f"pop {sub_pop:,.0f}, density {c['density_per_km2']:,.0f}/km²")
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
            src_tag = stats.get('population_source', 'GSO 2023')
            f.write(f"  Population ({src_tag}):\n")
            f.write(f"                      {stats['population']:>12,}\n")
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


def _read_raster_for_map(raster_path: Path, target_crs: str,
                         resampling: Resampling = Resampling.bilinear):
    """Read a raster and (if needed) reproject it to the map CRS.

    This prevents CRS mismatches (e.g., EPSG:4326 rasters plotted under UTM boundaries)
    that can explode the figure extent and crash Matplotlib.
    """
    with rasterio.open(raster_path) as src:
        data = src.read(1)
        tf = src.transform
        crs = src.crs
        nd = src.nodata

        if crs is None or str(crs) == str(target_crs):
            return data, tf, nd

        dst_tf, dst_w, dst_h = calculate_default_transform(
            crs, target_crs, src.width, src.height, *src.bounds
        )

        # Choose a nodata that matches dtype when missing.
        if nd is None:
            if np.issubdtype(data.dtype, np.unsignedinteger):
                dst_nd = 0
            elif np.issubdtype(data.dtype, np.integer):
                dst_nd = -9999
            else:
                dst_nd = -9999.0
        else:
            dst_nd = nd

        dst = np.full((dst_h, dst_w), dst_nd, dtype=data.dtype)
        reproject(
            source=data,
            destination=dst,
            src_transform=tf,
            src_crs=crs,
            dst_transform=dst_tf,
            dst_crs=target_crs,
            src_nodata=nd,
            dst_nodata=dst_nd,
            resampling=resampling,
        )
        return dst, dst_tf, dst_nd


def _save_raster_map(raster_path: Path, boundary: gpd.GeoDataFrame,
                     target_crs: str, title: str, out_png: Path,
                     cmap='terrain', vmin=None, vmax=None, label='',
                     hillshade: bool = False, dem_path: Optional[Path] = None,
                     internal_boundaries: Optional[gpd.GeoDataFrame] = None):
    """Generate ArcGIS-style PNG for a raster layer."""
    boundary_utm = boundary.to_crs(target_crs)
    ib_utm = internal_boundaries.to_crs(target_crs) if internal_boundaries is not None else None
    fig, ax = _make_fig(boundary_utm, title, internal_boundaries_utm=ib_utm)

    data, tf, nd = _read_raster_for_map(raster_path, target_crs, resampling=Resampling.bilinear)

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
            dem_data, dem_tf, dem_nd = _read_raster_for_map(dem_path, target_crs, resampling=Resampling.bilinear)
            hs_data = dem_data.astype(float)
            if dem_nd is not None:
                hs_data[dem_data == dem_nd] = np.nan
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

    data, tf, nd = _read_raster_for_map(raster_path, target_crs, resampling=Resampling.nearest)

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
    if nd is not None:
        data_plot[data == nd] = np.nan

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


def _save_population_map(gso_pop: Dict, boundary: gpd.GeoDataFrame,
                         target_crs: str, out_png: Path,
                         internal_boundaries: Optional[gpd.GeoDataFrame] = None):
    """Generate population density choropleth from GSO census data."""
    boundary_utm = boundary.to_crs(target_crs)
    ib_utm = internal_boundaries.to_crs(target_crs) if internal_boundaries is not None else None
    fig, ax = _make_fig(boundary_utm, "Population Density (GSO 2023)",
                        internal_boundaries_utm=ib_utm)

    constituents = gso_pop.get('constituents', [])
    pop_cmap = plt.cm.YlOrRd

    if ib_utm is not None and len(ib_utm) > 1 and len(constituents) > 1:
        density_lookup = {c['name']: c['density_per_km2'] for c in constituents}
        densities = [density_lookup.get(row['NAME_1'], 0) for _, row in ib_utm.iterrows()]
        vmin, vmax = min(densities), max(densities)
        if vmax <= vmin:
            vmax = vmin + 1

        norm = mcolors.Normalize(vmin=vmin, vmax=vmax)
        for _, row in ib_utm.iterrows():
            d = density_lookup.get(row['NAME_1'], 0)
            color = pop_cmap(norm(d))
            gpd.GeoDataFrame([row], crs=ib_utm.crs).plot(
                ax=ax, facecolor=color, edgecolor='#666666',
                linewidth=0.8, alpha=0.85, zorder=2)
            centroid = row.geometry.centroid
            label = f"{row['NAME_1']}\n{density_lookup.get(row['NAME_1'], 0):,.0f}/km²"
            ax.text(centroid.x, centroid.y, label,
                    fontsize=6, ha='center', va='center', fontweight='bold',
                    color='#333333', zorder=12,
                    bbox=dict(boxstyle='round,pad=0.2', fc='white',
                              ec='none', alpha=0.7))
    else:
        d = gso_pop.get('density_per_km2', 0)
        vmin, vmax = 0, max(d, 1)
        norm = mcolors.Normalize(vmin=0, vmax=vmax)
        color = pop_cmap(norm(d))
        boundary_utm.plot(ax=ax, facecolor=color, edgecolor=_ARCGIS_FRAME,
                          linewidth=1.5, alpha=0.85, zorder=2)

    sm = plt.cm.ScalarMappable(cmap=pop_cmap, norm=norm)
    sm.set_array([])
    _make_arcgis_colorbar(fig, ax, sm, label='Population density (per km²)')

    total_pop = gso_pop.get('population', 0)
    ax.text(0.02, 0.02, f"Total: {total_pop:,.0f}", transform=ax.transAxes,
            fontsize=9, fontweight='bold', va='bottom', color=_ARCGIS_LABEL,
            zorder=51,
            bbox=dict(boxstyle='round,pad=0.3', fc='white', ec=_ARCGIS_FRAME,
                      alpha=0.9, linewidth=0.8))

    b = boundary_utm.total_bounds
    ax.set_xlim(b[0], b[2])
    ax.set_ylim(b[1], b[3])
    _finalize_map(fig, ax, out_png,
                  attribution='Data: GSO Vietnam (Tổng cục Thống kê) 2023')


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
                  attribution='NQ 202/2025/QH15 | GSO | ESA | OSM')


def step_maps(output_dir: Path, boundary: gpd.GeoDataFrame, target_crs: str,
              raster_paths: dict, osm_paths: dict,
              dist_paths: dict, contour_path: Optional[Path],
              gso_pop: Optional[Dict] = None,
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
        'tri':               ('TRI — Terrain Ruggedness',  'magma',      'Ruggedness',      True),
        'flow_accumulation': ('Flow Accumulation (D8)',    'Blues',      'Upstream Cells',  True),
        'spi':               ('SPI — Stream Power Index',  'inferno',    'SPI',             True),
        'twi':               ('Topographic Wetness Index', 'YlGnBu',    'TWI',             True),
        'sti':               ('STI — Sediment Transport',  'plasma',     'STI',             True),
        'ndvi':              ('NDVI (MODIS - NASA NEO)',   'YlGn',       'NDVI',            False),
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

    # Geology (lithology classes)
    if 'geology' in raster_paths and raster_paths['geology'].exists():
        _save_raster_map(raster_paths['geology'], boundary, target_crs,
                         "Geology — Lithology Classes (OpenLandMap)",
                         maps_dir / "geology.png",
                         cmap='tab20', label='Class', hillshade=False,
                         internal_boundaries=ib)

    # Forest type (IGBP land cover classes)
    if 'forest_type' in raster_paths and raster_paths['forest_type'].exists():
        _save_raster_map(raster_paths['forest_type'], boundary, target_crs,
                         "Forest Type (MODIS IGBP classes, 2021)",
                         maps_dir / "forest_type.png",
                         cmap='tab20', label='Class', hillshade=False,
                         internal_boundaries=ib)

    # SDC (slope degree classes) categorical
    if 'sdc' in raster_paths and raster_paths['sdc'].exists():
        # Reuse landcover-style categorical rendering with a small custom palette
        # by mapping classes to colors via imshow + legend.
        boundary_utm = boundary.to_crs(target_crs)
        ib_utm = ib.to_crs(target_crs) if ib is not None else None
        fig, ax = _make_fig(boundary_utm, "SDC — Slope Degree Classes", internal_boundaries_utm=ib_utm)
        data, tf, nd = _read_raster_for_map(raster_paths['sdc'], target_crs, resampling=Resampling.nearest)
        extent = [tf[2], tf[2] + tf[0] * data.shape[1],
                  tf[5] + tf[4] * data.shape[0], tf[5]]
        sdc_colors = ['#00000000', '#C8E6C9', '#FFE082', '#FFB74D', '#E57373', '#8E24AA']
        cmap = mcolors.ListedColormap(sdc_colors)
        plot = data.astype(float)
        plot[plot == 0] = np.nan
        ax.imshow(plot, extent=extent, cmap=cmap, origin='upper', alpha=0.92, zorder=1, interpolation='nearest')
        patches = [
            Patch(facecolor=sdc_colors[1], edgecolor='#555555', linewidth=0.5, label='  0–5°'),
            Patch(facecolor=sdc_colors[2], edgecolor='#555555', linewidth=0.5, label='  5–15°'),
            Patch(facecolor=sdc_colors[3], edgecolor='#555555', linewidth=0.5, label='  15–30°'),
            Patch(facecolor=sdc_colors[4], edgecolor='#555555', linewidth=0.5, label='  30–45°'),
            Patch(facecolor=sdc_colors[5], edgecolor='#555555', linewidth=0.5, label='  >45°'),
        ]
        leg = ax.legend(handles=patches, loc='lower right', fontsize=7.5,
                        framealpha=0.95, edgecolor=_ARCGIS_FRAME,
                        fancybox=False, title='Slope classes', title_fontsize=8,
                        borderpad=0.8)
        leg.get_frame().set_linewidth(0.8)
        leg.get_title().set_fontweight('bold')
        ax.set_xlim(extent[0], extent[1])
        ax.set_ylim(extent[2], extent[3])
        _finalize_map(fig, ax, maps_dir / "sdc.png", transform=tf,
                      attribution='Derived: DEM → slope classes')

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

    # Population density map (GSO choropleth)
    if gso_pop:
        _save_population_map(gso_pop, boundary, target_crs,
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
def _run_single_province(province: str, contour_interval: float,
                         output_dir: Path, legacy_boundaries: bool,
                         osm_source: str, refresh_vn_data: bool,
                         ndvi_path: Optional[str] = None,
                         geology_path: Optional[str] = None,
                         forest_type_path: Optional[str] = None):
    """Run pipeline for one province — generates only what the web map needs."""
    log.info("[*] VIETNAM GIS PIPELINE")
    log.info(f"   Province:   {province}")
    log.info("")

    output_dir.mkdir(parents=True, exist_ok=True)
    log.info(f"   Output: {output_dir.resolve()}")
    log.info("")

    # Clear cached tiles so the web app re-renders with fresh data.
    tiles_dir = output_dir / "tiles"
    if tiles_dir.exists():
        shutil.rmtree(tiles_dir, ignore_errors=True)
        log.info("  🗑 Cleared tile cache")

    # ── Step 1: Boundary ──
    boundary, internal_boundaries = step_boundary(
        province, output_dir, legacy=legacy_boundaries)

    centroid = boundary.to_crs("EPSG:4326").geometry.centroid.iloc[0]
    target_crs = f"EPSG:{get_utm_epsg(centroid.x, centroid.y)}"
    log.info(f"  Target CRS: {target_crs}")

    # ── Step 2: DEM ──
    dem_path = step_dem(boundary, output_dir)

    # ── Step 3: Terrain (hillshade, slope, aspect) ──
    terrain_paths = step_terrain(dem_path, output_dir, boundary, target_crs)

    # ── Step 4: Contour lines ──
    contour_path = step_contour(terrain_paths['dem'], output_dir, contour_interval)

    # ── Step 5: OSM infrastructure ──
    osm_paths = step_osm(boundary, output_dir, source=osm_source,
                         refresh_vn_data=refresh_vn_data)

    # ── Step 6: Land cover ──
    lc_path = step_landcover(boundary, output_dir)

    # ── Step 6a/6d/6e: NDVI + Geology + Forest type (public sources) ──
    ndvi_auto = step_ndvi(boundary, output_dir, target_crs)
    geology_auto = step_geology(boundary, output_dir, target_crs)
    forest_auto = step_forest_type(boundary, output_dir, target_crs)

    # ── Step 7: Population (GSO) ──
    gso_pop = step_population(boundary, internal_boundaries, output_dir)

    # ── Step 8: Socioeconomic statistics ──
    socio_stats = step_socioeconomic(
        boundary, internal_boundaries, gso_pop, lc_path,
        terrain_paths, osm_paths, target_crs, output_dir)

    # ── Step 7: Drainage proximity (distance-to) rasters ──
    # Build a grid matching the native DEM cellsize for fast processing.
    try:
        with rasterio.open(terrain_paths["dem"]) as _ds:
            _cs = float(abs(_ds.transform[0]))
    except Exception:
        _cs = 30.0
    grid = compute_target_grid(boundary, target_crs, res=_cs)
    dist_paths = step_distance(osm_paths, grid, boundary, output_dir)

    # ── Optional: NDVI / Geology / Forest type (user-provided sources) ──
    optional_rasters: Dict[str, Path] = {}
    if ndvi_path:
        p = Path(ndvi_path)
        if p.exists():
            optional_rasters["ndvi"] = p
        else:
            log.warning(f"  ⚠ NDVI path not found: {p}")

    optional_vectors: Dict[str, Optional[Path]] = {}
    if geology_path:
        p = Path(geology_path)
        if p.exists():
            optional_vectors["geology"] = p
        else:
            log.warning(f"  ⚠ Geology path not found: {p}")
    if forest_type_path:
        p = Path(forest_type_path)
        if p.exists():
            optional_vectors["forest_type"] = p
        else:
            log.warning(f"  ⚠ Forest type path not found: {p}")

    # ── Step 9: PNG maps (optional but expected) ──
    # Use whatever layers we have available at this point.
    raster_paths = dict(terrain_paths)
    if lc_path is not None:
        raster_paths["landcover"] = lc_path
    if ndvi_auto is not None:
        raster_paths["ndvi"] = ndvi_auto
    if geology_auto is not None:
        raster_paths["geology"] = geology_auto
    if forest_auto is not None:
        raster_paths["forest_type"] = forest_auto
    raster_paths.update(optional_rasters)
    step_maps(
        output_dir=output_dir,
        boundary=boundary,
        target_crs=target_crs,
        raster_paths=raster_paths,
        osm_paths={**osm_paths, **optional_vectors},
        dist_paths=dist_paths,
        contour_path=contour_path,
        gso_pop=gso_pop,
        internal_boundaries=internal_boundaries,
        socio_stats=socio_stats,
    )

    # ── Summary ──
    log.info("")
    log.info("═" * 60)
    log.info("PIPELINE COMPLETE!")
    log.info("═" * 60)
    prov_name = boundary.iloc[0].get('NAME_1', province)
    log.info(f"  Province: {prov_name}")
    if internal_boundaries is not None:
        old_names = internal_boundaries['NAME_1'].tolist()
        log.info(f"  Merged from: {', '.join(old_names)}")
    log.info(f"  Output: {output_dir.resolve()}")

    if socio_stats:
        if 'area_km2' in socio_stats:
            log.info(f"  Area:       {socio_stats['area_km2']:>10,.0f} km²")
        if 'population' in socio_stats:
            log.info(f"  Population: {socio_stats['population']:>10,.0f}")
        if 'urban_percent' in socio_stats:
            log.info(f"  Urban:      {socio_stats['urban_percent']:>10.1f}%")


@click.command()
@click.option('--province', default="", help='Province name. Leave empty to run all Vietnam.')
@click.option('--list-provinces', is_flag=True, default=False,
              help='List all available province names and exit')
@click.option('--contour-interval', default=10.0, type=float, hidden=True)
@click.option('--output-dir', default=None, type=str, hidden=True)
@click.option('--legacy-boundaries', is_flag=True, default=False, hidden=True)
@click.option('--osm-source', type=click.Choice(['auto', 'geofabrik', 'overpass']),
              default='geofabrik', hidden=True)
@click.option('--refresh-vn-data', is_flag=True, default=False, hidden=True)
@click.option('--crawl-vn-data-only', is_flag=True, default=False, hidden=True)
@click.option('--ndvi-path', default=None, type=str,
              help='Optional NDVI raster (GeoTIFF). If provided, a PNG is generated.')
@click.option('--geology-path', default=None, type=str,
              help='Optional geology layer (GeoPackage/GeoJSON/Shapefile). If provided, a PNG is generated.')
@click.option('--forest-type-path', default=None, type=str,
              help='Optional forest type layer (GeoPackage/GeoJSON/Shapefile). If provided, a PNG is generated.')
def main(province, list_provinces, contour_interval, output_dir,
         legacy_boundaries, osm_source, refresh_vn_data, crawl_vn_data_only,
         ndvi_path, geology_path, forest_type_path):
    """
    Vietnam GIS Pipeline.

    \b
    Generates map data for the web viewer.
    By default runs all 34 provinces. Use --province to run one.

    \b
    Examples:
        python pipeline.py                             # All Vietnam
        python pipeline.py --province "Hồ Chí Minh"   # One province
        python pipeline.py --list-provinces
    """
    if list_provinces:
        log.info("═" * 60)
        log.info("  AVAILABLE PROVINCES (34 units, NQ 202/2025/QH15)")
        log.info("═" * 60)
        for new_name, old_names in sorted(PROVINCE_MERGER_2025.items()):
            if len(old_names) > 1:
                log.info(f"  {new_name:<16} <- {', '.join(old_names)}")
            else:
                log.info(f"  {new_name:<16}   (unchanged)")
        return

    if crawl_vn_data_only:
        crawl_vietnam_data(force_refresh=refresh_vn_data)
        return

    province = (province or "").strip()
    if province:
        safe_name = province.replace(" ", "_").replace(".", "")
        out_dir = Path(output_dir) if output_dir else Path("data") / "province" / safe_name
        _run_single_province(province, contour_interval, out_dir,
                             legacy_boundaries, osm_source, refresh_vn_data,
                             ndvi_path=ndvi_path,
                             geology_path=geology_path,
                             forest_type_path=forest_type_path)
        return

    base_out = Path(output_dir) if output_dir else Path("data") / "province"
    base_out.mkdir(parents=True, exist_ok=True)

    provinces = sorted(PROVINCE_MERGER_2025.keys())
    log.info("═" * 60)
    log.info(f"  WHOLE VIETNAM — {len(provinces)} provinces")
    log.info("═" * 60)

    failed = []
    for i, p in enumerate(provinces, 1):
        safe_name = p.replace(" ", "_").replace(".", "")
        out_dir = base_out / safe_name
        log.info(f"\n[{i}/{len(provinces)}] {p}")
        try:
            _run_single_province(p, contour_interval, out_dir,
                                 legacy_boundaries, osm_source, refresh_vn_data)
        except Exception as e:
            log.error(f"Failed: {p}: {e}")
            failed.append((p, str(e)))

    log.info("")
    if failed:
        log.warning(f"Completed with {len(failed)} failures:")
        for p, err in failed:
            log.warning(f"  - {p}: {err}")
    else:
        log.info(f"All {len(provinces)} provinces done.")


if __name__ == '__main__':
    main()
