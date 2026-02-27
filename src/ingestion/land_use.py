"""
Land Use / Land Cover (NLCD) Ingestion

Pulls land use classification data from:
  1. USGS National Land Cover Database (NLCD) — 30m resolution
     via MRLC ArcGIS REST MapServer
  2. NLCD WMS services for raster queries

NLCD Classification Codes (Anderson Level II):
  11 = Open Water
  12 = Perennial Ice/Snow
  21 = Developed, Open Space
  22 = Developed, Low Intensity
  23 = Developed, Medium Intensity
  24 = Developed, High Intensity
  31 = Barren Land
  41 = Deciduous Forest
  42 = Evergreen Forest
  43 = Mixed Forest
  51 = Dwarf Scrub (Alaska only)
  52 = Shrub/Scrub
  71 = Grassland/Herbaceous
  72 = Sedge/Herbaceous (Alaska only)
  73 = Lichens (Alaska only)
  74 = Moss (Alaska only)
  81 = Pasture/Hay
  82 = Cultivated Crops
  90 = Woody Wetlands
  95 = Emergent Herbaceous Wetlands

BESS suitability by land cover:
  - EXCELLENT: 82 (Cultivated Crops), 81 (Pasture/Hay),
               71 (Grassland), 31 (Barren)
  - GOOD: 52 (Shrub/Scrub), 21 (Developed Open Space)
  - MARGINAL: 22 (Dev Low), 41-43 (Forest)
  - POOR: 23-24 (Dev Med/High), 11 (Water), 90/95 (Wetlands)
"""

import logging
from typing import Optional, Dict, Tuple
from pathlib import Path

import pandas as pd
import numpy as np

from ..utils.api_client import APIClient, ArcGISClient

logger = logging.getLogger(__name__)

# ── NLCD ArcGIS endpoints ────────────────────────────────────────
# MRLC NLCD MapServer — 30m land cover
NLCD_MAPSERVER = (
    "https://www.mrlc.gov/arcgis/rest/services/NLCD/"
    "USGS_EDC_LandCover_NLCD/MapServer"
)

# National Map NLCD (100m — faster for broad queries)
NLCD_NATIONALMAP = (
    "https://smallscale.nationalmap.gov/arcgis/rest/services/"
    "LandCover/MapServer"
)

# ── NLCD Classification System ───────────────────────────────────
NLCD_CLASSES = {
    11: {"name": "Open Water", "category": "Water", "bess_score": 0},
    12: {"name": "Perennial Ice/Snow", "category": "Water", "bess_score": 0},
    21: {"name": "Developed, Open Space", "category": "Developed", "bess_score": 65},
    22: {"name": "Developed, Low Intensity", "category": "Developed", "bess_score": 40},
    23: {"name": "Developed, Medium Intensity", "category": "Developed", "bess_score": 20},
    24: {"name": "Developed, High Intensity", "category": "Developed", "bess_score": 10},
    31: {"name": "Barren Land", "category": "Barren", "bess_score": 85},
    41: {"name": "Deciduous Forest", "category": "Forest", "bess_score": 30},
    42: {"name": "Evergreen Forest", "category": "Forest", "bess_score": 25},
    43: {"name": "Mixed Forest", "category": "Forest", "bess_score": 25},
    51: {"name": "Dwarf Scrub", "category": "Shrubland", "bess_score": 75},
    52: {"name": "Shrub/Scrub", "category": "Shrubland", "bess_score": 75},
    71: {"name": "Grassland/Herbaceous", "category": "Grassland", "bess_score": 90},
    72: {"name": "Sedge/Herbaceous", "category": "Grassland", "bess_score": 70},
    73: {"name": "Lichens", "category": "Grassland", "bess_score": 60},
    74: {"name": "Moss", "category": "Grassland", "bess_score": 50},
    81: {"name": "Pasture/Hay", "category": "Agriculture", "bess_score": 95},
    82: {"name": "Cultivated Crops", "category": "Agriculture", "bess_score": 90},
    90: {"name": "Woody Wetlands", "category": "Wetlands", "bess_score": 5},
    95: {"name": "Emergent Herbaceous Wetlands", "category": "Wetlands", "bess_score": 5},
}

# Simplified categories for reporting
BESS_SUITABILITY = {
    "Excellent": [82, 81, 71, 31, 52],  # Crops, Pasture, Grassland, Barren, Scrub
    "Good": [21, 51, 72],                # Developed Open, Dwarf Scrub, Sedge
    "Marginal": [22, 41, 42, 43],        # Dev Low, Forests
    "Poor": [23, 24, 11, 12, 90, 95],    # Dev Med/High, Water, Wetlands
}


class LandUseIngestor:
    """
    Ingests USGS NLCD land use/land cover data.

    Provides:
      - get_land_cover_at_point(): NLCD class at a specific coordinate
      - get_land_cover_in_area(): NLCD composition within a bounding box
      - score_land_suitability(): Score a location for BESS suitability
      - get_land_use_summary(): Summary for pipeline output
    """

    def __init__(self, config: dict):
        self.config = config
        self.arcgis = ArcGISClient(
            cache_dir=config.get("cache", {}).get("directory", "./data/cache"),
            cache_enabled=config.get("cache", {}).get("enabled", True),
        )
        self.cache_hours = config.get("cache", {}).get("grid_data_expiry_hours", 720)

    def get_land_cover_at_point(
        self,
        lat: float,
        lon: float,
    ) -> Dict:
        """
        Get NLCD land cover classification at a specific point.

        Uses the NLCD MapServer identify operation to query
        the raster value at the given coordinate.

        Returns:
            Dict with nlcd_code, name, category, bess_score
        """
        try:
            # Use the MapServer identify endpoint
            url = f"{NLCD_MAPSERVER}/identify"
            params = {
                "geometry": f"{lon},{lat}",
                "geometryType": "esriGeometryPoint",
                "sr": "4326",
                "layers": "all",
                "tolerance": 2,
                "mapExtent": f"{lon-0.01},{lat-0.01},{lon+0.01},{lat+0.01}",
                "imageDisplay": "400,300,96",
                "returnGeometry": "false",
                "f": "json",
            }

            data = self.arcgis.get(url, params=params, cache_hours=self.cache_hours)

            if not data or "results" not in data:
                return self._default_response(lat, lon)

            # Parse the identify results
            for result in data.get("results", []):
                attrs = result.get("attributes", {})
                # NLCD raster value
                pixel_value = attrs.get("Pixel Value", attrs.get("Class_Value", None))
                if pixel_value is not None:
                    try:
                        code = int(float(pixel_value))
                    except (ValueError, TypeError):
                        continue

                    info = NLCD_CLASSES.get(code, {})
                    return {
                        "lat": lat,
                        "lon": lon,
                        "nlcd_code": code,
                        "name": info.get("name", f"Unknown ({code})"),
                        "category": info.get("category", "Unknown"),
                        "bess_score": info.get("bess_score", 50),
                        "source": "NLCD",
                    }

            return self._default_response(lat, lon)

        except Exception as e:
            logger.warning(f"  NLCD query failed at ({lat}, {lon}): {e}")
            return self._default_response(lat, lon)

    def _default_response(self, lat: float, lon: float) -> Dict:
        return {
            "lat": lat,
            "lon": lon,
            "nlcd_code": None,
            "name": "Unknown",
            "category": "Unknown",
            "bess_score": 50,
            "source": "default",
        }

    def get_land_cover_in_area(
        self,
        lat: float,
        lon: float,
        radius_miles: float = 1.0,
        sample_points: int = 9,
    ) -> Dict:
        """
        Sample NLCD land cover within a radius to get composition.

        Creates a grid of sample points within the radius and queries
        each for land cover class. Returns composition percentages.

        Args:
            lat, lon: Center coordinate
            radius_miles: Search radius
            sample_points: Number of points to sample (3x3=9, 5x5=25, etc.)
        """
        import math

        # Create grid of sample points
        side = int(math.sqrt(sample_points))
        if side < 3:
            side = 3

        lat_step = radius_miles / 69.0  # approx degrees per mile
        lon_step = radius_miles / (69.0 * math.cos(math.radians(lat)))

        results = []
        for i in range(side):
            for j in range(side):
                plat = lat + (i - side // 2) * lat_step * 2 / (side - 1)
                plon = lon + (j - side // 2) * lon_step * 2 / (side - 1)
                lc = self.get_land_cover_at_point(plat, plon)
                results.append(lc)

        if not results:
            return {"composition": {}, "dominant": "Unknown", "bess_score": 50}

        # Calculate composition
        df = pd.DataFrame(results)
        composition = {}
        if "category" in df.columns:
            counts = df["category"].value_counts()
            total = len(df)
            for cat, count in counts.items():
                composition[cat] = round(count / total * 100, 1)

        # Dominant class
        dominant = max(composition, key=composition.get) if composition else "Unknown"

        # Average BESS score
        avg_score = df["bess_score"].mean() if "bess_score" in df.columns else 50

        return {
            "center_lat": lat,
            "center_lon": lon,
            "radius_miles": radius_miles,
            "samples": len(results),
            "composition": composition,
            "dominant_land_cover": dominant,
            "avg_bess_score": round(avg_score, 1),
        }

    def score_land_suitability(
        self,
        lat: float,
        lon: float,
        radius_miles: float = 0.5,
    ) -> Dict:
        """
        Score a location's land use suitability for BESS development.

        Returns:
            Dict with score (0-100), category, and details
        """
        # Single-point query first (fast)
        point_lc = self.get_land_cover_at_point(lat, lon)
        score = point_lc.get("bess_score", 50)
        category = point_lc.get("category", "Unknown")

        # Determine suitability tier
        if score >= 80:
            tier = "Excellent"
        elif score >= 60:
            tier = "Good"
        elif score >= 30:
            tier = "Marginal"
        else:
            tier = "Poor"

        return {
            "lat": lat,
            "lon": lon,
            "land_use_score": score,
            "land_use_tier": tier,
            "nlcd_class": point_lc.get("name", "Unknown"),
            "nlcd_code": point_lc.get("nlcd_code"),
            "category": category,
        }

    def get_land_use_summary(self, sites: list) -> Dict:
        """
        Generate land use summary for multiple candidate sites.

        Args:
            sites: List of dicts with 'lat' and 'lon' keys

        Returns:
            Summary with distribution of land use across sites
        """
        results = []
        for site in sites:
            lat = site.get("lat") or site.get("latitude")
            lon = site.get("lon") or site.get("longitude")
            if lat and lon:
                score = self.score_land_suitability(lat, lon)
                results.append(score)

        if not results:
            return {"total_sites": 0}

        df = pd.DataFrame(results)

        summary = {
            "total_sites": len(results),
            "avg_land_use_score": round(df["land_use_score"].mean(), 1),
        }

        # Distribution by tier
        if "land_use_tier" in df.columns:
            tier_counts = df["land_use_tier"].value_counts().to_dict()
            summary["tier_distribution"] = tier_counts

        # Distribution by category
        if "category" in df.columns:
            cat_counts = df["category"].value_counts().to_dict()
            summary["category_distribution"] = cat_counts

        # NLCD class distribution
        if "nlcd_class" in df.columns:
            class_counts = df["nlcd_class"].value_counts().to_dict()
            summary["nlcd_class_distribution"] = class_counts

        return summary

    @staticmethod
    def get_nlcd_legend() -> Dict:
        """Return the full NLCD classification legend."""
        return NLCD_CLASSES

    @staticmethod
    def get_suitability_guide() -> Dict:
        """Return BESS suitability guide by land cover type."""
        return BESS_SUITABILITY
