"""
EIA Data Ingestion — Power Plants, Grid Monitor & Wholesale Prices

Queries the U.S. Energy Information Administration (EIA) Open Data API v2
for power plant locations, generation capacity, and wholesale electricity
pricing data relevant to BESS site selection.

API docs: https://www.eia.gov/opendata/documentation.php
Free API key: https://www.eia.gov/opendata/
"""

import logging
from typing import Optional

import geopandas as gpd
from shapely.geometry import Point

from ..utils.api_client import APIClient
from ..utils.geo import haversine_distance, WGS84

logger = logging.getLogger(__name__)

# EIA plant-level data also available via ArcGIS
EIA_POWER_PLANTS_URL = (
    "https://services2.arcgis.com/FiaPA4ga0iQKduv3/arcgis/rest/services/"
    "US_Electric_Power_Plants/FeatureServer/0"
)


class EIAIngestor:
    """Ingests EIA power plant and grid data for BESS siting."""

    def __init__(self, config: dict):
        self.config = config
        self.api_key = config.get("api_keys", {}).get("eia", "")
        self.client = APIClient(
            cache_dir=config.get("cache", {}).get("directory", "./data/cache"),
            cache_enabled=config.get("cache", {}).get("enabled", True),
        )
        self.cache_hours = config.get("cache", {}).get("grid_data_expiry_hours", 720)

    def get_nearby_power_plants(
        self,
        lat: float,
        lon: float,
        radius_miles: float = 10.0,
    ) -> dict:
        """
        Find power plants near a location using the HIFLD ArcGIS service.

        Returns dict with:
            - count: number of plants found
            - plants: list of plant dicts
            - total_capacity_mw: sum of nameplate capacity
            - nearest_distance_mi: distance to closest plant
            - fuel_mix: dict of fuel type -> count
        """
        from ..utils.api_client import ArcGISClient
        from ..utils.geo import geojson_to_geodataframe

        arcgis = ArcGISClient(
            cache_dir=self.config.get("cache", {}).get("directory", "./data/cache"),
            cache_enabled=self.config.get("cache", {}).get("enabled", True),
        )

        try:
            geojson = arcgis.query_point_radius(
                service_url=EIA_POWER_PLANTS_URL,
                lat=lat,
                lon=lon,
                radius_miles=radius_miles,
                out_fields="NAME,NAICS_DESC,SOURCE,SOURCEDATE,LINES,MAX_VOLT,MIN_VOLT,"
                "TOTAL_MW,NET_GEN,TECH_DESC,PRIMSOURCE",
                cache_hours=self.cache_hours,
            )
        except Exception as e:
            logger.warning(f"EIA power plant query failed: {e}")
            return {
                "count": 0,
                "plants": [],
                "total_capacity_mw": 0,
                "nearest_distance_mi": None,
                "fuel_mix": {},
            }

        gdf = geojson_to_geodataframe(geojson)

        if gdf.empty:
            return {
                "count": 0,
                "plants": [],
                "total_capacity_mw": 0,
                "nearest_distance_mi": None,
                "fuel_mix": {},
            }

        # Calculate distances
        distances = gdf.geometry.apply(
            lambda g: haversine_distance(lat, lon, g.centroid.y, g.centroid.x)
        )

        # Build fuel mix
        fuel_col = "PRIMSOURCE" if "PRIMSOURCE" in gdf.columns else "TECH_DESC"
        fuel_mix = {}
        if fuel_col in gdf.columns:
            fuel_mix = gdf[fuel_col].value_counts().to_dict()

        # Total capacity
        total_mw = 0
        if "TOTAL_MW" in gdf.columns:
            total_mw = gdf["TOTAL_MW"].sum()

        plants = []
        for idx, row in gdf.iterrows():
            plants.append(
                {
                    "name": row.get("NAME", "Unknown"),
                    "capacity_mw": row.get("TOTAL_MW", 0),
                    "fuel": row.get(fuel_col, "Unknown"),
                    "distance_mi": round(distances.iloc[idx] if idx < len(distances) else 0, 2),
                }
            )

        return {
            "count": len(gdf),
            "plants": plants[:10],  # top 10 nearest
            "total_capacity_mw": round(total_mw, 1),
            "nearest_distance_mi": round(distances.min(), 2) if len(distances) > 0 else None,
            "fuel_mix": fuel_mix,
        }

    def assess_grid_density(self, lat: float, lon: float) -> dict:
        """
        Assess grid infrastructure density around a location.
        Higher density = better interconnection prospects.

        Returns:
            - grid_density_score: 0-100 (higher = more infrastructure)
            - nearby_plants: count
            - nearby_capacity_mw: total capacity
            - risk_flags: list
        """
        result = self.get_nearby_power_plants(lat, lon, radius_miles=15.0)

        # Score based on nearby generation capacity
        capacity = result["total_capacity_mw"]
        count = result["count"]

        if capacity >= 5000:
            score = 100  # Major generation hub
        elif capacity >= 1000:
            score = 80
        elif capacity >= 500:
            score = 60
        elif capacity >= 100:
            score = 40
        elif count > 0:
            score = 20
        else:
            score = 5  # Remote area — could be good for BESS

        flags = []
        if count == 0:
            flags.append("NOTE: No power plants within 15mi — remote grid area")
        if capacity >= 5000:
            flags.append(f"POSITIVE: Major generation hub ({capacity:.0f} MW within 15mi)")

        return {
            "grid_density_score": score,
            "nearby_plants": count,
            "nearby_capacity_mw": capacity,
            "fuel_mix": result["fuel_mix"],
            "risk_flags": flags,
        }
