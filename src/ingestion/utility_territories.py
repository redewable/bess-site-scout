"""
Utility Service Territory Ingestion

Pulls utility service area boundaries from:
  1. HIFLD (DHS) — Electric Retail Service Territories via ArcGIS
  2. EIA-861 — Utility data with service area boundaries
  3. US Energy Atlas — EIA utility territory layer

Knowing which utility serves a location matters for:
  - Interconnection process (each utility has different rules/timelines)
  - PPA offtake opportunities (utility procurement needs)
  - Regulatory environment (state PUC jurisdiction)
  - Net metering / DER policies
  - Rate structures affecting BESS economics
"""

import logging
from typing import Optional, Dict, List
from pathlib import Path

import pandas as pd
import geopandas as gpd
from shapely.geometry import Point

from ..utils.api_client import APIClient, ArcGISClient
from ..utils.geo import WGS84

logger = logging.getLogger(__name__)

# ── HIFLD Electric Retail Service Territories ─────────────────────
# Free, no auth, nationwide coverage
HIFLD_SERVICE_TERRITORIES = (
    "https://services1.arcgis.com/Hp6G80Pky0om7QvQ/arcgis/rest/services/"
    "Electric_Retail_Service_Territories_2/FeatureServer/0"
)

# Alternative: NASA NCCS HIFLD mirror
HIFLD_SERVICE_TERRITORIES_ALT = (
    "https://maps.nccs.nasa.gov/mapping/rest/services/"
    "hifld_open/energy/FeatureServer/26"
)

# ── EIA Utility Data ─────────────────────────────────────────────
EIA_API_BASE = "https://api.eia.gov/v2"
EIA_UTILITY_ENDPOINT = f"{EIA_API_BASE}/electricity/state-electricity-profiles/data/"

# ── Utility ownership types ──────────────────────────────────────
OWNERSHIP_TYPES = {
    "IOU": "Investor-Owned Utility",
    "COOP": "Cooperative",
    "MUNI": "Municipal",
    "STATE": "State/Political Subdivision",
    "FED": "Federal (TVA, BPA, WAPA)",
    "POU": "Publicly-Owned Utility",
}

# ── Major IOUs by state (partial — most relevant for BESS) ───────
MAJOR_IOUS = {
    "TX": ["Oncor", "CenterPoint", "AEP Texas", "TNMP"],
    "CA": ["PG&E", "SCE", "SDG&E"],
    "FL": ["FPL", "Duke Energy Florida", "Tampa Electric"],
    "NY": ["ConEdison", "National Grid", "NYSEG", "Central Hudson"],
    "PA": ["PECO", "PPL", "Duquesne Light"],
    "OH": ["AEP Ohio", "FirstEnergy Ohio", "Duke Energy Ohio"],
    "IL": ["ComEd", "Ameren Illinois"],
    "GA": ["Georgia Power"],
    "NC": ["Duke Energy Carolinas", "Duke Energy Progress"],
    "VA": ["Dominion Energy"],
    "AZ": ["APS", "TEP", "SRP"],
    "NV": ["NV Energy"],
    "CO": ["Xcel Energy"],
    "MN": ["Xcel Energy", "Minnesota Power"],
    "WI": ["WEC Energy", "Alliant Energy"],
    "MI": ["DTE Energy", "Consumers Energy"],
    "NJ": ["PSE&G", "JCP&L", "ACE"],
    "MA": ["Eversource", "National Grid"],
    "CT": ["Eversource", "UI"],
    "IN": ["Duke Energy Indiana", "AES Indiana"],
}


class UtilityTerritoryIngestor:
    """
    Ingests utility service territory data.

    Provides:
      - get_utility_at_point(): Identify serving utility for a coordinate
      - get_utilities_in_area(): All utilities within a bounding box
      - get_utility_details(): EIA data for a specific utility
      - get_utility_summary(): Summary for pipeline output
    """

    def __init__(self, config: dict):
        self.config = config
        self.api_keys = config.get("api_keys", {})
        self.arcgis = ArcGISClient(
            cache_dir=config.get("cache", {}).get("directory", "./data/cache"),
            cache_enabled=config.get("cache", {}).get("enabled", True),
        )
        self.cache_hours = config.get("cache", {}).get("grid_data_expiry_hours", 720)
        self._territory_cache = {}

    def get_utility_at_point(
        self,
        lat: float,
        lon: float,
    ) -> Dict:
        """
        Identify which utility serves a specific coordinate.

        Uses HIFLD Electric Retail Service Territories layer.
        Returns utility name, ID, ownership type, state, and contact info.
        """
        try:
            # Query HIFLD service territory layer at point
            geojson = self.arcgis.query_features(
                service_url=HIFLD_SERVICE_TERRITORIES,
                where="1=1",
                out_fields="*",
                geometry=f"{lon},{lat}",
                geometry_type="esriGeometryPoint",
                spatial_rel="esriSpatialRelIntersects",
                return_geometry=False,
                cache_hours=self.cache_hours,
            )

            features = geojson.get("features", [])
            if not features:
                # Try alternate endpoint
                geojson = self.arcgis.query_features(
                    service_url=HIFLD_SERVICE_TERRITORIES_ALT,
                    where="1=1",
                    out_fields="*",
                    geometry=f"{lon},{lat}",
                    geometry_type="esriGeometryPoint",
                    spatial_rel="esriSpatialRelIntersects",
                    return_geometry=False,
                    cache_hours=self.cache_hours,
                )
                features = geojson.get("features", [])

            if not features:
                return {
                    "lat": lat,
                    "lon": lon,
                    "utility_name": "Unknown",
                    "utility_id": None,
                    "ownership_type": "Unknown",
                }

            # Parse first matching territory
            props = features[0].get("properties", {})

            # HIFLD uses various field names
            name_fields = ["NAME", "COMP_NAME", "Company_Na", "UTILITY_NA"]
            id_fields = ["ID", "OBJECTID", "UTILITY_ID", "EIA_ID"]
            type_fields = ["TYPE", "OWNERSHIP", "COMP_TYPE"]
            state_fields = ["STATE", "STATEFP"]

            utility_name = ""
            for f in name_fields:
                if props.get(f):
                    utility_name = props[f]
                    break

            utility_id = None
            for f in id_fields:
                if props.get(f):
                    utility_id = props[f]
                    break

            ownership = ""
            for f in type_fields:
                if props.get(f):
                    ownership = props[f]
                    break

            state = ""
            for f in state_fields:
                if props.get(f):
                    state = props[f]
                    break

            result = {
                "lat": lat,
                "lon": lon,
                "utility_name": utility_name,
                "utility_id": utility_id,
                "ownership_type": ownership,
                "ownership_description": OWNERSHIP_TYPES.get(ownership, ownership),
                "state": state,
            }

            # Add all available properties
            for key, val in props.items():
                if key not in result and val is not None:
                    result[f"hifld_{key.lower()}"] = val

            return result

        except Exception as e:
            logger.warning(f"  Utility territory query failed at ({lat}, {lon}): {e}")
            return {
                "lat": lat,
                "lon": lon,
                "utility_name": "Unknown",
                "error": str(e),
            }

    def get_utilities_in_area(
        self,
        lat: float,
        lon: float,
        radius_miles: float = 25.0,
    ) -> gpd.GeoDataFrame:
        """
        Get all utility service territories within a radius.
        Returns GeoDataFrame with territory boundaries.
        """
        try:
            geojson = self.arcgis.query_point_radius(
                service_url=HIFLD_SERVICE_TERRITORIES,
                lat=lat,
                lon=lon,
                radius_miles=radius_miles,
                out_fields="*",
                cache_hours=self.cache_hours,
            )

            features = geojson.get("features", [])
            if not features:
                return gpd.GeoDataFrame()

            gdf = gpd.GeoDataFrame.from_features(features, crs=WGS84)
            logger.info(
                f"  HIFLD: {len(gdf)} utility territories near ({lat:.4f}, {lon:.4f})"
            )
            return gdf

        except Exception as e:
            logger.warning(f"  Utility area query failed: {e}")
            return gpd.GeoDataFrame()

    def get_utility_details(
        self,
        utility_name: str,
        state: Optional[str] = None,
    ) -> Dict:
        """
        Get EIA details for a specific utility.
        Uses EIA Open Data API for customer counts, sales, revenue.
        """
        eia_key = self.api_keys.get("eia", "")
        if not eia_key:
            return {"utility_name": utility_name, "note": "No EIA API key"}

        try:
            params = {
                "api_key": eia_key,
                "data[0]": "customers",
                "data[1]": "sales",
                "data[2]": "revenue",
                "frequency": "annual",
                "sort[0][column]": "period",
                "sort[0][direction]": "desc",
                "length": 10,
            }

            if state:
                params["facets[stateid][]"] = state

            data = self.arcgis.get(
                EIA_UTILITY_ENDPOINT,
                params=params,
                cache_hours=self.cache_hours,
            )

            records = data.get("response", {}).get("data", [])
            if records:
                return {
                    "utility_name": utility_name,
                    "eia_data": records[:5],
                }

        except Exception as e:
            logger.debug(f"  EIA utility detail fetch failed: {e}")

        return {"utility_name": utility_name}

    def classify_interconnection_process(self, utility_info: Dict) -> Dict:
        """
        Classify the expected interconnection process based on utility type.

        IOUs in organized markets (ISO/RTO) → ISO queue process
        Munis/Coops → Bilateral, often faster but smaller
        Federal (TVA, BPA) → Unique process per entity
        """
        ownership = utility_info.get("ownership_type", "").upper()
        utility = utility_info.get("utility_name", "")

        if ownership in ("IOU", "INVESTOR-OWNED"):
            return {
                "process_type": "ISO/RTO Queue",
                "complexity": "High",
                "typical_timeline_years": "3-5",
                "note": "Standard ISO interconnection study process",
            }
        elif ownership in ("COOP", "COOPERATIVE"):
            return {
                "process_type": "Bilateral/Direct",
                "complexity": "Moderate",
                "typical_timeline_years": "1-3",
                "note": "May have simpler process but smaller capacity",
            }
        elif ownership in ("MUNI", "MUNICIPAL"):
            return {
                "process_type": "Municipal Process",
                "complexity": "Low-Moderate",
                "typical_timeline_years": "1-2",
                "note": "City/county approval; may have political considerations",
            }
        elif ownership in ("FED", "FEDERAL"):
            return {
                "process_type": "Federal Entity",
                "complexity": "High",
                "typical_timeline_years": "2-5",
                "note": "TVA/BPA/WAPA have unique processes",
            }
        else:
            return {
                "process_type": "Unknown",
                "complexity": "Unknown",
                "typical_timeline_years": "2-4",
            }

    def get_utility_summary(self, sites: list) -> Dict:
        """
        Generate utility territory summary for multiple candidate sites.

        Args:
            sites: List of dicts with 'lat' and 'lon' keys
        """
        results = []
        for site in sites:
            lat = site.get("lat") or site.get("latitude")
            lon = site.get("lon") or site.get("longitude")
            if lat and lon:
                info = self.get_utility_at_point(lat, lon)
                info["interconnection"] = self.classify_interconnection_process(info)
                results.append(info)

        if not results:
            return {"total_sites": 0}

        df = pd.DataFrame(results)
        summary = {
            "total_sites": len(results),
        }

        if "utility_name" in df.columns:
            utility_counts = df["utility_name"].value_counts().to_dict()
            summary["utilities_serving_sites"] = utility_counts
            summary["unique_utilities"] = len(utility_counts)

        if "ownership_type" in df.columns:
            ownership_counts = df["ownership_type"].value_counts().to_dict()
            summary["ownership_distribution"] = ownership_counts

        if "state" in df.columns:
            state_counts = df["state"].value_counts().to_dict()
            summary["state_distribution"] = state_counts

        return summary
