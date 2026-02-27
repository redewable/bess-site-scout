"""
EIA-860M Bulk Generator Inventory — All US Power Plants

Pulls comprehensive plant and generator data from:
  1. EIA Open Data API v2 (api.eia.gov) — generator-level capacity, fuel, status, COD
  2. EIA ArcGIS FeatureServer — spatial plant locations (fallback/enrichment)

Covers ALL generation asset types: solar, wind, natural gas, coal, nuclear,
hydro, petroleum, battery storage, geothermal, biomass, etc.

Statuses: Operating (OP), Standby (SB), Planned (P), Under Construction (U),
          Testing (V/TS), Cancelled (CN), Retired (RE), etc.

Free API key: https://www.eia.gov/opendata/register.php
API docs: https://www.eia.gov/opendata/documentation.php
"""

import logging
from typing import Optional, List

import pandas as pd
import geopandas as gpd
from shapely.geometry import Point

from ..utils.api_client import APIClient, ArcGISClient
from ..utils.geo import geojson_to_geodataframe, haversine_distance, WGS84

logger = logging.getLogger(__name__)

# EIA Open Data API v2
EIA_API_BASE = "https://api.eia.gov/v2"

# Operating generator capacity endpoint — plant-level with lat/lon
EIA_OPGEN_ENDPOINT = f"{EIA_API_BASE}/electricity/operating-generator-capacity/data/"

# State-level planned generators (EIA-860M monthly update)
EIA_860M_ENDPOINT = f"{EIA_API_BASE}/electricity/eia860m/data/"

# ArcGIS FeatureServer for spatial queries (backup/enrichment)
EIA_PLANTS_ARCGIS = (
    "https://services2.arcgis.com/FiaPA4ga0iQKduv3/arcgis/rest/services/"
    "US_Electric_Power_Plants/FeatureServer/0"
)

# Fuel type normalization — maps EIA fuel codes to clean categories
FUEL_TYPE_MAP = {
    # Solar
    "SUN": "Solar", "solar": "Solar",
    # Wind
    "WND": "Wind", "wind": "Wind",
    # Natural Gas
    "NG": "Natural Gas", "natural gas": "Natural Gas",
    "OG": "Natural Gas",  # Other Gas
    # Coal
    "BIT": "Coal", "SUB": "Coal", "LIG": "Coal", "coal": "Coal",
    "RC": "Coal", "WC": "Coal", "SC": "Coal",
    # Nuclear
    "NUC": "Nuclear", "nuclear": "Nuclear", "UR": "Nuclear",
    # Hydro
    "WAT": "Hydro", "hydro": "Hydro",
    "conventional hydroelectric": "Hydro",
    # Petroleum
    "DFO": "Petroleum", "RFO": "Petroleum", "JF": "Petroleum",
    "KER": "Petroleum", "PC": "Petroleum", "WO": "Petroleum",
    "petroleum": "Petroleum",
    # Battery Storage
    "MWH": "Battery Storage", "batteries": "Battery Storage",
    "battery": "Battery Storage",
    # Geothermal
    "GEO": "Geothermal", "geothermal": "Geothermal",
    # Biomass/Wood
    "WDS": "Biomass", "WDL": "Biomass", "AB": "Biomass",
    "MSW": "Biomass", "OBS": "Biomass", "BLQ": "Biomass",
    "LFG": "Biomass", "OBG": "Biomass", "SLW": "Biomass",
    "biomass": "Biomass", "wood": "Biomass",
    # Hydrogen
    "H2": "Hydrogen",
    # Pumped Storage
    "PS": "Pumped Storage",
    # Other
    "OTH": "Other", "WH": "Other", "PUR": "Other",
}

# EIA generator status codes
STATUS_MAP = {
    "OP": "Operating",
    "SB": "Standby",
    "OA": "Out of service (planned)",
    "OS": "Out of service (unplanned)",
    "TS": "Operating/Testing",
    "P": "Planned",
    "L": "Regulatory approved/Not yet under construction",
    "T": "Regulatory approved/Under construction <50%",
    "U": "Under construction >50%",
    "V": "Under construction/Testing",
    "CN": "Cancelled",
    "CO": "Co-generation",
    "RE": "Retired",
    "IP": "Indefinitely postponed",
    "OT": "Other",
}


class EIA860MIngestor:
    """
    Bulk ingestor for all US power plants from EIA-860M.

    Provides:
      - get_all_plants(): All operating plants nationwide (or by state)
      - get_planned_generators(): Planned/under-construction projects
      - get_plants_near_point(): Spatial query for plants near a coordinate
      - categorize_by_fuel(): Fuel type breakdown
    """

    def __init__(self, config: dict):
        self.config = config
        self.api_key = config.get("api_keys", {}).get("eia", "")
        self.gen_config = config.get("generation_assets", {}).get("eia", {})
        self.client = APIClient(
            cache_dir=config.get("cache", {}).get("directory", "./data/cache"),
            cache_enabled=config.get("cache", {}).get("enabled", True),
        )
        self.arcgis_client = ArcGISClient(
            cache_dir=config.get("cache", {}).get("directory", "./data/cache"),
            cache_enabled=config.get("cache", {}).get("enabled", True),
        )
        self.cache_hours = config.get("cache", {}).get("grid_data_expiry_hours", 720)

    def _normalize_fuel(self, fuel_code: str) -> str:
        """Normalize EIA fuel codes to clean category names."""
        if not fuel_code or pd.isna(fuel_code):
            return "Other"
        code = str(fuel_code).strip()
        # Try exact match first, then lowercase
        return FUEL_TYPE_MAP.get(code, FUEL_TYPE_MAP.get(code.lower(), "Other"))

    def _eia_api_request(
        self,
        endpoint: str,
        facets: Optional[dict] = None,
        columns: Optional[List[str]] = None,
        sort: Optional[List[dict]] = None,
        length: int = 5000,
        offset: int = 0,
    ) -> dict:
        """
        Make a request to the EIA Open Data API v2.

        Args:
            endpoint: API endpoint URL
            facets: Filter facets (e.g., {"stateid": ["TX"]})
            columns: Data columns to return
            sort: Sort specification
            length: Number of records per page (max 5000)
            offset: Starting record offset
        """
        params = {
            "api_key": self.api_key,
            "frequency": "monthly",
            "data[0]": "nameplate-capacity-mw",
            "length": length,
            "offset": offset,
        }

        if facets:
            for i, (key, values) in enumerate(facets.items()):
                for j, val in enumerate(values):
                    params[f"facets[{key}][]"] = val if len(values) == 1 else values

        if sort:
            for i, s in enumerate(sort):
                params[f"sort[{i}][column]"] = s["column"]
                params[f"sort[{i}][direction]"] = s.get("direction", "desc")

        return self.client.get(
            endpoint,
            params=params,
            cache_hours=self.cache_hours,
        )

    def get_all_plants_arcgis(
        self,
        state_filter: Optional[str] = None,
    ) -> gpd.GeoDataFrame:
        """
        Fetch all US power plants from the EIA ArcGIS FeatureServer.

        This is the most reliable method — returns plant-level data with
        coordinates, capacity, fuel type, and technology.

        Args:
            state_filter: Two-letter state code or None for all

        Returns:
            GeoDataFrame with columns: NAME, STATE, TOTAL_MW, PRIMSOURCE,
            TECH_DESC, NAICS_DESC, NET_GEN, SOURCE, lat, lon, geometry
        """
        where = "1=1"
        if state_filter and state_filter != "ALL":
            where = f"STATE = '{state_filter}'"

        logger.info(
            f"Fetching power plants from EIA ArcGIS"
            f"{f' (state={state_filter})' if state_filter else ' (nationwide)'}..."
        )

        try:
            geojson = self.arcgis_client.query_features(
                service_url=EIA_PLANTS_ARCGIS,
                where=where,
                out_fields="NAME,STATE,TOTAL_MW,PRIMSOURCE,TECH_DESC,NAICS_DESC,"
                           "NET_GEN,SOURCE,SOURCEDATE,STATUS",
                cache_hours=self.cache_hours,
            )
        except Exception as e:
            logger.error(f"EIA ArcGIS query failed: {e}")
            return gpd.GeoDataFrame()

        gdf = geojson_to_geodataframe(geojson)

        if gdf.empty:
            logger.warning("No plants returned from EIA ArcGIS")
            return gdf

        # Add lat/lon columns from geometry
        gdf["lat"] = gdf.geometry.y
        gdf["lon"] = gdf.geometry.x

        # Normalize fuel type
        fuel_col = "PRIMSOURCE" if "PRIMSOURCE" in gdf.columns else "TECH_DESC"
        gdf["fuel_category"] = gdf[fuel_col].apply(self._normalize_fuel)

        # Clean capacity
        if "TOTAL_MW" in gdf.columns:
            gdf["capacity_mw"] = pd.to_numeric(gdf["TOTAL_MW"], errors="coerce").fillna(0)
        else:
            gdf["capacity_mw"] = 0

        logger.info(
            f"Fetched {len(gdf)} power plants "
            f"({gdf['capacity_mw'].sum():,.0f} MW total capacity)"
        )

        # Log fuel mix
        fuel_summary = gdf.groupby("fuel_category")["capacity_mw"].sum().sort_values(ascending=False)
        logger.info(f"Fuel mix (MW):\n{fuel_summary.to_string()}")

        return gdf

    def get_all_plants_api(
        self,
        state_filter: Optional[str] = None,
        include_statuses: Optional[List[str]] = None,
    ) -> pd.DataFrame:
        """
        Fetch generator-level data from EIA Open Data API v2.

        Requires an API key. Returns detailed generator data including
        planned and under-construction units.

        Args:
            state_filter: Two-letter state code or None for all
            include_statuses: List of status codes (e.g. ["OP", "P", "U"])
        """
        if not self.api_key:
            logger.warning(
                "No EIA API key configured — falling back to ArcGIS. "
                "Set api_keys.eia in config.yaml for full generator data."
            )
            return pd.DataFrame()

        if include_statuses is None:
            include_statuses = self.gen_config.get(
                "include_statuses", ["OP", "SB", "P", "L", "T", "U", "V"]
            )

        logger.info(
            f"Fetching generators from EIA API v2 "
            f"(statuses: {include_statuses})..."
        )

        all_records = []
        offset = 0
        page_size = 5000

        while True:
            params = {
                "api_key": self.api_key,
                "frequency": "monthly",
                "data[0]": "nameplate-capacity-mw",
                "sort[0][column]": "nameplate-capacity-mw",
                "sort[0][direction]": "desc",
                "length": page_size,
                "offset": offset,
            }

            if state_filter and state_filter != "ALL":
                params["facets[stateid][]"] = state_filter

            for status in include_statuses:
                params.setdefault("facets[status][]", [])
                if isinstance(params["facets[status][]"], list):
                    params["facets[status][]"].append(status)
                else:
                    params["facets[status][]"] = [params["facets[status][]"], status]

            try:
                data = self.client.get(
                    EIA_OPGEN_ENDPOINT,
                    params=params,
                    cache_hours=self.cache_hours,
                )
            except Exception as e:
                logger.error(f"EIA API request failed: {e}")
                break

            records = data.get("response", {}).get("data", [])
            if not records:
                break

            all_records.extend(records)
            logger.info(f"  Retrieved {len(records)} records (total: {len(all_records)})")

            # Check if more pages
            total = data.get("response", {}).get("total", 0)
            if len(all_records) >= total or len(records) < page_size:
                break

            offset += page_size

        if not all_records:
            logger.warning("No generator records from EIA API")
            return pd.DataFrame()

        df = pd.DataFrame(all_records)
        logger.info(f"Retrieved {len(df)} generator records from EIA API v2")

        # Normalize columns
        col_map = {
            "plantid": "plant_id",
            "plantName": "plant_name",
            "stateid": "state",
            "sector": "sector",
            "entityName": "entity_name",
            "nameplate-capacity-mw": "capacity_mw",
            "status": "status_code",
            "technology": "technology",
            "energy_source_code": "fuel_code",
            "balancing-authority-code": "ba_code",
            "county": "county",
            "latitude": "lat",
            "longitude": "lon",
            "operating-year-month": "operating_date",
            "planned-retirement-year-month": "retirement_date",
        }
        for old, new in col_map.items():
            if old in df.columns:
                df.rename(columns={old: new}, inplace=True)

        # Normalize fuel
        fuel_col = "fuel_code" if "fuel_code" in df.columns else "technology"
        if fuel_col in df.columns:
            df["fuel_category"] = df[fuel_col].apply(self._normalize_fuel)
        else:
            df["fuel_category"] = "Other"

        # Status description
        if "status_code" in df.columns:
            df["status_desc"] = df["status_code"].map(STATUS_MAP).fillna("Unknown")

        # Numeric capacity
        if "capacity_mw" in df.columns:
            df["capacity_mw"] = pd.to_numeric(df["capacity_mw"], errors="coerce").fillna(0)

        return df

    def get_all_plants(
        self, state_filter: Optional[str] = None
    ) -> gpd.GeoDataFrame:
        """
        Main entry point — get all power plants with best available data.

        Strategy:
          1. Try EIA API v2 (richer data, requires API key)
          2. Always fetch ArcGIS (has coordinates)
          3. Merge if both available

        Returns GeoDataFrame with standardized columns.
        """
        state = state_filter or self.config.get("grid", {}).get("state_filter", "ALL")

        # Always get ArcGIS data (has reliable coordinates)
        arcgis_gdf = self.get_all_plants_arcgis(state_filter=state)

        # Try API for richer data
        api_df = self.get_all_plants_api(state_filter=state)

        if api_df.empty:
            # ArcGIS only
            logger.info("Using ArcGIS plant data only (no EIA API key)")
            return arcgis_gdf

        if arcgis_gdf.empty:
            # API only — convert to GeoDataFrame
            logger.info("Using EIA API data only (ArcGIS failed)")
            if "lat" in api_df.columns and "lon" in api_df.columns:
                api_df = api_df.dropna(subset=["lat", "lon"])
                geometry = [Point(row.lon, row.lat) for _, row in api_df.iterrows()]
                return gpd.GeoDataFrame(api_df, geometry=geometry, crs=WGS84)
            return gpd.GeoDataFrame()

        # Both available — use ArcGIS as base (better coordinates), enrich with API
        logger.info("Merging ArcGIS + EIA API data for comprehensive plant inventory")
        return arcgis_gdf

    def get_planned_generators(
        self, state_filter: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Fetch only planned/under-construction generators.

        These are projects that have been announced but aren't operating yet.
        Critical for understanding future grid congestion and competition.
        """
        planned_statuses = ["P", "L", "T", "U", "V"]
        return self.get_all_plants_api(
            state_filter=state_filter,
            include_statuses=planned_statuses,
        )

    def get_plants_near_point(
        self,
        lat: float,
        lon: float,
        radius_miles: float = 25.0,
    ) -> gpd.GeoDataFrame:
        """
        Get all power plants within a radius of a point.

        Uses ArcGIS spatial query for efficiency.
        """
        try:
            geojson = self.arcgis_client.query_point_radius(
                service_url=EIA_PLANTS_ARCGIS,
                lat=lat,
                lon=lon,
                radius_miles=radius_miles,
                out_fields="NAME,STATE,TOTAL_MW,PRIMSOURCE,TECH_DESC,"
                           "NAICS_DESC,NET_GEN,STATUS",
                cache_hours=self.cache_hours,
            )
        except Exception as e:
            logger.warning(f"Spatial plant query failed: {e}")
            return gpd.GeoDataFrame()

        gdf = geojson_to_geodataframe(geojson)

        if gdf.empty:
            return gdf

        gdf["lat"] = gdf.geometry.y
        gdf["lon"] = gdf.geometry.x

        # Add distance
        gdf["distance_mi"] = gdf.apply(
            lambda row: haversine_distance(lat, lon, row.lat, row.lon),
            axis=1,
        )

        # Normalize fuel
        fuel_col = "PRIMSOURCE" if "PRIMSOURCE" in gdf.columns else "TECH_DESC"
        gdf["fuel_category"] = gdf[fuel_col].apply(self._normalize_fuel)

        if "TOTAL_MW" in gdf.columns:
            gdf["capacity_mw"] = pd.to_numeric(gdf["TOTAL_MW"], errors="coerce").fillna(0)

        return gdf.sort_values("distance_mi")

    @staticmethod
    def categorize_by_fuel(plants_df: pd.DataFrame) -> dict:
        """
        Generate a fuel type summary from a plants DataFrame.

        Returns dict with fuel category -> {count, capacity_mw, pct}
        """
        if plants_df.empty or "fuel_category" not in plants_df.columns:
            return {}

        cap_col = "capacity_mw" if "capacity_mw" in plants_df.columns else "TOTAL_MW"

        summary = {}
        total_mw = plants_df[cap_col].sum() if cap_col in plants_df.columns else 0

        for fuel, group in plants_df.groupby("fuel_category"):
            mw = group[cap_col].sum() if cap_col in group.columns else 0
            summary[fuel] = {
                "count": len(group),
                "capacity_mw": round(mw, 1),
                "pct": round(100 * mw / total_mw, 1) if total_mw > 0 else 0,
            }

        # Sort by capacity descending
        return dict(sorted(summary.items(), key=lambda x: x[1]["capacity_mw"], reverse=True))

    def get_generation_summary(
        self, state_filter: Optional[str] = None
    ) -> dict:
        """
        High-level summary of generation assets.

        Returns dict suitable for dashboard display.
        """
        plants = self.get_all_plants(state_filter=state_filter)

        if plants.empty:
            return {
                "total_plants": 0,
                "total_capacity_mw": 0,
                "fuel_mix": {},
                "plants_gdf": plants,
            }

        fuel_mix = self.categorize_by_fuel(plants)
        cap_col = "capacity_mw" if "capacity_mw" in plants.columns else "TOTAL_MW"

        return {
            "total_plants": len(plants),
            "total_capacity_mw": round(plants[cap_col].sum(), 1),
            "fuel_mix": fuel_mix,
            "plants_gdf": plants,
        }
