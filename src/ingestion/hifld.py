"""
HIFLD Data Ingestion — Substations & Transmission Lines

Pulls transmission infrastructure data from HIFLD Open Data portal
via ArcGIS REST API. Filters to target voltage range and Texas/ERCOT.
"""

import logging
from typing import Optional

import geopandas as gpd
import pandas as pd

from ..utils.api_client import ArcGISClient
from ..utils.geo import geojson_to_geodataframe, WGS84

logger = logging.getLogger(__name__)

# HIFLD ArcGIS Feature Service URLs
# NOTE: HIFLD migrated from services1 (Hp6G80Pky0om6HgQ) to services2 (FiaPA4ga0iQKduv3)
#       in August 2025. Old URLs are dead. Updated Feb 2026.
SUBSTATIONS_URL = (
    "https://services2.arcgis.com/FiaPA4ga0iQKduv3/arcgis/rest/services/"
    "US_Electric_Substations/FeatureServer/0"
)
TRANSMISSION_LINES_URL = (
    "https://services2.arcgis.com/FiaPA4ga0iQKduv3/arcgis/rest/services/"
    "US_Electric_Power_Transmission_Lines/FeatureServer/0"
)

# HIFLD voltage class mapping
VOLTAGE_CLASSES = {
    "UNDER 100": (0, 99),
    "100-161": (100, 161),
    "220-287": (220, 287),
    "345": (345, 345),
    "500": (500, 500),
    "735 AND ABOVE": (735, 999),
    "DC": None,
    "NOT AVAILABLE": None,
}


class HIFLDIngestor:
    """Ingests and filters HIFLD transmission infrastructure data."""

    def __init__(self, config: dict):
        self.config = config
        self.client = ArcGISClient(
            cache_dir=config.get("cache", {}).get("directory", "./data/cache"),
            cache_enabled=config.get("cache", {}).get("enabled", True),
        )
        self.grid_config = config.get("grid", {})
        self.cache_hours = config.get("cache", {}).get("grid_data_expiry_hours", 720)

    def _build_voltage_where_clause(self) -> str:
        """Build SQL WHERE clause for target voltage classes."""
        target_classes = self.grid_config.get("hifld_voltage_classes", ["100-161", "220-287", "345"])
        quoted = [f"'{vc}'" for vc in target_classes]
        return f"VOLT_CLASS IN ({','.join(quoted)})"

    def get_texas_substations(self) -> gpd.GeoDataFrame:
        """
        Fetch all substations in Texas matching target voltage range.

        Returns GeoDataFrame with columns:
            NAME, CITY, STATE, ZIP, TYPE, STATUS, OWNER,
            MAX_VOLT, MIN_VOLT, VOLT_CLASS, LATITUDE, LONGITUDE, geometry
        """
        logger.info("Fetching Texas substations from HIFLD...")

        voltage_clause = self._build_voltage_where_clause()
        where = f"STATE = 'TX' AND STATUS = 'IN SERVICE' AND {voltage_clause}"

        geojson = self.client.query_features(
            service_url=SUBSTATIONS_URL,
            where=where,
            out_fields="*",
            cache_hours=self.cache_hours,
        )

        gdf = geojson_to_geodataframe(geojson)

        if gdf.empty:
            logger.warning("No substations found matching criteria")
            return gdf

        logger.info(f"Found {len(gdf)} Texas substations in target voltage range")

        # Add convenience columns
        if "LATITUDE" in gdf.columns and "LONGITUDE" in gdf.columns:
            gdf["lat"] = pd.to_numeric(gdf["LATITUDE"], errors="coerce")
            gdf["lon"] = pd.to_numeric(gdf["LONGITUDE"], errors="coerce")
        else:
            gdf["lat"] = gdf.geometry.y
            gdf["lon"] = gdf.geometry.x

        # Log voltage class distribution
        if "VOLT_CLASS" in gdf.columns:
            logger.info(f"Voltage class distribution:\n{gdf['VOLT_CLASS'].value_counts().to_string()}")

        return gdf

    def get_texas_transmission_lines(self) -> gpd.GeoDataFrame:
        """
        Fetch all transmission lines in Texas matching target voltage range.

        Returns GeoDataFrame with columns:
            OWNER, VOLTAGE, VOLT_CLASS, SUB_1, SUB_2, STATUS, geometry
        """
        logger.info("Fetching Texas transmission lines from HIFLD...")

        voltage_clause = self._build_voltage_where_clause()

        # Texas bounding box for spatial filter
        # (HIFLD lines don't always have STATE field)
        tx_bbox = {
            "xmin": -106.65,
            "ymin": 25.84,
            "xmax": -93.51,
            "ymax": 36.50,
            "spatialReference": {"wkid": 4326},
        }

        geojson = self.client.query_features(
            service_url=TRANSMISSION_LINES_URL,
            where=voltage_clause,
            out_fields="*",
            geometry=tx_bbox,
            geometry_type="esriGeometryEnvelope",
            cache_hours=self.cache_hours,
        )

        gdf = geojson_to_geodataframe(geojson)

        if gdf.empty:
            logger.warning("No transmission lines found matching criteria")
            return gdf

        logger.info(f"Found {len(gdf)} transmission lines in target voltage range")

        return gdf

    def get_substations_on_lines(
        self,
        substations: gpd.GeoDataFrame,
        transmission_lines: gpd.GeoDataFrame,
        buffer_miles: Optional[float] = None,
    ) -> gpd.GeoDataFrame:
        """
        Filter substations to only those that are connected to
        qualifying transmission lines (within buffer distance).
        """
        if buffer_miles is None:
            buffer_miles = self.grid_config.get("substation_buffer_miles", 2.0)

        logger.info(f"Filtering substations within {buffer_miles} mi of target lines...")

        if substations.empty or transmission_lines.empty:
            return substations

        # Project to meters for accurate buffering
        lines_proj = transmission_lines.to_crs("EPSG:3081")
        subs_proj = substations.to_crs("EPSG:3081")

        # Buffer lines
        buffer_meters = buffer_miles * 1609.34
        lines_buffer = lines_proj.geometry.buffer(buffer_meters).unary_union

        # Filter substations within buffer
        mask = subs_proj.geometry.within(lines_buffer)
        filtered = substations[mask].copy()

        logger.info(f"Filtered to {len(filtered)} substations on qualifying lines")

        return filtered

    def get_all_grid_data(self) -> dict:
        """
        Main entry point — fetch and process all grid data.

        Returns dict with:
            - substations: GeoDataFrame of qualifying substations
            - transmission_lines: GeoDataFrame of qualifying lines
            - substations_on_lines: GeoDataFrame of substations on qualifying lines
        """
        substations = self.get_texas_substations()
        lines = self.get_texas_transmission_lines()

        # Filter to substations actually connected to target lines
        connected_subs = self.get_substations_on_lines(substations, lines)

        return {
            "substations": substations,
            "transmission_lines": lines,
            "substations_on_lines": connected_subs,
        }
