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

# HIFLD ArcGIS Service URLs
# NOTE: HIFLD portal shut down Aug 2025. Transmission lines migrated to services2.
#       Substations NOT on services2 — using Rutgers MARCO mirror (MapServer).
#       Updated Feb 2026.
SUBSTATIONS_URL = (
    "https://oceandata.rad.rutgers.edu/arcgis/rest/services/RenewableEnergy/"
    "HIFLD_Electric_SubstationsTransmissionLines/MapServer/0"
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

        NOTE: The Rutgers MapServer mirror doesn't support WHERE clauses on
        STATE or VOLT_CLASS fields. We use a Texas bounding box as a spatial
        filter, then filter STATE and voltage client-side.

        Returns GeoDataFrame with columns:
            NAME, CITY, STATE, ZIP, TYPE, STATUS, OWNER,
            MAX_VOLT, MIN_VOLT, VOLT_CLASS, LATITUDE, LONGITUDE, geometry
        """
        logger.info("Fetching Texas substations from HIFLD...")

        # Texas bounding box — spatial filter works on MapServer
        tx_bbox = {
            "xmin": -106.65,
            "ymin": 25.84,
            "xmax": -93.51,
            "ymax": 36.50,
            "spatialReference": {"wkid": 4326},
        }

        geojson = self.client.query_features(
            service_url=SUBSTATIONS_URL,
            where="1=1",  # MapServer may not support complex WHERE; filter client-side
            out_fields="*",
            geometry=tx_bbox,
            geometry_type="esriGeometryEnvelope",
            cache_hours=self.cache_hours,
        )

        gdf = geojson_to_geodataframe(geojson)

        if gdf.empty:
            logger.warning("No substations found in Texas bounding box")
            return gdf

        logger.info(f"Fetched {len(gdf)} substations in Texas bounding box")

        # Client-side filter: STATE = TX (in case bbox catches border areas)
        if "STATE" in gdf.columns:
            gdf = gdf[gdf["STATE"] == "TX"].copy()
            logger.info(f"Filtered to {len(gdf)} Texas substations")

        # Client-side filter: STATUS = IN SERVICE
        if "STATUS" in gdf.columns:
            gdf = gdf[gdf["STATUS"] == "IN SERVICE"].copy()
            logger.info(f"Filtered to {len(gdf)} in-service substations")

        # Client-side filter: target voltage classes
        target_classes = set(
            self.grid_config.get("hifld_voltage_classes", ["100-161", "220-287", "345"])
        )
        if "VOLT_CLASS" in gdf.columns:
            gdf = gdf[gdf["VOLT_CLASS"].isin(target_classes)].copy()
            logger.info(f"Filtered to {len(gdf)} substations in target voltage range")
        elif "MAX_VOLT" in gdf.columns:
            # Fallback: filter by numeric MAX_VOLT if VOLT_CLASS not available
            min_kv = self.grid_config.get("min_voltage_kv", 100)
            max_kv = self.grid_config.get("max_voltage_kv", 500)
            gdf["MAX_VOLT"] = pd.to_numeric(gdf["MAX_VOLT"], errors="coerce")
            gdf = gdf[(gdf["MAX_VOLT"] >= min_kv) & (gdf["MAX_VOLT"] <= max_kv)].copy()
            logger.info(f"Filtered to {len(gdf)} substations by MAX_VOLT ({min_kv}-{max_kv}kV)")

        if gdf.empty:
            logger.warning("No substations found matching criteria after filtering")
            return gdf

        logger.info(f"Found {len(gdf)} qualifying Texas substations")

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
