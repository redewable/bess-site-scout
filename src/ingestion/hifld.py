"""
HIFLD Data Ingestion — Substations & Transmission Lines

Pulls transmission infrastructure data from HIFLD Open Data portal
via ArcGIS REST API. Supports nationwide or state-filtered queries.
Substations are derived from transmission line endpoints (SUB_1/SUB_2).
"""

import logging
from typing import Optional

import geopandas as gpd
import pandas as pd
from shapely.geometry import Point

from ..utils.api_client import ArcGISClient
from ..utils.geo import geojson_to_geodataframe, WGS84

logger = logging.getLogger(__name__)

# HIFLD ArcGIS Service URLs
# NOTE: HIFLD portal shut down Aug 2025. Transmission lines on services2.
#       Substations derived from transmission line endpoints (SUB_1/SUB_2).
#       Updated Feb 2026.
TRANSMISSION_LINES_URL = (
    "https://services2.arcgis.com/FiaPA4ga0iQKduv3/arcgis/rest/services/"
    "US_Electric_Power_Transmission_Lines/FeatureServer/0"
)

# HIFLD Substations dataset — provides owner, operator, status, city, state, zip
# Fields: NAME, CITY, STATE, ZIP, TYPE, STATUS, COUNTY, COUNTYFIPS, COUNTRY,
#         NAICS_CODE, NAICS_DESC, SOURCE, SOURCEDATE, VAL_METHOD, VAL_DATE,
#         LINES, MAX_VOLT, MIN_VOLT, MAX_INFER, MIN_INFER, OWNER, OPERATOR
SUBSTATIONS_URL = (
    "https://services1.arcgis.com/Hp6G80Pky0om6HgQ/arcgis/rest/services/"
    "Substations/FeatureServer/0"
)

# State bounding boxes (lon_min, lat_min, lon_max, lat_max)
STATE_BBOXES = {
    "TX": (-106.65, 25.84, -93.51, 36.50),
    "CA": (-124.48, 32.53, -114.13, 42.01),
    "AZ": (-114.82, 31.33, -109.04, 37.00),
    "NV": (-120.01, 35.00, -114.04, 42.00),
    "NM": (-109.05, 31.33, -103.00, 37.00),
    "FL": (-87.63, 24.52, -80.03, 31.00),
    "NY": (-79.76, 40.50, -71.86, 45.02),
    "IL": (-91.51, 36.97, -87.02, 42.51),
    "PA": (-80.52, 39.72, -74.69, 42.27),
    "OH": (-84.82, 38.40, -80.52, 42.32),
    "GA": (-85.61, 30.36, -80.84, 35.00),
    "NC": (-84.32, 33.84, -75.46, 36.59),
    "VA": (-83.68, 36.54, -75.24, 39.47),
    "MI": (-90.42, 41.70, -82.12, 48.31),
    "IN": (-88.10, 37.77, -84.78, 41.76),
    "CO": (-109.06, 36.99, -102.04, 41.00),
    # CONUS = no spatial filter, just voltage
}

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
        target_classes = self.grid_config.get(
            "hifld_voltage_classes", ["100-161", "220-287", "345"]
        )
        quoted = [f"'{vc}'" for vc in target_classes]
        return f"VOLT_CLASS IN ({','.join(quoted)})"

    def get_transmission_lines(self) -> gpd.GeoDataFrame:
        """
        Fetch transmission lines matching target voltage range.
        Supports nationwide (ALL) or state-specific queries via config.

        Returns GeoDataFrame with columns:
            OWNER, VOLTAGE, VOLT_CLASS, SUB_1, SUB_2, STATUS, geometry
        """
        state_filter = self.grid_config.get("state_filter", "ALL")
        voltage_clause = self._build_voltage_where_clause()

        if state_filter == "ALL":
            logger.info("Fetching ALL US transmission lines from HIFLD (nationwide)...")
            geojson = self.client.query_features(
                service_url=TRANSMISSION_LINES_URL,
                where=voltage_clause,
                out_fields="*",
                cache_hours=self.cache_hours,
            )
        else:
            logger.info(f"Fetching {state_filter} transmission lines from HIFLD...")
            bbox_tuple = STATE_BBOXES.get(state_filter)
            if bbox_tuple:
                bbox = {
                    "xmin": bbox_tuple[0],
                    "ymin": bbox_tuple[1],
                    "xmax": bbox_tuple[2],
                    "ymax": bbox_tuple[3],
                    "spatialReference": {"wkid": 4326},
                }
                geojson = self.client.query_features(
                    service_url=TRANSMISSION_LINES_URL,
                    where=voltage_clause,
                    out_fields="*",
                    geometry=bbox,
                    geometry_type="esriGeometryEnvelope",
                    cache_hours=self.cache_hours,
                )
            else:
                # Unknown state — fetch all and log warning
                logger.warning(
                    f"No bbox defined for state '{state_filter}', fetching nationwide"
                )
                geojson = self.client.query_features(
                    service_url=TRANSMISSION_LINES_URL,
                    where=voltage_clause,
                    out_fields="*",
                    cache_hours=self.cache_hours,
                )

        gdf = geojson_to_geodataframe(geojson)

        if gdf.empty:
            logger.warning("No transmission lines found matching criteria")
            return gdf

        logger.info(f"Found {len(gdf)} transmission lines in target voltage range")
        return gdf

    # Keep legacy method name for backwards compat
    def get_texas_transmission_lines(self) -> gpd.GeoDataFrame:
        return self.get_transmission_lines()

    def derive_substations_from_lines(
        self, transmission_lines: gpd.GeoDataFrame
    ) -> gpd.GeoDataFrame:
        """
        Derive substation locations from transmission line endpoints.

        Each transmission line has SUB_1 (start) and SUB_2 (end) fields naming
        the connected substations. We extract unique substations with their
        coordinates from the line geometry endpoints and the highest voltage
        class of any connected line.

        Returns GeoDataFrame with columns:
            NAME, VOLT_CLASS, max_voltage_kv, connected_lines, lat, lon, geometry
        """
        logger.info("Deriving substations from transmission line endpoints...")

        if transmission_lines.empty:
            return gpd.GeoDataFrame()

        # Voltage class to numeric kV for comparison
        vc_to_kv = {
            "UNDER 100": 69,
            "100-161": 138,
            "220-287": 230,
            "345": 345,
            "500": 500,
            "735 AND ABOVE": 765,
        }

        # Build a dict of unique substations, tracking highest voltage
        subs_dict = {}  # name -> {lat, lon, max_kv, volt_class, count}

        for _, line in transmission_lines.iterrows():
            volt_class = str(line.get("VOLT_CLASS", ""))
            line_kv = vc_to_kv.get(volt_class, 0)
            geom = line.geometry

            if geom is None:
                continue

            # Handle both LineString and MultiLineString geometries
            try:
                if geom.geom_type == "MultiLineString":
                    lines_list = list(geom.geoms)
                    start_coord = list(lines_list[0].coords)[0]
                    end_coord = list(lines_list[-1].coords)[-1]
                elif geom.geom_type == "LineString":
                    coords = list(geom.coords)
                    if not coords:
                        continue
                    start_coord = coords[0]
                    end_coord = coords[-1]
                else:
                    continue
            except (IndexError, StopIteration):
                continue

            endpoints = [
                ("SUB_1", start_coord),
                ("SUB_2", end_coord),
            ]

            for sub_field, (lon, lat, *_) in endpoints:
                name = line.get(sub_field, "")
                if (
                    not name
                    or pd.isna(name)
                    or str(name).strip() in ("NOT AVAILABLE", "NONE", "")
                ):
                    continue

                name = str(name).strip()

                if name not in subs_dict:
                    subs_dict[name] = {
                        "lat": lat,
                        "lon": lon,
                        "max_kv": line_kv,
                        "volt_class": volt_class,
                        "connected_lines": 1,
                    }
                else:
                    existing = subs_dict[name]
                    existing["connected_lines"] += 1
                    if line_kv > existing["max_kv"]:
                        existing["max_kv"] = line_kv
                        existing["volt_class"] = volt_class

        if not subs_dict:
            logger.warning("Could not extract any substations from transmission lines")
            return gpd.GeoDataFrame()

        # Convert to GeoDataFrame
        rows = []
        for name, data in subs_dict.items():
            rows.append(
                {
                    "NAME": name,
                    "VOLT_CLASS": data["volt_class"],
                    "max_voltage_kv": data["max_kv"],
                    "connected_lines": data["connected_lines"],
                    "lat": data["lat"],
                    "lon": data["lon"],
                    "geometry": Point(data["lon"], data["lat"]),
                }
            )

        gdf = gpd.GeoDataFrame(rows, crs=WGS84)

        # Filter to CONUS (exclude Hawaii, Alaska, territories, offshore)
        conus_mask = (
            (gdf["lat"] >= 24.0)
            & (gdf["lat"] <= 50.0)
            & (gdf["lon"] >= -125.0)
            & (gdf["lon"] <= -66.0)
        )
        before = len(gdf)
        gdf = gdf[conus_mask].copy()
        if len(gdf) < before:
            logger.info(
                f"Filtered {before - len(gdf)} non-CONUS substations "
                f"(kept {len(gdf)})"
            )

        logger.info(
            f"Derived {len(gdf)} unique substations from "
            f"{len(transmission_lines)} transmission lines"
        )
        logger.info(
            f"Voltage class distribution:\n"
            f"{gdf['VOLT_CLASS'].value_counts().to_string()}"
        )
        logger.info(
            f"Connectivity: avg {gdf['connected_lines'].mean():.1f} lines per substation"
        )

        return gdf

    def get_substations_on_lines(
        self,
        substations: gpd.GeoDataFrame,
        transmission_lines: gpd.GeoDataFrame,
        buffer_miles: Optional[float] = None,
    ) -> gpd.GeoDataFrame:
        """
        Filter substations to only those connected to qualifying lines.
        When derived from line endpoints, all are connected by definition.
        """
        if buffer_miles is None:
            buffer_miles = self.grid_config.get("substation_buffer_miles", 2.0)

        logger.info(
            f"Verifying substations within {buffer_miles} mi of target lines..."
        )

        if substations.empty or transmission_lines.empty:
            return substations

        if "connected_lines" in substations.columns:
            logger.info(
                f"Substations derived from lines — all {len(substations)} "
                f"are on qualifying lines by definition"
            )
            return substations

        # Fallback: spatial filter for externally-sourced substations
        lines_proj = transmission_lines.to_crs("EPSG:3081")
        subs_proj = substations.to_crs("EPSG:3081")

        buffer_meters = buffer_miles * 1609.34
        lines_buffer = lines_proj.geometry.buffer(buffer_meters).unary_union

        mask = subs_proj.geometry.within(lines_buffer)
        filtered = substations[mask].copy()

        logger.info(f"Filtered to {len(filtered)} substations on qualifying lines")
        return filtered

    def enrich_from_hifld_substations(
        self, derived_subs: gpd.GeoDataFrame
    ) -> gpd.GeoDataFrame:
        """
        Enrich derived substations with data from the HIFLD Substations dataset.

        Spatial-joins derived substations (from transmission line endpoints) with
        the HIFLD substations FeatureServer to add: OWNER, OPERATOR, STATUS,
        CITY, STATE, COUNTY, TYPE, LINES (official line count), MAX_VOLT, MIN_VOLT.

        Falls back gracefully if the HIFLD substations service is unavailable.
        """
        if derived_subs.empty:
            return derived_subs

        logger.info("Enriching substations from HIFLD Substations dataset...")

        try:
            # Query HIFLD substations — fetch all with key fields
            geojson = self.client.query_features(
                service_url=SUBSTATIONS_URL,
                where="STATUS = 'IN SERVICE'",
                out_fields="NAME,CITY,STATE,ZIP,COUNTY,TYPE,STATUS,OWNER,OPERATOR,LINES,MAX_VOLT,MIN_VOLT,NAICS_DESC",
                cache_hours=self.cache_hours,
            )
            hifld_subs = geojson_to_geodataframe(geojson)

            if hifld_subs.empty:
                logger.warning("HIFLD Substations dataset returned no results")
                return derived_subs

            logger.info(f"Fetched {len(hifld_subs)} substations from HIFLD dataset")

            # Try name-based matching first (more reliable than spatial)
            # Normalize names for matching
            derived_subs["_match_name"] = derived_subs["NAME"].str.upper().str.strip()
            hifld_subs["_match_name"] = hifld_subs["NAME"].str.upper().str.strip()

            # Merge on name
            enrichment_cols = [
                "_match_name", "OWNER", "OPERATOR", "STATUS", "CITY",
                "STATE", "COUNTY", "ZIP", "TYPE", "LINES",
                "MAX_VOLT", "MIN_VOLT", "NAICS_DESC",
            ]
            available_cols = [c for c in enrichment_cols if c in hifld_subs.columns]

            # Deduplicate HIFLD subs by name (keep first)
            hifld_unique = hifld_subs[available_cols].drop_duplicates(
                subset=["_match_name"], keep="first"
            )

            merged = derived_subs.merge(
                hifld_unique,
                on="_match_name",
                how="left",
                suffixes=("", "_hifld"),
            )

            matched = merged["OWNER"].notna().sum() if "OWNER" in merged.columns else 0
            logger.info(
                f"Name-matched {matched}/{len(derived_subs)} substations "
                f"({matched/len(derived_subs)*100:.0f}%) with HIFLD dataset"
            )

            # Clean up
            merged.drop(columns=["_match_name"], inplace=True, errors="ignore")

            return merged

        except Exception as e:
            logger.warning(f"HIFLD Substations enrichment failed: {e}")
            logger.info("Continuing with transmission-line-derived data only")
            return derived_subs

    def get_all_grid_data(self) -> dict:
        """
        Main entry point — fetch and process all grid data.

        Returns dict with:
            - substations: GeoDataFrame of all derived substations
            - transmission_lines: GeoDataFrame of qualifying lines
            - substations_on_lines: GeoDataFrame on qualifying lines (enriched)
        """
        lines = self.get_transmission_lines()
        substations = self.derive_substations_from_lines(lines)
        connected_subs = self.get_substations_on_lines(substations, lines)

        # Enrich with HIFLD substations dataset (owner, operator, etc.)
        enriched_subs = self.enrich_from_hifld_substations(connected_subs)

        return {
            "substations": substations,
            "transmission_lines": lines,
            "substations_on_lines": enriched_subs,
        }
