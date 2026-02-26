"""
FEMA National Flood Hazard Layer (NFHL) Ingestion

Queries FEMA flood zone data for parcels to assess flood risk.
"""

import logging
from typing import Tuple

import geopandas as gpd

from ..utils.api_client import ArcGISClient
from ..utils.geo import geojson_to_geodataframe, point_buffer_bbox

logger = logging.getLogger(__name__)

# NOTE: FEMA NFHL migrated from /gis/nfhl/rest/ to /arcgis/rest/ — updated Feb 2026
NFHL_URL = "https://hazards.fema.gov/arcgis/rest/services/public/NFHL/MapServer"
FLOOD_ZONES_LAYER = 28  # S_Fld_Haz_Ar (Flood Hazard Zones) — confirmed working


class FEMAIngestor:
    """Ingests FEMA National Flood Hazard Layer data."""

    # Zone risk classification
    HIGH_RISK_ZONES = {"A", "AE", "AH", "AO", "AR", "V", "VE", "A99"}
    MODERATE_RISK_ZONES = {"X"}  # Shaded X (0.2% annual chance)
    LOW_RISK_ZONES = {"X"}  # Unshaded X
    UNDETERMINED_ZONES = {"D"}

    def __init__(self, config: dict):
        self.config = config
        self.client = ArcGISClient(
            cache_dir=config.get("cache", {}).get("directory", "./data/cache"),
            cache_enabled=config.get("cache", {}).get("enabled", True),
        )
        self.flood_config = config.get("environmental", {}).get("flood", {})
        self.cache_hours = config.get("cache", {}).get("environmental_expiry_hours", 168)

    def get_flood_zones_at_point(
        self,
        lat: float,
        lon: float,
        radius_miles: float = 0.5,
    ) -> gpd.GeoDataFrame:
        """
        Fetch FEMA flood zone data around a point.

        Returns GeoDataFrame with flood zone polygons and attributes.
        """
        logger.debug(f"Querying FEMA flood zones at ({lat}, {lon}), r={radius_miles}mi")

        service_url = f"{NFHL_URL}/{FLOOD_ZONES_LAYER}"

        geojson = self.client.query_point_radius(
            service_url=service_url,
            lat=lat,
            lon=lon,
            radius_miles=radius_miles,
            out_fields="FLD_ZONE,ZONE_SUBTY,SFHA_TF,STATIC_BFE,DEPTH,LEN_UNIT,V_DATUM",
            cache_hours=self.cache_hours,
        )

        return geojson_to_geodataframe(geojson)

    def get_flood_zones_in_bbox(
        self,
        xmin: float,
        ymin: float,
        xmax: float,
        ymax: float,
    ) -> gpd.GeoDataFrame:
        """Fetch FEMA flood zone data within a bounding box."""
        service_url = f"{NFHL_URL}/{FLOOD_ZONES_LAYER}"

        geojson = self.client.query_bbox(
            service_url=service_url,
            xmin=xmin, ymin=ymin, xmax=xmax, ymax=ymax,
            out_fields="FLD_ZONE,ZONE_SUBTY,SFHA_TF,STATIC_BFE",
            cache_hours=self.cache_hours,
        )

        return geojson_to_geodataframe(geojson)

    def assess_flood_risk(
        self,
        lat: float,
        lon: float,
        parcel_geometry=None,
    ) -> dict:
        """
        Assess flood risk for a location/parcel.

        Returns dict with:
            - flood_zone: Primary flood zone at location
            - in_sfha: Whether in Special Flood Hazard Area
            - risk_level: "high", "moderate", "low", "undetermined"
            - zones_present: List of all flood zones intersecting area
            - floodplain_pct: Percentage of parcel in floodplain (if geometry provided)
            - eliminate: Whether this triggers elimination
            - details: Human-readable summary
        """
        flood_data = self.get_flood_zones_at_point(lat, lon)

        result = {
            "flood_zone": None,
            "in_sfha": False,
            "risk_level": "unknown",
            "zones_present": [],
            "floodplain_pct": 0.0,
            "eliminate": False,
            "details": "",
        }

        if flood_data.empty:
            result["risk_level"] = "unknown"
            result["details"] = "No FEMA flood data available for this location"
            return result

        # Get unique zones
        zones = set()
        if "FLD_ZONE" in flood_data.columns:
            zones = set(flood_data["FLD_ZONE"].dropna().unique())
        result["zones_present"] = sorted(zones)

        # Check for SFHA
        if "SFHA_TF" in flood_data.columns:
            result["in_sfha"] = any(flood_data["SFHA_TF"] == "T")

        # Determine risk level
        high_risk = zones & self.HIGH_RISK_ZONES
        if high_risk:
            result["risk_level"] = "high"
            result["flood_zone"] = sorted(high_risk)[0]
        elif zones & self.UNDETERMINED_ZONES:
            result["risk_level"] = "undetermined"
            result["flood_zone"] = "D"
        elif "ZONE_SUBTY" in flood_data.columns:
            # Check for shaded vs unshaded Zone X
            subtypes = set(flood_data["ZONE_SUBTY"].dropna().unique())
            if any("SHADED" in str(s).upper() for s in subtypes):
                result["risk_level"] = "moderate"
                result["flood_zone"] = "X (shaded)"
            else:
                result["risk_level"] = "low"
                result["flood_zone"] = "X (unshaded)"
        else:
            result["risk_level"] = "low"
            result["flood_zone"] = sorted(zones)[0] if zones else None

        # Calculate floodplain percentage if parcel geometry provided
        if parcel_geometry is not None and not flood_data.empty:
            high_risk_zones = flood_data[
                flood_data["FLD_ZONE"].isin(self.HIGH_RISK_ZONES)
            ]
            if not high_risk_zones.empty:
                from ..utils.geo import check_intersection
                _, pct = check_intersection(parcel_geometry, high_risk_zones)
                result["floodplain_pct"] = round(pct, 1)

        # Check elimination criteria
        max_pct = self.flood_config.get("max_floodplain_pct", 25)
        eliminate_zones = set(self.flood_config.get("eliminate_zones", self.HIGH_RISK_ZONES))

        if high_risk and result["floodplain_pct"] > max_pct:
            result["eliminate"] = True
        elif zones & eliminate_zones and parcel_geometry is None:
            # If no parcel geometry, flag high-risk zones as potential eliminators
            result["eliminate"] = False  # Can't confirm without parcel geometry

        # Human-readable summary
        if result["eliminate"]:
            result["details"] = (
                f"ELIMINATE: {result['floodplain_pct']}% in {result['flood_zone']} "
                f"floodplain (max {max_pct}%)"
            )
        elif result["risk_level"] == "high":
            result["details"] = f"HIGH RISK: Zone {result['flood_zone']} present — requires parcel-level review"
        elif result["risk_level"] == "moderate":
            result["details"] = f"MODERATE: Zone X (shaded) — 0.2% annual chance flood area"
        elif result["risk_level"] == "low":
            result["details"] = f"LOW RISK: Zone X (unshaded) — minimal flood hazard"
        else:
            result["details"] = f"Flood zones present: {', '.join(result['zones_present'])}"

        return result
