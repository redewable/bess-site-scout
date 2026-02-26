"""
USFWS Data Ingestion — Wetlands & Endangered Species

National Wetlands Inventory (NWI) and Critical Habitat data
from US Fish & Wildlife Service.
"""

import logging
from typing import Optional

import geopandas as gpd

from ..utils.api_client import ArcGISClient
from ..utils.geo import geojson_to_geodataframe, check_intersection

logger = logging.getLogger(__name__)

# Service URLs
NWI_URL = (
    "https://fwspublicservices.wim.usgs.gov/wetlandsmapservice/rest/services/"
    "Wetlands/MapServer/0"
)
# NOTE: Critical habitat service renamed — updated Feb 2026
CRITICAL_HABITAT_URL = (
    "https://services.arcgis.com/QVENGdaPbd4LUkLV/arcgis/rest/services/"
    "USFWS_Critical_Habitat/FeatureServer/0"
)


class USFWSIngestor:
    """Ingests USFWS wetlands and endangered species habitat data."""

    def __init__(self, config: dict):
        self.config = config
        self.client = ArcGISClient(
            cache_dir=config.get("cache", {}).get("directory", "./data/cache"),
            cache_enabled=config.get("cache", {}).get("enabled", True),
        )
        self.wetland_config = config.get("environmental", {}).get("wetlands", {})
        self.es_config = config.get("environmental", {}).get("endangered_species", {})
        self.cache_hours = config.get("cache", {}).get("environmental_expiry_hours", 168)

    def search_wetlands(
        self,
        lat: float,
        lon: float,
        radius_miles: float = 0.5,
    ) -> gpd.GeoDataFrame:
        """
        Search for NWI wetlands near a point.

        Returns GeoDataFrame with wetland polygons and classification codes.
        """
        logger.debug(f"Querying NWI wetlands at ({lat}, {lon}), r={radius_miles}mi")

        try:
            geojson = self.client.query_point_radius(
                service_url=NWI_URL,
                lat=lat, lon=lon,
                radius_miles=radius_miles,
                out_fields="WETLAND_TYPE,ATTRIBUTE,ACRES,SHAPE_Area",
                cache_hours=self.cache_hours,
            )
            return geojson_to_geodataframe(geojson)
        except Exception as e:
            logger.warning(f"NWI wetlands query failed: {e}")
            return gpd.GeoDataFrame()

    def search_critical_habitat(
        self,
        lat: float,
        lon: float,
        radius_miles: float = 1.0,
    ) -> gpd.GeoDataFrame:
        """
        Search for designated critical habitat for threatened/endangered species.

        Returns GeoDataFrame with critical habitat polygons and species info.
        """
        logger.debug(f"Querying critical habitat at ({lat}, {lon}), r={radius_miles}mi")

        try:
            geojson = self.client.query_point_radius(
                service_url=CRITICAL_HABITAT_URL,
                lat=lat, lon=lon,
                radius_miles=radius_miles,
                out_fields="comname,sciname,status,listing_st,SHAPE_Area",
                cache_hours=self.cache_hours,
            )
            return geojson_to_geodataframe(geojson)
        except Exception as e:
            logger.warning(f"Critical habitat query failed: {e}")
            return gpd.GeoDataFrame()

    def run_full_screening(
        self,
        lat: float,
        lon: float,
        parcel_geometry=None,
    ) -> dict:
        """
        Run complete USFWS screening for a location.

        Returns dict with:
            - wetlands: {count, types, acres, intersection_pct}
            - critical_habitat: {present, species}
            - risk_flags: list of flag strings
            - eliminate: bool
        """
        results = {
            "wetlands": {
                "count": 0,
                "types": [],
                "total_acres": 0.0,
                "intersection_pct": 0.0,
            },
            "critical_habitat": {
                "present": False,
                "species": [],
            },
            "risk_flags": [],
            "eliminate": False,
        }

        # 1. Wetlands
        wetlands = self.search_wetlands(lat, lon)
        if not wetlands.empty:
            results["wetlands"]["count"] = len(wetlands)

            if "WETLAND_TYPE" in wetlands.columns:
                results["wetlands"]["types"] = wetlands["WETLAND_TYPE"].unique().tolist()
            if "ACRES" in wetlands.columns:
                results["wetlands"]["total_acres"] = round(wetlands["ACRES"].sum(), 1)

            # Check intersection with parcel
            if parcel_geometry is not None:
                intersects, pct = check_intersection(parcel_geometry, wetlands)
                results["wetlands"]["intersection_pct"] = round(pct, 1)

                max_pct = self.wetland_config.get("max_wetland_pct", 50)
                if pct > max_pct:
                    results["eliminate"] = True
                    results["risk_flags"].append(
                        f"ELIMINATE: {pct:.1f}% of parcel is NWI wetlands (max {max_pct}%)"
                    )
                elif pct > 0:
                    results["risk_flags"].append(
                        f"WARNING: {pct:.1f}% of parcel intersects NWI wetlands — "
                        f"Section 404 permitting may be required"
                    )
            else:
                results["risk_flags"].append(
                    f"WARNING: {len(wetlands)} NWI wetland feature(s) near site — "
                    f"parcel-level review needed"
                )

        # 2. Critical Habitat
        habitat = self.search_critical_habitat(lat, lon)
        if not habitat.empty:
            results["critical_habitat"]["present"] = True

            species = []
            if "comname" in habitat.columns:
                species = habitat["comname"].dropna().unique().tolist()
            elif "sciname" in habitat.columns:
                species = habitat["sciname"].dropna().unique().tolist()
            results["critical_habitat"]["species"] = species

            if self.es_config.get("eliminate", False):
                results["eliminate"] = True
                results["risk_flags"].append(
                    f"ELIMINATE: Critical habitat for {', '.join(species[:3])}"
                )
            else:
                results["risk_flags"].append(
                    f"CRITICAL: Designated critical habitat for "
                    f"{', '.join(species[:3])} — ESA consultation required"
                )

        return results
