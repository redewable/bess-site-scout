"""
TCEQ Environmental Database Ingestion

Texas Commission on Environmental Quality data:
- Leaking Petroleum Storage Tanks (LPST)
- Petroleum Storage Tanks (UST/AST)
- Industrial & Hazardous Waste (IHW)
- Municipal Solid Waste (MSW)
"""

import logging
from typing import Optional

import geopandas as gpd

from ..utils.api_client import ArcGISClient
from ..utils.geo import geojson_to_geodataframe, haversine_distance

logger = logging.getLogger(__name__)

# TCEQ ArcGIS service URLs
# NOTE (Feb 2026): The KTcxiTD9dsQw4r7Z ArcGIS server is actually TxDOT, not TCEQ.
# TCEQ previously cross-listed LPST/PST services there, but they've been removed.
# These URLs are BROKEN. The module will gracefully skip with warnings.
# EPA ECHO covers most of the same facilities (petroleum storage, hazardous waste).
# TODO: Find new TCEQ GIS endpoints or switch to TCEQ CSV bulk downloads.
#       Check: https://gis-tceq.opendata.arcgis.com
#       Bulk: https://www.tceq.texas.gov/agency/data/lookup-data/pst-datasets-records.html
TCEQ_SERVICES = {
    "lpst": {
        "url": "https://services.arcgis.com/KTcxiTD9dsQw4r7Z/arcgis/rest/services/LPST_Points/FeatureServer/0",
        "name": "Leaking Petroleum Storage Tanks",
        "verified": False,
        "status": "BROKEN — service removed from TxDOT ArcGIS server",
    },
    "pst": {
        "url": "https://services.arcgis.com/KTcxiTD9dsQw4r7Z/arcgis/rest/services/PetroleumStorageTanks/FeatureServer/0",
        "name": "Petroleum Storage Tanks (UST/AST)",
        "verified": False,
        "status": "BROKEN — service removed from TxDOT ArcGIS server",
    },
    "ihw": {
        "url": "https://services.arcgis.com/KTcxiTD9dsQw4r7Z/arcgis/rest/services/IHW_Facilities/FeatureServer/0",
        "name": "Industrial & Hazardous Waste",
        "verified": False,
    },
    "msw": {
        "url": "https://services.arcgis.com/KTcxiTD9dsQw4r7Z/arcgis/rest/services/MSW_Sites/FeatureServer/0",
        "name": "Municipal Solid Waste",
        "verified": False,
    },
    "drycleaners": {
        "url": "https://services.arcgis.com/KTcxiTD9dsQw4r7Z/arcgis/rest/services/Drycleaners/FeatureServer/0",
        "name": "Dry Cleaners",
        "verified": False,
    },
}


class TCEQIngestor:
    """Ingests TCEQ environmental data for Phase I ESA screening."""

    def __init__(self, config: dict):
        self.config = config
        self.client = ArcGISClient(
            cache_dir=config.get("cache", {}).get("directory", "./data/cache"),
            cache_enabled=config.get("cache", {}).get("enabled", True),
        )
        self.tceq_config = config.get("environmental", {}).get("tceq", {})
        self.cache_hours = config.get("cache", {}).get("environmental_expiry_hours", 168)

    def _query_service(
        self,
        service_key: str,
        lat: float,
        lon: float,
        radius_miles: float,
    ) -> gpd.GeoDataFrame:
        """Generic query for any TCEQ ArcGIS service."""
        service = TCEQ_SERVICES.get(service_key)
        if not service:
            logger.error(f"Unknown TCEQ service: {service_key}")
            return gpd.GeoDataFrame()

        logger.debug(f"Querying TCEQ {service['name']} within {radius_miles}mi of ({lat}, {lon})")

        try:
            geojson = self.client.query_point_radius(
                service_url=service["url"],
                lat=lat, lon=lon,
                radius_miles=radius_miles,
                cache_hours=self.cache_hours,
            )
            return geojson_to_geodataframe(geojson)
        except Exception as e:
            logger.warning(f"TCEQ {service['name']} query failed: {e}")
            if not service.get("verified", True):
                logger.info(
                    f"  Service URL may have changed. Check TCEQ GIS Hub: "
                    f"https://gis-tceq.opendata.arcgis.com"
                )
            return gpd.GeoDataFrame()

    def search_lpst(
        self, lat: float, lon: float, radius_miles: Optional[float] = None
    ) -> gpd.GeoDataFrame:
        """Search for Leaking Petroleum Storage Tanks near a point."""
        if radius_miles is None:
            radius_miles = self.tceq_config.get("lpst_radius", 0.5)
        return self._query_service("lpst", lat, lon, radius_miles)

    def search_ust(
        self, lat: float, lon: float, radius_miles: Optional[float] = None
    ) -> gpd.GeoDataFrame:
        """Search for all Petroleum Storage Tanks (UST/AST) near a point."""
        if radius_miles is None:
            radius_miles = self.tceq_config.get("ust_radius", 0.25)
        return self._query_service("pst", lat, lon, radius_miles)

    def search_ihw(
        self, lat: float, lon: float, radius_miles: Optional[float] = None
    ) -> gpd.GeoDataFrame:
        """Search for Industrial & Hazardous Waste facilities."""
        if radius_miles is None:
            radius_miles = self.tceq_config.get("ihw_radius", 0.5)
        return self._query_service("ihw", lat, lon, radius_miles)

    def search_msw(
        self, lat: float, lon: float, radius_miles: Optional[float] = None
    ) -> gpd.GeoDataFrame:
        """Search for Municipal Solid Waste sites (landfills)."""
        if radius_miles is None:
            radius_miles = self.tceq_config.get("msw_radius", 1.0)
        return self._query_service("msw", lat, lon, radius_miles)

    def search_drycleaners(
        self, lat: float, lon: float, radius_miles: Optional[float] = None
    ) -> gpd.GeoDataFrame:
        """Search for dry cleaner sites (PCE/perc contamination risk)."""
        if radius_miles is None:
            radius_miles = self.tceq_config.get("drycleaners_radius", 0.25)
        return self._query_service("drycleaners", lat, lon, radius_miles)

    def run_full_screening(self, lat: float, lon: float) -> dict:
        """
        Run complete TCEQ environmental screening for a location.

        Returns dict with results from all TCEQ databases.
        """
        results = {
            "lpst": {"count": 0, "nearest_distance_mi": None, "sites": []},
            "ust": {"count": 0, "nearest_distance_mi": None},
            "ihw": {"count": 0},
            "msw": {"count": 0},
            "drycleaners": {"count": 0},
            "risk_flags": [],
            "eliminate": False,
        }

        # 1. Leaking Petroleum Storage Tanks — most critical
        lpst = self.search_lpst(lat, lon)
        if not lpst.empty:
            results["lpst"]["count"] = len(lpst)

            # Calculate nearest distance
            distances = lpst.geometry.apply(
                lambda g: haversine_distance(lat, lon, g.centroid.y, g.centroid.x)
            )
            nearest = distances.min()
            results["lpst"]["nearest_distance_mi"] = round(nearest, 3)

            # Get site names if available
            name_cols = ["SITE_NAME", "FACILITY_N", "NAME", "FacilityNa"]
            for col in name_cols:
                if col in lpst.columns:
                    results["lpst"]["sites"] = lpst[col].tolist()[:5]
                    break

            if nearest < 0.1:
                results["risk_flags"].append(
                    f"CRITICAL: LPST within {nearest:.3f}mi — likely contamination concern"
                )
            else:
                results["risk_flags"].append(
                    f"WARNING: {len(lpst)} LPST(s) within search radius, nearest {nearest:.2f}mi"
                )

        # 2. Underground Storage Tanks
        ust = self.search_ust(lat, lon)
        if not ust.empty:
            results["ust"]["count"] = len(ust)
            distances = ust.geometry.apply(
                lambda g: haversine_distance(lat, lon, g.centroid.y, g.centroid.x)
            )
            results["ust"]["nearest_distance_mi"] = round(distances.min(), 3)
            results["risk_flags"].append(
                f"NOTE: {len(ust)} UST/AST(s) within search radius"
            )

        # 3. Industrial & Hazardous Waste
        ihw = self.search_ihw(lat, lon)
        if not ihw.empty:
            results["ihw"]["count"] = len(ihw)
            results["risk_flags"].append(
                f"WARNING: {len(ihw)} IHW facility(ies) within search radius"
            )

        # 4. Municipal Solid Waste / Landfills
        msw = self.search_msw(lat, lon)
        if not msw.empty:
            results["msw"]["count"] = len(msw)
            results["risk_flags"].append(
                f"NOTE: {len(msw)} MSW site(s) within search radius"
            )

        # 5. Dry Cleaners
        dc = self.search_drycleaners(lat, lon)
        if not dc.empty:
            results["drycleaners"]["count"] = len(dc)
            results["risk_flags"].append(
                f"NOTE: {len(dc)} dry cleaner(s) within search radius (PCE risk)"
            )

        return results
