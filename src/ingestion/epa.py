"""
EPA Environmental Database Ingestion

Queries EPA Envirofacts (SEMS/Superfund, RCRA, TRI, Brownfields)
and ECHO (Enforcement & Compliance) for environmental screening.
"""

import logging
from typing import Optional

import geopandas as gpd

from ..utils.api_client import ArcGISClient, EPAClient, ECHOClient
from ..utils.geo import geojson_to_geodataframe, haversine_distance

logger = logging.getLogger(__name__)

# EPA ArcGIS services for spatial queries (preferred over Envirofacts for geo)
EPA_SERVICES = {
    "superfund_npl": {
        "url": "https://services.arcgis.com/cJ9YHowT8TkDC5LP/arcgis/rest/services/Superfund_NPL_Sites/FeatureServer/0",
        "name": "Superfund NPL Sites",
    },
    "brownfields": {
        "url": "https://services.arcgis.com/cJ9YHowT8TkDC5LP/arcgis/rest/services/ACRES_Brownfields/FeatureServer/0",
        "name": "Brownfields (ACRES)",
    },
    "tri_facilities": {
        "url": "https://services.arcgis.com/cJ9YHowT8TkDC5LP/arcgis/rest/services/TRI_Facilities/FeatureServer/0",
        "name": "Toxic Release Inventory Facilities",
    },
}

# Note: Some EPA ArcGIS services may have different or updated URLs.
# The ECHO API is the most reliable for radius-based facility searches.


class EPAIngestor:
    """Ingests EPA environmental data for Phase I ESA screening."""

    def __init__(self, config: dict):
        self.config = config
        self.arcgis_client = ArcGISClient(
            cache_dir=config.get("cache", {}).get("directory", "./data/cache"),
            cache_enabled=config.get("cache", {}).get("enabled", True),
        )
        self.epa_client = EPAClient(
            cache_dir=config.get("cache", {}).get("directory", "./data/cache"),
            cache_enabled=config.get("cache", {}).get("enabled", True),
        )
        self.echo_client = ECHOClient(
            cache_dir=config.get("cache", {}).get("directory", "./data/cache"),
            cache_enabled=config.get("cache", {}).get("enabled", True),
        )
        self.epa_config = config.get("environmental", {}).get("epa", {})
        self.cache_hours = config.get("cache", {}).get("environmental_expiry_hours", 168)

    def search_echo_facilities(
        self,
        lat: float,
        lon: float,
        radius_miles: float = 1.0,
    ) -> dict:
        """
        Search EPA ECHO for all regulated facilities near a point.
        ECHO is the most comprehensive single-query option for EPA data.
        """
        logger.debug(f"Querying EPA ECHO at ({lat}, {lon}), r={radius_miles}mi")

        try:
            data = self.echo_client.get_facilities(
                lat=lat, lon=lon,
                radius_miles=radius_miles,
                cache_hours=self.cache_hours,
            )
            return data
        except Exception as e:
            logger.warning(f"ECHO query failed: {e}")
            return {}

    def search_superfund(
        self,
        lat: float,
        lon: float,
        radius_miles: Optional[float] = None,
    ) -> gpd.GeoDataFrame:
        """Search for Superfund/NPL sites near a point."""
        if radius_miles is None:
            radius_miles = self.epa_config.get("npl_radius", 1.0)

        logger.debug(f"Searching Superfund NPL within {radius_miles}mi of ({lat}, {lon})")

        try:
            service = EPA_SERVICES["superfund_npl"]
            geojson = self.arcgis_client.query_point_radius(
                service_url=service["url"],
                lat=lat, lon=lon,
                radius_miles=radius_miles,
                cache_hours=self.cache_hours,
            )
            return geojson_to_geodataframe(geojson)
        except Exception as e:
            logger.warning(f"Superfund ArcGIS query failed: {e}")
            return gpd.GeoDataFrame()

    def search_brownfields(
        self,
        lat: float,
        lon: float,
        radius_miles: Optional[float] = None,
    ) -> gpd.GeoDataFrame:
        """Search for Brownfields/ACRES sites near a point."""
        if radius_miles is None:
            radius_miles = self.epa_config.get("brownfields_radius", 0.5)

        logger.debug(f"Searching Brownfields within {radius_miles}mi of ({lat}, {lon})")

        try:
            service = EPA_SERVICES["brownfields"]
            geojson = self.arcgis_client.query_point_radius(
                service_url=service["url"],
                lat=lat, lon=lon,
                radius_miles=radius_miles,
                cache_hours=self.cache_hours,
            )
            return geojson_to_geodataframe(geojson)
        except Exception as e:
            logger.warning(f"Brownfields query failed: {e}")
            return gpd.GeoDataFrame()

    def search_tri(
        self,
        lat: float,
        lon: float,
        radius_miles: float = 1.0,
    ) -> gpd.GeoDataFrame:
        """Search for Toxic Release Inventory facilities near a point."""
        logger.debug(f"Searching TRI within {radius_miles}mi of ({lat}, {lon})")

        try:
            service = EPA_SERVICES["tri_facilities"]
            geojson = self.arcgis_client.query_point_radius(
                service_url=service["url"],
                lat=lat, lon=lon,
                radius_miles=radius_miles,
                cache_hours=self.cache_hours,
            )
            return geojson_to_geodataframe(geojson)
        except Exception as e:
            logger.warning(f"TRI query failed: {e}")
            return gpd.GeoDataFrame()

    def run_full_screening(self, lat: float, lon: float) -> dict:
        """
        Run complete EPA environmental screening for a location.

        Returns dict with results from all EPA databases:
            - superfund: {count, sites, nearest_distance_mi}
            - rcra: {count, facilities}
            - brownfields: {count, sites}
            - tri: {count, facilities}
            - echo_summary: {total_facilities, violations}
            - risk_flags: list of flag strings
            - eliminate: bool
        """
        results = {
            "superfund": {"count": 0, "sites": [], "nearest_distance_mi": None},
            "brownfields": {"count": 0, "sites": []},
            "tri": {"count": 0, "facilities": []},
            "echo_summary": {"total_facilities": 0, "significant_violations": 0},
            "risk_flags": [],
            "eliminate": False,
        }

        # 1. Superfund / NPL
        npl_radius = self.epa_config.get("npl_radius", 1.0)
        superfund = self.search_superfund(lat, lon, npl_radius)
        if not superfund.empty:
            results["superfund"]["count"] = len(superfund)
            results["superfund"]["sites"] = superfund.get("SITE_NAME", superfund.index).tolist()

            # Calculate nearest distance
            if "geometry" in superfund.columns:
                distances = superfund.geometry.apply(
                    lambda g: haversine_distance(lat, lon, g.centroid.y, g.centroid.x)
                )
                results["superfund"]["nearest_distance_mi"] = round(distances.min(), 2)

            if results["superfund"]["nearest_distance_mi"] and results["superfund"]["nearest_distance_mi"] < 0.25:
                results["risk_flags"].append(
                    f"CRITICAL: Superfund NPL site within {results['superfund']['nearest_distance_mi']}mi"
                )
                results["eliminate"] = True
            else:
                results["risk_flags"].append(
                    f"WARNING: {len(superfund)} Superfund site(s) within {npl_radius}mi"
                )

        # 2. Brownfields
        bf_radius = self.epa_config.get("brownfields_radius", 0.5)
        brownfields = self.search_brownfields(lat, lon, bf_radius)
        if not brownfields.empty:
            results["brownfields"]["count"] = len(brownfields)
            results["risk_flags"].append(
                f"NOTE: {len(brownfields)} Brownfields site(s) within {bf_radius}mi"
            )

        # 3. TRI
        tri = self.search_tri(lat, lon, radius_miles=1.0)
        if not tri.empty:
            results["tri"]["count"] = len(tri)
            results["risk_flags"].append(
                f"NOTE: {len(tri)} TRI facility(ies) within 1.0mi"
            )

        # 4. ECHO comprehensive search
        try:
            echo_data = self.search_echo_facilities(lat, lon, radius_miles=1.0)
            if echo_data and "Results" in echo_data:
                facilities = echo_data["Results"].get("Facilities", [])
                results["echo_summary"]["total_facilities"] = len(facilities)

                # Count significant violations
                sig_violations = sum(
                    1 for f in facilities
                    if f.get("CWAStatus") == "Significant Violation"
                    or f.get("RCRAStatus") == "Significant Violation"
                    or f.get("CAASstatus") == "Significant Violation"
                )
                results["echo_summary"]["significant_violations"] = sig_violations

                if sig_violations > 0:
                    results["risk_flags"].append(
                        f"WARNING: {sig_violations} facilities with significant violations within 1mi"
                    )
        except Exception as e:
            logger.warning(f"ECHO comprehensive search failed: {e}")

        return results
