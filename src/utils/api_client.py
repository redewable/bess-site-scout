"""
Base API client with caching, retry logic, and rate limiting.
Handles ArcGIS REST, EPA Envirofacts, and generic REST APIs.
"""

import time
import json
import hashlib
import logging
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urlencode

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)


class APIClient:
    """Base HTTP client with caching and retry logic."""

    def __init__(self, cache_dir: str = "./data/cache", cache_enabled: bool = True):
        self.cache_dir = Path(cache_dir)
        self.cache_enabled = cache_enabled
        self.cache_dir.mkdir(parents=True, exist_ok=True)

        # Setup session with retry
        self.session = requests.Session()
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)
        self.session.headers.update({
            "User-Agent": "BESS-Site-Scout/1.0 (ReDewable Energy Internal Tool)"
        })

        # Rate limiting
        self._last_request_time = 0
        self._min_request_interval = 0.25  # 4 requests/sec max

    def _cache_key(self, url: str, params: dict) -> str:
        """Generate a cache key from URL and params."""
        raw = f"{url}|{json.dumps(params, sort_keys=True)}"
        return hashlib.md5(raw.encode()).hexdigest()

    def _get_cached(self, key: str, max_age_hours: float = 24) -> Optional[dict]:
        """Retrieve cached response if fresh enough."""
        if not self.cache_enabled:
            return None
        cache_file = self.cache_dir / f"{key}.json"
        if cache_file.exists():
            age_hours = (time.time() - cache_file.stat().st_mtime) / 3600
            if age_hours < max_age_hours:
                logger.debug(f"Cache hit: {key} (age: {age_hours:.1f}h)")
                with open(cache_file, "r") as f:
                    return json.load(f)
        return None

    def _set_cache(self, key: str, data: dict):
        """Store response in cache."""
        if not self.cache_enabled:
            return
        cache_file = self.cache_dir / f"{key}.json"
        with open(cache_file, "w") as f:
            json.dump(data, f)

    def _rate_limit(self):
        """Enforce rate limiting between requests."""
        elapsed = time.time() - self._last_request_time
        if elapsed < self._min_request_interval:
            time.sleep(self._min_request_interval - elapsed)
        self._last_request_time = time.time()

    def get(
        self,
        url: str,
        params: Optional[dict] = None,
        cache_hours: float = 24,
        timeout: int = 60,
    ) -> dict:
        """Make a cached GET request."""
        params = params or {}
        cache_key = self._cache_key(url, params)

        # Check cache
        cached = self._get_cached(cache_key, cache_hours)
        if cached is not None:
            return cached

        # Make request
        self._rate_limit()
        logger.info(f"GET {url}")
        logger.debug(f"  params: {params}")

        try:
            response = self.session.get(url, params=params, timeout=timeout)
            response.raise_for_status()
            data = response.json()
            self._set_cache(cache_key, data)
            return data
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed: {url} — {e}")
            raise

    def get_raw(self, url: str, params: Optional[dict] = None, timeout: int = 60) -> str:
        """Make a GET request and return raw text (no caching)."""
        self._rate_limit()
        response = self.session.get(url, params=params, timeout=timeout)
        response.raise_for_status()
        return response.text


class ArcGISClient(APIClient):
    """
    Client for ArcGIS REST Feature Service queries.
    Handles pagination, geometry queries, and GeoJSON conversion.
    """

    MAX_RECORD_COUNT = 1000  # ArcGIS default limit

    def query_features(
        self,
        service_url: str,
        where: str = "1=1",
        out_fields: str = "*",
        geometry: Optional[dict] = None,
        geometry_type: str = "esriGeometryEnvelope",
        spatial_rel: str = "esriSpatialRelIntersects",
        distance: Optional[float] = None,
        units: str = "esriSRUnit_Meter",
        return_geometry: bool = True,
        cache_hours: float = 24,
        max_records: Optional[int] = None,
    ) -> dict:
        """
        Query an ArcGIS Feature Service and return GeoJSON.
        Handles pagination automatically.
        """
        all_features = []
        offset = 0
        records_per_page = self.MAX_RECORD_COUNT

        while True:
            params = {
                "where": where,
                "outFields": out_fields,
                "f": "geojson",
                "returnGeometry": str(return_geometry).lower(),
                "resultRecordCount": records_per_page,
                "resultOffset": offset,
            }

            if geometry:
                if isinstance(geometry, dict):
                    params["geometry"] = json.dumps(geometry)
                else:
                    params["geometry"] = str(geometry)
                params["geometryType"] = geometry_type
                params["spatialRel"] = spatial_rel
                params["inSR"] = "4326"
                params["outSR"] = "4326"

            if distance is not None:
                params["distance"] = distance
                params["units"] = units

            url = f"{service_url}/query"
            data = self.get(url, params=params, cache_hours=cache_hours)

            features = data.get("features", [])
            all_features.extend(features)

            logger.info(f"  Retrieved {len(features)} features (total: {len(all_features)})")

            # Check if we got a full page (more data available)
            if len(features) < records_per_page:
                break

            # Check max records limit
            if max_records and len(all_features) >= max_records:
                all_features = all_features[:max_records]
                break

            offset += records_per_page

        return {
            "type": "FeatureCollection",
            "features": all_features,
        }

    def query_point_radius(
        self,
        service_url: str,
        lat: float,
        lon: float,
        radius_miles: float,
        where: str = "1=1",
        out_fields: str = "*",
        cache_hours: float = 24,
    ) -> dict:
        """
        Query features within a radius of a point.
        Convenience wrapper around query_features.
        """
        radius_meters = radius_miles * 1609.34

        geometry = f"{lon},{lat}"

        return self.query_features(
            service_url=service_url,
            where=where,
            out_fields=out_fields,
            geometry=geometry,
            geometry_type="esriGeometryPoint",
            distance=radius_meters,
            units="esriSRUnit_Meter",
            cache_hours=cache_hours,
        )

    def query_bbox(
        self,
        service_url: str,
        xmin: float,
        ymin: float,
        xmax: float,
        ymax: float,
        where: str = "1=1",
        out_fields: str = "*",
        cache_hours: float = 24,
    ) -> dict:
        """
        Query features within a bounding box.
        Coordinates in WGS84 (lon/lat).
        """
        geometry = {
            "xmin": xmin,
            "ymin": ymin,
            "xmax": xmax,
            "ymax": ymax,
            "spatialReference": {"wkid": 4326},
        }

        return self.query_features(
            service_url=service_url,
            where=where,
            out_fields=out_fields,
            geometry=geometry,
            geometry_type="esriGeometryEnvelope",
            cache_hours=cache_hours,
        )


class EPAClient(APIClient):
    """Client for EPA Envirofacts REST API."""

    BASE_URL = "https://enviro.epa.gov/enviro/efservice"

    def query_table(
        self,
        table: str,
        filters: Optional[dict] = None,
        rows: str = "0:999",
        format: str = "json",
        cache_hours: float = 168,
    ) -> list:
        """
        Query an EPA Envirofacts table.

        Args:
            table: Table path, e.g. "sems.sems_active_sites"
            filters: Dict of {column: value} filters
            rows: Row range, e.g. "0:999"
            format: Response format (json, xml, csv)
        """
        # Build URL path from filters
        url_parts = [self.BASE_URL, table]
        if filters:
            for col, val in filters.items():
                url_parts.append(f"{col}/{val}")
        url_parts.append(f"rows/{rows}")
        url_parts.append(format)

        url = "/".join(url_parts)
        return self.get(url, cache_hours=cache_hours)

    def query_by_state(self, table: str, state: str = "TX", **kwargs) -> list:
        """Query a table filtered by state."""
        filters = {"state_code": state}
        return self.query_table(table, filters=filters, **kwargs)


class ECHOClient(APIClient):
    """Client for EPA ECHO (Enforcement & Compliance History Online)."""

    BASE_URL = "https://echo.epa.gov/api"

    def get_facilities(
        self,
        lat: float,
        lon: float,
        radius_miles: float = 1.0,
        state: str = "TX",
        cache_hours: float = 168,
    ) -> dict:
        """Get facilities near a point from ECHO."""
        params = {
            "output": "JSON",
            "p_st": state,
            "p_lat": lat,
            "p_long": lon,
            "p_radius": radius_miles,
        }
        url = f"{self.BASE_URL}/echo_rest_services.get_facilities"
        return self.get(url, params=params, cache_hours=cache_hours)
