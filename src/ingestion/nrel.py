"""
NREL Data Ingestion — Solar Resource & Renewable Energy Data

Queries the National Renewable Energy Laboratory (NREL) APIs for
solar irradiance data (GHI, DNI) to assess co-location potential
for solar+storage BESS projects.

API docs: https://developer.nrel.gov/docs/solar/
Free API key: https://developer.nrel.gov/
"""

import logging
from typing import Optional

from ..utils.api_client import APIClient

logger = logging.getLogger(__name__)

# NREL Solar Resource API endpoint
SOLAR_RESOURCE_URL = "https://developer.nrel.gov/api/solar/solar_resource/v1.json"


class NRELIngestor:
    """Ingests NREL solar resource data for BESS co-location analysis."""

    def __init__(self, config: dict):
        self.config = config
        self.api_key = config.get("api_keys", {}).get("nrel", "DEMO_KEY")
        self.client = APIClient(
            cache_dir=config.get("cache", {}).get("directory", "./data/cache"),
            cache_enabled=config.get("cache", {}).get("enabled", True),
        )
        self.cache_hours = config.get("cache", {}).get("grid_data_expiry_hours", 720)

    def get_solar_resource(self, lat: float, lon: float) -> dict:
        """
        Get annual average solar resource data for a location.

        Returns dict with:
            - ghi_annual: Global Horizontal Irradiance (kWh/m²/day)
            - dni_annual: Direct Normal Irradiance (kWh/m²/day)
            - solar_score: 0-100 score based on GHI
            - co_location_potential: "high" / "medium" / "low"
            - monthly_ghi: dict of month -> GHI
        """
        try:
            data = self.client.get(
                SOLAR_RESOURCE_URL,
                params={
                    "api_key": self.api_key,
                    "lat": lat,
                    "lon": lon,
                },
                cache_hours=self.cache_hours,
            )
        except Exception as e:
            logger.warning(f"NREL solar resource query failed: {e}")
            return self._default_result()

        if "errors" in data and data["errors"]:
            logger.warning(f"NREL API errors: {data['errors']}")
            return self._default_result()

        outputs = data.get("outputs", {})
        avg_ghi = outputs.get("avg_ghi", {})
        avg_dni = outputs.get("avg_dni", {})

        ghi_annual = avg_ghi.get("annual", 0)
        dni_annual = avg_dni.get("annual", 0)

        # Score: US GHI ranges from ~3.0 (Pacific NW) to ~6.5 (Southwest)
        # 5.0+ = excellent, 4.0-5.0 = good, 3.0-4.0 = moderate, <3.0 = poor
        if ghi_annual >= 5.5:
            solar_score = 100
            co_location = "excellent"
        elif ghi_annual >= 5.0:
            solar_score = 85
            co_location = "high"
        elif ghi_annual >= 4.5:
            solar_score = 70
            co_location = "high"
        elif ghi_annual >= 4.0:
            solar_score = 55
            co_location = "medium"
        elif ghi_annual >= 3.5:
            solar_score = 35
            co_location = "low"
        else:
            solar_score = 15
            co_location = "low"

        # Extract monthly data
        monthly_ghi = {}
        for month_num in range(1, 13):
            key = str(month_num)
            if key in avg_ghi:
                monthly_ghi[key] = avg_ghi[key]

        return {
            "ghi_annual": round(ghi_annual, 2),
            "dni_annual": round(dni_annual, 2),
            "solar_score": solar_score,
            "co_location_potential": co_location,
            "monthly_ghi": monthly_ghi,
        }

    def _default_result(self) -> dict:
        return {
            "ghi_annual": 0,
            "dni_annual": 0,
            "solar_score": 50,  # neutral default
            "co_location_potential": "unknown",
            "monthly_ghi": {},
        }
