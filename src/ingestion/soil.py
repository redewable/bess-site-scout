"""
Soil & Geological Conditions Ingestion (USDA SSURGO)

Pulls soil properties from:
  1. USDA Soil Data Access (SDA) — REST/WFS service for SSURGO data
  2. USDA Web Soil Survey — tabular soil property queries
  3. USGS Geologic Hazards — earthquake, landslide, sinkhole risk

For BESS site feasibility, soil data determines:
  - Foundation requirements (bearing capacity)
  - Drainage / stormwater management costs
  - Flood susceptibility (beyond FEMA zones)
  - Excavation difficulty (depth to bedrock)
  - Erosion risk (site stabilization costs)
  - Corrosion potential (affects underground infrastructure)
  - Shrink-swell potential (foundation design impacts)
"""

import logging
import json
from typing import Optional, Dict
from pathlib import Path

import pandas as pd

from ..utils.api_client import APIClient

logger = logging.getLogger(__name__)

# ── USDA Soil Data Access (SDA) endpoints ─────────────────────────
SDA_TABULAR_URL = "https://SDMDataAccess.sc.egov.usda.gov/Tabular/post.rest"
SDA_SPATIAL_WFS = (
    "https://SDMDataAccess.sc.egov.usda.gov/Spatial/SDMWGS84Geographic.wfs"
)

# ── USGS Geologic Hazards ────────────────────────────────────────
USGS_EARTHQUAKE_SERVICE = (
    "https://earthquake.usgs.gov/ws/designmaps/asce7-22.json"
)
USGS_LANDSLIDE_SERVICE = (
    "https://gis.usgs.gov/arcgis/rest/services/lss/MapServer"
)

# ── BESS-relevant soil properties and their SDA query columns ─────
SOIL_PROPERTIES = {
    "drainage_class": {
        "column": "drainagecl",
        "table": "component",
        "description": "Natural drainage class",
        "bess_impact": "Poor drainage = expensive stormwater systems",
        "good_values": ["Well drained", "Somewhat excessively drained", "Excessively drained"],
        "bad_values": ["Poorly drained", "Very poorly drained"],
    },
    "hydrologic_group": {
        "column": "hydgrp",
        "table": "component",
        "description": "Hydrologic soil group (A-D)",
        "bess_impact": "Group D = highest runoff, hardest to drain",
        "good_values": ["A", "B"],
        "bad_values": ["C/D", "D"],
    },
    "depth_to_water_table": {
        "column": "soimoistdept_r",
        "table": "cosoilmoist",
        "description": "Depth to seasonal high water table (cm)",
        "bess_impact": "Shallow water table = foundation issues",
        "good_threshold_cm": 100,  # >100cm is good
    },
    "depth_to_bedrock": {
        "column": "brockdepmin",
        "table": "component",
        "description": "Minimum depth to bedrock (cm)",
        "bess_impact": "Shallow bedrock = expensive excavation",
        "good_threshold_cm": 100,
    },
    "flooding_frequency": {
        "column": "flodfreqcl",
        "table": "comonth",
        "description": "Flooding frequency class",
        "bess_impact": "Frequent flooding = site instability",
        "good_values": ["None"],
        "bad_values": ["Frequent", "Very frequent"],
    },
    "slope": {
        "column": "slope_r",
        "table": "component",
        "description": "Representative slope (%)",
        "bess_impact": "Steep slopes = grading costs",
        "good_threshold_pct": 5,  # <5% is ideal for BESS
    },
    "shrink_swell": {
        "column": "weg",
        "table": "component",
        "description": "Wind erodibility group / shrink-swell proxy",
        "bess_impact": "High shrink-swell = foundation movement risk",
    },
    "corrosion_steel": {
        "column": "corcon",
        "table": "component",
        "description": "Corrosion of concrete",
        "bess_impact": "High corrosion = accelerated infrastructure degradation",
        "good_values": ["Low"],
        "bad_values": ["High"],
    },
}


class SoilIngestor:
    """
    Ingests soil and geological data from USDA SSURGO.

    Provides:
      - get_soil_at_point(): Soil properties at a specific coordinate
      - get_soil_suitability(): Score soil suitability for BESS
      - get_earthquake_risk(): Seismic design parameters
      - get_soil_summary(): Summary for pipeline output
    """

    def __init__(self, config: dict):
        self.config = config
        self.client = APIClient(
            cache_dir=config.get("cache", {}).get("directory", "./data/cache"),
            cache_enabled=config.get("cache", {}).get("enabled", True),
        )
        self.cache_hours = config.get("cache", {}).get("grid_data_expiry_hours", 720)
        self.cache_dir = Path(
            config.get("cache", {}).get("directory", "./data/cache")
        )
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def _query_sda(self, sql: str) -> pd.DataFrame:
        """
        Execute a T-SQL query against USDA Soil Data Access.

        The SDA REST endpoint accepts T-SQL queries that run against
        the SSURGO database. Returns results as JSON.
        """
        try:
            payload = {
                "query": sql,
                "format": "JSON",
            }

            response = self.client.session.post(
                SDA_TABULAR_URL,
                json=payload,
                timeout=60,
            )
            response.raise_for_status()
            data = response.json()

            if "Table" not in data:
                return pd.DataFrame()

            rows = data["Table"]
            if not rows:
                return pd.DataFrame()

            # First row is column names
            columns = rows[0]
            data_rows = rows[1:]

            df = pd.DataFrame(data_rows, columns=columns)
            return df

        except Exception as e:
            logger.warning(f"  SDA query failed: {e}")
            return pd.DataFrame()

    def get_soil_at_point(
        self,
        lat: float,
        lon: float,
    ) -> Dict:
        """
        Get soil properties at a specific coordinate using SDA spatial query.

        Uses SDA's spatial stored procedure to find the soil map unit
        at the given point, then queries component properties.
        """
        try:
            # Step 1: Find the map unit at this point
            # SDA spatial query using a point geometry
            spatial_sql = f"""
            SELECT
                mu.mukey, mu.muname, mu.mukind,
                mu.farmlndcl, mu.musym
            FROM mapunit AS mu
            INNER JOIN SDA_Get_Mukey_from_intersection_with_WktWgs84(
                'POINT({lon} {lat})'
            ) AS p ON mu.mukey = p.mukey
            """

            mu_df = self._query_sda(spatial_sql)

            if mu_df.empty:
                logger.info(f"  SSURGO: no soil data at ({lat}, {lon})")
                return self._default_soil(lat, lon)

            mukey = mu_df.iloc[0].get("mukey", "")
            muname = mu_df.iloc[0].get("muname", "")
            farmland = mu_df.iloc[0].get("farmlndcl", "")

            # Step 2: Get component properties
            comp_sql = f"""
            SELECT TOP 1
                c.compname, c.comppct_r, c.drainagecl,
                c.hydgrp, c.slope_r, c.slope_l, c.slope_h,
                c.taxorder, c.taxsubgrp,
                c.corcon, c.corsteel,
                c.tfact, c.wei, c.weg,
                c.brockdepmin
            FROM component AS c
            WHERE c.mukey = '{mukey}'
            AND c.majcompflag = 'Yes'
            ORDER BY c.comppct_r DESC
            """

            comp_df = self._query_sda(comp_sql)

            result = {
                "lat": lat,
                "lon": lon,
                "mukey": mukey,
                "soil_name": muname,
                "farmland_class": farmland,
                "source": "SSURGO",
            }

            if not comp_df.empty:
                row = comp_df.iloc[0]
                result.update({
                    "component_name": row.get("compname", ""),
                    "component_pct": row.get("comppct_r", ""),
                    "drainage_class": row.get("drainagecl", ""),
                    "hydrologic_group": row.get("hydgrp", ""),
                    "slope_pct": self._safe_float(row.get("slope_r")),
                    "slope_min": self._safe_float(row.get("slope_l")),
                    "slope_max": self._safe_float(row.get("slope_h")),
                    "depth_to_bedrock_cm": self._safe_float(row.get("brockdepmin")),
                    "corrosion_concrete": row.get("corcon", ""),
                    "corrosion_steel": row.get("corsteel", ""),
                    "t_factor": row.get("tfact", ""),
                    "wind_erodibility_index": row.get("wei", ""),
                    "wind_erodibility_group": row.get("weg", ""),
                    "tax_order": row.get("taxorder", ""),
                    "tax_subgroup": row.get("taxsubgrp", ""),
                })

            return result

        except Exception as e:
            logger.warning(f"  SSURGO query failed at ({lat}, {lon}): {e}")
            return self._default_soil(lat, lon)

    def _default_soil(self, lat: float, lon: float) -> Dict:
        return {
            "lat": lat,
            "lon": lon,
            "soil_name": "Unknown",
            "source": "default",
            "bess_score": 50,
        }

    @staticmethod
    def _safe_float(val) -> Optional[float]:
        if val is None:
            return None
        try:
            return float(val)
        except (ValueError, TypeError):
            return None

    def get_soil_suitability(
        self,
        lat: float,
        lon: float,
    ) -> Dict:
        """
        Score soil suitability for BESS development at a location.

        Returns score 0-100 based on:
          - Drainage class (25%)
          - Slope (25%)
          - Depth to bedrock (20%)
          - Corrosion potential (15%)
          - Hydrologic group (15%)
        """
        soil = self.get_soil_at_point(lat, lon)

        if soil.get("source") == "default":
            soil["bess_score"] = 50
            soil["score_breakdown"] = {}
            return soil

        scores = {}

        # Drainage (25%)
        drainage = soil.get("drainage_class", "")
        drainage_scores = {
            "Excessively drained": 95,
            "Somewhat excessively drained": 90,
            "Well drained": 85,
            "Moderately well drained": 70,
            "Somewhat poorly drained": 40,
            "Poorly drained": 20,
            "Very poorly drained": 5,
        }
        scores["drainage"] = drainage_scores.get(drainage, 50) * 0.25

        # Slope (25%)
        slope = soil.get("slope_pct")
        if slope is not None:
            if slope <= 3:
                scores["slope"] = 95 * 0.25
            elif slope <= 5:
                scores["slope"] = 80 * 0.25
            elif slope <= 10:
                scores["slope"] = 55 * 0.25
            elif slope <= 15:
                scores["slope"] = 30 * 0.25
            else:
                scores["slope"] = 10 * 0.25
        else:
            scores["slope"] = 50 * 0.25

        # Depth to bedrock (20%)
        bedrock = soil.get("depth_to_bedrock_cm")
        if bedrock is not None:
            if bedrock >= 200:
                scores["bedrock"] = 95 * 0.20
            elif bedrock >= 100:
                scores["bedrock"] = 75 * 0.20
            elif bedrock >= 50:
                scores["bedrock"] = 45 * 0.20
            else:
                scores["bedrock"] = 15 * 0.20
        else:
            scores["bedrock"] = 50 * 0.20

        # Corrosion (15%)
        corrosion = soil.get("corrosion_concrete", "")
        corr_scores = {
            "Low": 90, "Moderate": 60, "High": 25,
        }
        scores["corrosion"] = corr_scores.get(corrosion, 50) * 0.15

        # Hydrologic group (15%)
        hydro = soil.get("hydrologic_group", "")
        hydro_scores = {
            "A": 95, "B": 80, "A/D": 70, "B/D": 55,
            "C": 45, "C/D": 30, "D": 15,
        }
        scores["hydrologic"] = hydro_scores.get(hydro, 50) * 0.15

        total_score = sum(scores.values())

        # Determine tier
        if total_score >= 75:
            tier = "Excellent"
        elif total_score >= 55:
            tier = "Good"
        elif total_score >= 35:
            tier = "Marginal"
        else:
            tier = "Poor"

        soil["bess_score"] = round(total_score, 1)
        soil["score_tier"] = tier
        soil["score_breakdown"] = {k: round(v, 1) for k, v in scores.items()}

        return soil

    def get_earthquake_risk(
        self,
        lat: float,
        lon: float,
    ) -> Dict:
        """
        Get seismic design parameters from USGS for ASCE 7 compliance.
        Returns spectral acceleration values for structural design.
        """
        try:
            params = {
                "latitude": lat,
                "longitude": lon,
                "riskCategory": "III",  # Essential facilities
                "siteClass": "D",  # Default site class
                "title": "BESS Site Scout",
            }

            data = self.client.get(
                USGS_EARTHQUAKE_SERVICE,
                params=params,
                cache_hours=self.cache_hours,
            )

            response_data = data.get("response", {}).get("data", {})
            if not response_data:
                return {"lat": lat, "lon": lon, "seismic_data": "unavailable"}

            return {
                "lat": lat,
                "lon": lon,
                "ss": response_data.get("ss"),  # Short-period spectral acceleration
                "s1": response_data.get("s1"),  # 1-second spectral acceleration
                "sds": response_data.get("sds"),  # Design spectral acceleration (short)
                "sd1": response_data.get("sd1"),  # Design spectral acceleration (1s)
                "pga": response_data.get("pga"),  # Peak ground acceleration
                "seismic_category": self._classify_seismic(
                    response_data.get("sds"), response_data.get("sd1")
                ),
            }

        except Exception as e:
            logger.debug(f"  USGS earthquake data failed: {e}")
            return {"lat": lat, "lon": lon, "seismic_data": "unavailable"}

    @staticmethod
    def _classify_seismic(sds, sd1) -> str:
        """Classify seismic design category from ASCE 7 parameters."""
        if sds is None:
            return "Unknown"
        try:
            sds = float(sds)
        except (ValueError, TypeError):
            return "Unknown"

        if sds < 0.167:
            return "A (Very Low)"
        elif sds < 0.33:
            return "B (Low)"
        elif sds < 0.50:
            return "C (Moderate)"
        elif sds < 0.75:
            return "D (High)"
        else:
            return "E (Very High)"

    def get_soil_summary(self, sites: list) -> Dict:
        """
        Generate soil summary for multiple candidate sites.

        Args:
            sites: List of dicts with 'lat' and 'lon' keys
        """
        results = []
        for site in sites[:50]:  # Limit for API courtesy
            lat = site.get("lat") or site.get("latitude")
            lon = site.get("lon") or site.get("longitude")
            if lat and lon:
                score = self.get_soil_suitability(lat, lon)
                results.append(score)

        if not results:
            return {"total_sites": 0}

        df = pd.DataFrame(results)
        summary = {
            "total_sites": len(results),
            "avg_soil_score": round(df["bess_score"].mean(), 1),
        }

        if "score_tier" in df.columns:
            tier_counts = df["score_tier"].value_counts().to_dict()
            summary["tier_distribution"] = tier_counts

        if "drainage_class" in df.columns:
            drain_counts = df["drainage_class"].value_counts().to_dict()
            summary["drainage_distribution"] = drain_counts

        if "hydrologic_group" in df.columns:
            hydro_counts = df["hydrologic_group"].value_counts().to_dict()
            summary["hydrologic_group_distribution"] = hydro_counts

        return summary
