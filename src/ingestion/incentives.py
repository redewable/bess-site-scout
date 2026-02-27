"""
State & Federal Incentives / ITC Eligibility Ingestion

Tracks energy storage incentives from:
  1. Federal ITC (Investment Tax Credit) — 30%+ for standalone BESS (IRA 2022)
  2. Federal ITC Bonus Credits — prevailing wage, domestic content, energy community
  3. State-level BESS/storage incentives (compiled from public sources)
  4. DSIRE database reference (subscription API, free web lookup)
  5. DOE/NREL Energy Communities Tax Credit Bonus

The IRA (Inflation Reduction Act) made standalone energy storage
eligible for the 30% ITC starting 2023. Additional bonuses:
  +10% prevailing wage & apprenticeship
  +10% domestic content
  +10% energy community (brownfield, coal closure, fossil fuel)
  = Up to 50% effective ITC for qualifying BESS projects
"""

import logging
from typing import Optional, Dict, List
from pathlib import Path

import pandas as pd

from ..utils.api_client import APIClient, ArcGISClient
from ..utils.geo import WGS84

logger = logging.getLogger(__name__)

# ── DOE Energy Communities layer ──────────────────────────────────
# Energy Community bonus zones (+10% ITC)
# Source: DOE Interagency Working Group
ENERGY_COMMUNITIES_ARCGIS = (
    "https://services1.arcgis.com/RbMX0mRVOFNTdLzd/arcgis/rest/services/"
    "IWG_EC_StatisticalAreas_2024v2/FeatureServer/0"
)

# Coal closure communities
COAL_CLOSURE_ARCGIS = (
    "https://services1.arcgis.com/RbMX0mRVOFNTdLzd/arcgis/rest/services/"
    "IWG_EC_CoalClosures_2024v2/FeatureServer/0"
)

# ── Federal ITC Structure (IRA / Section 48E) ────────────────────
FEDERAL_ITC = {
    "base_itc_pct": 30,
    "prevailing_wage_bonus_pct": 10,
    "domestic_content_bonus_pct": 10,
    "energy_community_bonus_pct": 10,
    "max_itc_pct": 50,
    "eligible_technologies": [
        "Battery Energy Storage (standalone)",
        "Battery Energy Storage (co-located with solar/wind)",
        "Pumped Hydro Storage",
        "Compressed Air Energy Storage",
        "Hydrogen Storage",
        "Thermal Energy Storage",
        "Flywheel Storage",
    ],
    "minimum_capacity_kwh": 5,  # 5 kWh minimum
    "placed_in_service_after": "2023-01-01",
    "notes": (
        "IRA Section 48E makes standalone BESS eligible for ITC. "
        "Base rate is 6% (small projects) or 30% (prevailing wage). "
        "Additional bonuses for domestic content, energy communities."
    ),
}

# ── State Energy Storage Incentives ──────────────────────────────
# Compiled from public state energy office publications and DSIRE
STATE_INCENTIVES = {
    "CA": {
        "name": "California",
        "programs": [
            {
                "program": "Self-Generation Incentive Program (SGIP)",
                "type": "Rebate",
                "value": "$150-$1,000/kWh",
                "status": "Active",
                "note": "Largest state storage incentive; equity resiliency budget available",
                "url": "https://www.cpuc.ca.gov/sgip/",
            },
            {
                "program": "Energy Storage Mandate (AB 2514)",
                "type": "Mandate",
                "value": "1,325 MW by 2024 (exceeded)",
                "status": "Active",
                "note": "Utilities required to procure energy storage",
            },
            {
                "program": "Resource Adequacy",
                "type": "Market",
                "value": "~$8-12/kW-month",
                "status": "Active",
                "note": "RA value supports BESS offtake contracts",
            },
        ],
        "overall_rating": "Excellent",
        "score": 95,
    },
    "NY": {
        "name": "New York",
        "programs": [
            {
                "program": "NY-SUN + Storage",
                "type": "Rebate",
                "value": "$200-$350/kWh",
                "status": "Active",
                "note": "NYSERDA bulk storage incentive",
                "url": "https://www.nyserda.ny.gov/",
            },
            {
                "program": "6 GW Storage Goal by 2030",
                "type": "Mandate/Goal",
                "value": "6,000 MW target",
                "status": "Active",
                "note": "Strongest state storage target in US",
            },
            {
                "program": "Value of DER (VDER)",
                "type": "Tariff",
                "value": "Location-based",
                "status": "Active",
                "note": "Value stack compensation for distributed storage",
            },
        ],
        "overall_rating": "Excellent",
        "score": 90,
    },
    "TX": {
        "name": "Texas",
        "programs": [
            {
                "program": "No state-level storage incentive",
                "type": "None",
                "value": "N/A",
                "status": "N/A",
                "note": "Texas relies on market signals (ERCOT energy-only)",
            },
            {
                "program": "Property Tax Exemption (Ch. 313 successor)",
                "type": "Tax Abatement",
                "value": "Up to 10 years property tax limitation",
                "status": "Active (varies by county)",
                "note": "County-level tax abatements for energy projects",
            },
        ],
        "overall_rating": "Moderate",
        "score": 55,
    },
    "MA": {
        "name": "Massachusetts",
        "programs": [
            {
                "program": "SMART Program + Storage Adder",
                "type": "Adder",
                "value": "$0.045-0.065/kWh",
                "status": "Active",
                "note": "Storage adder on top of solar SMART tariff",
            },
            {
                "program": "Clean Peak Energy Standard",
                "type": "REC Market",
                "value": "~$10-20/MWh CPEC",
                "status": "Active",
                "note": "Clean Peak Energy Certificates for storage dispatch",
            },
        ],
        "overall_rating": "Good",
        "score": 75,
    },
    "NJ": {
        "name": "New Jersey",
        "programs": [
            {
                "program": "NJ Storage Incentive Program",
                "type": "Incentive",
                "value": "Up to $350/kWh",
                "status": "Active",
                "note": "BPU energy storage incentive program",
            },
            {
                "program": "2 GW Storage Goal by 2030",
                "type": "Goal",
                "value": "2,000 MW target",
                "status": "Active",
            },
        ],
        "overall_rating": "Good",
        "score": 70,
    },
    "AZ": {
        "name": "Arizona",
        "programs": [
            {
                "program": "APS/TEP Storage Procurement",
                "type": "Utility RFP",
                "value": "Market-based",
                "status": "Active",
                "note": "Utilities actively procuring storage via RFPs",
            },
        ],
        "overall_rating": "Moderate",
        "score": 60,
    },
    "NV": {
        "name": "Nevada",
        "programs": [
            {
                "program": "NV Energy Storage RFPs",
                "type": "Utility RFP",
                "value": "Market-based",
                "status": "Active",
            },
            {
                "program": "1 GW Storage Goal by 2030",
                "type": "Goal",
                "value": "1,000 MW target",
                "status": "Active",
            },
        ],
        "overall_rating": "Moderate",
        "score": 60,
    },
    "VA": {
        "name": "Virginia",
        "programs": [
            {
                "program": "Virginia Clean Economy Act",
                "type": "Mandate",
                "value": "3,100 MW storage by 2035",
                "status": "Active",
                "note": "Dominion Energy required to develop storage",
            },
        ],
        "overall_rating": "Good",
        "score": 65,
    },
    "IL": {
        "name": "Illinois",
        "programs": [
            {
                "program": "IL Solar for All + Storage",
                "type": "Rebate",
                "value": "Varies",
                "status": "Active",
                "note": "CEJA legislation supports storage deployment",
            },
        ],
        "overall_rating": "Moderate",
        "score": 55,
    },
    "CO": {
        "name": "Colorado",
        "programs": [
            {
                "program": "Xcel Energy Storage RFPs",
                "type": "Utility RFP",
                "value": "Market-based",
                "status": "Active",
            },
        ],
        "overall_rating": "Moderate",
        "score": 55,
    },
}

# Default for states without specific programs
DEFAULT_STATE = {
    "programs": [],
    "overall_rating": "Low",
    "score": 30,
    "note": "No specific state storage incentive identified",
}


class IncentivesIngestor:
    """
    Tracks federal and state energy storage incentives.

    Provides:
      - get_federal_itc(): Federal ITC structure and eligibility
      - check_energy_community(): Check if location qualifies for bonus
      - get_state_incentives(): State-level incentive programs
      - get_incentive_score(): Combined incentive score for a location
      - get_incentive_summary(): Summary for pipeline output
    """

    def __init__(self, config: dict):
        self.config = config
        self.arcgis = ArcGISClient(
            cache_dir=config.get("cache", {}).get("directory", "./data/cache"),
            cache_enabled=config.get("cache", {}).get("enabled", True),
        )
        self.cache_hours = config.get("cache", {}).get("grid_data_expiry_hours", 720)

    def get_federal_itc(self) -> Dict:
        """Return the current federal ITC structure for BESS."""
        return FEDERAL_ITC

    def check_energy_community(
        self,
        lat: float,
        lon: float,
    ) -> Dict:
        """
        Check if a location qualifies as an Energy Community
        for the +10% ITC bonus.

        Energy Communities include:
          1. Brownfield sites
          2. Metropolitan/non-metropolitan areas with significant fossil fuel employment
          3. Census tracts with coal closures (mine or plant)
        """
        result = {
            "lat": lat,
            "lon": lon,
            "is_energy_community": False,
            "qualifying_criteria": [],
            "bonus_pct": 0,
        }

        # Check statistical area layer
        try:
            geojson = self.arcgis.query_features(
                service_url=ENERGY_COMMUNITIES_ARCGIS,
                where="1=1",
                out_fields="*",
                geometry=f"{lon},{lat}",
                geometry_type="esriGeometryPoint",
                spatial_rel="esriSpatialRelIntersects",
                return_geometry=False,
                cache_hours=self.cache_hours,
            )

            features = geojson.get("features", [])
            if features:
                result["is_energy_community"] = True
                result["bonus_pct"] = 10
                result["qualifying_criteria"].append("Fossil Fuel Employment Area")

                props = features[0].get("properties", {})
                for k, v in props.items():
                    if v is not None:
                        result[f"ec_{k.lower()}"] = v

        except Exception as e:
            logger.debug(f"  Energy community check failed (statistical): {e}")

        # Check coal closure layer
        try:
            geojson = self.arcgis.query_features(
                service_url=COAL_CLOSURE_ARCGIS,
                where="1=1",
                out_fields="*",
                geometry=f"{lon},{lat}",
                geometry_type="esriGeometryPoint",
                spatial_rel="esriSpatialRelIntersects",
                return_geometry=False,
                cache_hours=self.cache_hours,
            )

            features = geojson.get("features", [])
            if features:
                result["is_energy_community"] = True
                result["bonus_pct"] = 10
                if "Coal Closure Community" not in result["qualifying_criteria"]:
                    result["qualifying_criteria"].append("Coal Closure Community")

        except Exception as e:
            logger.debug(f"  Energy community check failed (coal): {e}")

        return result

    def get_state_incentives(self, state: str) -> Dict:
        """
        Get state-level energy storage incentive programs.

        Args:
            state: Two-letter state code (e.g., "TX", "CA")
        """
        state_upper = state.upper()
        incentives = STATE_INCENTIVES.get(state_upper, DEFAULT_STATE.copy())
        incentives["state"] = state_upper
        incentives["name"] = incentives.get("name", state_upper)
        return incentives

    def get_incentive_score(
        self,
        lat: float,
        lon: float,
        state: str,
    ) -> Dict:
        """
        Calculate combined federal + state incentive score for a location.

        Returns:
            Dict with total ITC %, state score, energy community status
        """
        # Federal ITC
        federal = FEDERAL_ITC.copy()
        base_itc = federal["base_itc_pct"]

        # Energy community check
        ec = self.check_energy_community(lat, lon)
        ec_bonus = ec.get("bonus_pct", 0)

        # Assume prevailing wage compliance (standard for utility-scale)
        pw_bonus = federal["prevailing_wage_bonus_pct"]

        # Total federal ITC (without domestic content — project-specific)
        total_federal_itc = base_itc + pw_bonus + ec_bonus

        # State incentives
        state_data = self.get_state_incentives(state)
        state_score = state_data.get("score", 30)

        # Combined score (0-100)
        # Federal ITC is worth up to 50 points, state incentives up to 50
        federal_score = min(50, (total_federal_itc / 50) * 50)
        combined_score = federal_score + (state_score / 100 * 50)

        return {
            "lat": lat,
            "lon": lon,
            "state": state.upper(),
            "federal_itc_pct": total_federal_itc,
            "base_itc": base_itc,
            "prevailing_wage_bonus": pw_bonus,
            "energy_community_bonus": ec_bonus,
            "domestic_content_bonus_available": federal["domestic_content_bonus_pct"],
            "is_energy_community": ec.get("is_energy_community", False),
            "energy_community_criteria": ec.get("qualifying_criteria", []),
            "state_incentive_score": state_score,
            "state_rating": state_data.get("overall_rating", "Unknown"),
            "state_programs_count": len(state_data.get("programs", [])),
            "combined_incentive_score": round(combined_score, 1),
        }

    def get_incentive_summary(self, sites: list = None) -> Dict:
        """Generate incentive summary for pipeline output."""
        summary = {
            "federal_itc": FEDERAL_ITC,
            "state_programs": {
                state: {
                    "rating": data.get("overall_rating"),
                    "score": data.get("score"),
                    "programs_count": len(data.get("programs", [])),
                }
                for state, data in STATE_INCENTIVES.items()
            },
            "top_states_for_bess": sorted(
                [
                    {"state": s, "score": d.get("score", 0)}
                    for s, d in STATE_INCENTIVES.items()
                ],
                key=lambda x: x["score"],
                reverse=True,
            )[:10],
        }

        if sites:
            site_scores = []
            for site in sites[:50]:  # Limit for performance
                lat = site.get("lat") or site.get("latitude")
                lon = site.get("lon") or site.get("longitude")
                state = site.get("state", "")
                if lat and lon and state:
                    score = self.get_incentive_score(lat, lon, state)
                    site_scores.append(score)

            if site_scores:
                df = pd.DataFrame(site_scores)
                summary["sites_analyzed"] = len(site_scores)
                summary["avg_federal_itc"] = round(
                    df["federal_itc_pct"].mean(), 1
                )
                summary["energy_community_sites"] = int(
                    df["is_energy_community"].sum()
                )
                summary["avg_combined_score"] = round(
                    df["combined_incentive_score"].mean(), 1
                )

        return summary
