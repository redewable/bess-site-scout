"""
Composite Scoring — Final Site Ranking

Combines grid proximity, environmental risk, land cost, parcel size,
flood risk, grid density, and solar resource into a single composite score.
"""

import logging
import math
from typing import Dict, Any, List

logger = logging.getLogger(__name__)


class CompositeScorer:
    """Generates final composite scores and rankings for candidate parcels."""

    def __init__(self, config: dict):
        self.config = config
        self.weights = config.get("scoring", {})
        self.ideal_acres = self.weights.get("ideal_parcel_acres", 40)

    def score_site(
        self,
        distance_to_substation_mi: float,
        substation_voltage_kv: float,
        environmental_score: float,
        environmental_eliminate: bool,
        price_per_acre: float,
        parcel_acres: float,
        flood_risk_level: str = "low",
        grid_density_score: float = 50.0,
        solar_score: float = 50.0,
    ) -> Dict[str, Any]:
        """
        Generate composite score for a candidate site.

        All sub-scores are normalized to 0-100, then weighted.
        Higher = better.
        """
        if environmental_eliminate:
            return {
                "composite_score": 0,
                "grade": "ELIMINATED",
                "sub_scores": {},
                "reason": "Environmental elimination criteria met",
            }

        sub_scores = {}

        # 1. Proximity to Substation (closer = better)
        w = self.weights.get("proximity_to_substation", 0.25)
        prox_score = max(0, 100 * math.exp(-0.5 * distance_to_substation_mi))
        sub_scores["proximity"] = {"score": round(prox_score, 1), "weight": w}

        # 2. Voltage Class (higher = better for BESS)
        w = self.weights.get("voltage_class", 0.15)
        if substation_voltage_kv >= 345:
            volt_score = 100
        elif substation_voltage_kv >= 220:
            volt_score = 75
        elif substation_voltage_kv >= 161:
            volt_score = 50
        else:
            volt_score = 25
        sub_scores["voltage"] = {"score": volt_score, "weight": w}

        # 3. Environmental Risk (already 0-100, higher = cleaner)
        w = self.weights.get("environmental_risk", 0.20)
        sub_scores["environmental"] = {"score": round(environmental_score, 1), "weight": w}

        # 4. Land Cost (lower = better)
        w = self.weights.get("land_cost", 0.10)
        max_price = self.config.get("real_estate", {}).get("max_price_per_acre", 50000)
        cost_score = max(0, 100 * (1 - price_per_acre / max_price))
        sub_scores["land_cost"] = {"score": round(cost_score, 1), "weight": w}

        # 5. Parcel Size (closer to ideal = better)
        w = self.weights.get("parcel_size", 0.05)
        size_diff = abs(parcel_acres - self.ideal_acres) / self.ideal_acres
        size_score = 100 * math.exp(-2 * size_diff ** 2)
        sub_scores["parcel_size"] = {"score": round(size_score, 1), "weight": w}

        # 6. Flood Risk
        w = self.weights.get("flood_risk", 0.05)
        flood_scores = {
            "low": 100, "moderate": 50, "undetermined": 30,
            "high": 0, "unknown": 50,
        }
        flood_score = flood_scores.get(flood_risk_level, 50)
        sub_scores["flood_risk"] = {"score": flood_score, "weight": w}

        # 7. Grid Density (more nearby generation = better interconnection)
        w = self.weights.get("grid_density", 0.10)
        sub_scores["grid_density"] = {"score": round(grid_density_score, 1), "weight": w}

        # 8. Solar Resource (higher GHI = better co-location potential)
        w = self.weights.get("solar_resource", 0.10)
        sub_scores["solar_resource"] = {"score": round(solar_score, 1), "weight": w}

        # Weighted composite
        composite = sum(s["score"] * s["weight"] for s in sub_scores.values())
        composite = round(composite, 1)

        # Grade
        if composite >= 80:
            grade = "A"
        elif composite >= 65:
            grade = "B"
        elif composite >= 50:
            grade = "C"
        elif composite >= 35:
            grade = "D"
        else:
            grade = "F"

        return {
            "composite_score": composite,
            "grade": grade,
            "sub_scores": sub_scores,
        }

    def rank_sites(self, scored_sites: List[Dict]) -> List[Dict]:
        """
        Rank a list of scored sites by composite score.
        Filters out eliminated sites and sorts descending.
        """
        viable = [s for s in scored_sites if s.get("grade") != "ELIMINATED"]
        viable.sort(key=lambda x: x.get("composite_score", 0), reverse=True)

        for i, site in enumerate(viable, 1):
            site["rank"] = i

        eliminated = [s for s in scored_sites if s.get("grade") == "ELIMINATED"]
        logger.info(
            f"Ranked {len(viable)} viable sites, {len(eliminated)} eliminated"
        )

        return viable
