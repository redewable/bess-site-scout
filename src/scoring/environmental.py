"""
Environmental Risk Scoring

Combines results from all environmental data sources into
a composite risk score for each candidate parcel.
"""

import logging
from typing import Dict, Any

logger = logging.getLogger(__name__)


class EnvironmentalScorer:
    """Scores parcels based on environmental risk factors."""

    def __init__(self, config: dict):
        self.config = config

    def score_parcel(
        self,
        fema_results: dict,
        epa_results: dict,
        tceq_results: dict,
        usfws_results: dict,
    ) -> Dict[str, Any]:
        """
        Generate composite environmental risk score for a parcel.

        Score ranges from 0 (highest risk) to 100 (lowest risk / cleanest).

        Returns:
            {
                "score": float (0-100),
                "grade": str (A/B/C/D/F),
                "eliminate": bool,
                "risk_flags": list[str],
                "details": dict of per-source scores,
            }
        """
        score = 100.0
        all_flags = []
        eliminate = False
        details = {}

        # --- FEMA Flood Risk (max -30 points) ---
        flood_penalty = 0
        if fema_results.get("eliminate"):
            eliminate = True
            flood_penalty = 30
        elif fema_results.get("risk_level") == "high":
            flood_penalty = 25
        elif fema_results.get("risk_level") == "moderate":
            flood_penalty = 10
        elif fema_results.get("risk_level") == "undetermined":
            flood_penalty = 15

        score -= flood_penalty
        details["flood"] = {"penalty": flood_penalty, "zone": fema_results.get("flood_zone")}

        # --- EPA Federal (max -30 points) ---
        epa_penalty = 0
        if epa_results.get("eliminate"):
            eliminate = True
            epa_penalty = 30

        # Superfund
        npl_count = epa_results.get("superfund", {}).get("count", 0)
        npl_nearest = epa_results.get("superfund", {}).get("nearest_distance_mi")
        if npl_count > 0:
            if npl_nearest and npl_nearest < 0.25:
                epa_penalty = max(epa_penalty, 30)
            elif npl_nearest and npl_nearest < 0.5:
                epa_penalty = max(epa_penalty, 20)
            else:
                epa_penalty = max(epa_penalty, 10)

        # Brownfields
        bf_count = epa_results.get("brownfields", {}).get("count", 0)
        if bf_count > 0:
            epa_penalty = max(epa_penalty, min(5 * bf_count, 10))

        # TRI
        tri_count = epa_results.get("tri", {}).get("count", 0)
        if tri_count > 3:
            epa_penalty = max(epa_penalty, 10)
        elif tri_count > 0:
            epa_penalty = max(epa_penalty, 5)

        score -= min(epa_penalty, 30)
        details["epa"] = {"penalty": epa_penalty, "npl_count": npl_count, "tri_count": tri_count}

        # --- TCEQ State (max -25 points) ---
        tceq_penalty = 0

        # LPST — most impactful
        lpst_count = tceq_results.get("lpst", {}).get("count", 0)
        lpst_nearest = tceq_results.get("lpst", {}).get("nearest_distance_mi")
        if lpst_count > 0:
            if lpst_nearest and lpst_nearest < 0.1:
                tceq_penalty += 20
            elif lpst_nearest and lpst_nearest < 0.25:
                tceq_penalty += 12
            else:
                tceq_penalty += 5

        # UST
        ust_count = tceq_results.get("ust", {}).get("count", 0)
        if ust_count > 0:
            tceq_penalty += min(2 * ust_count, 5)

        # IHW
        ihw_count = tceq_results.get("ihw", {}).get("count", 0)
        if ihw_count > 0:
            tceq_penalty += min(3 * ihw_count, 8)

        # MSW / Landfills
        msw_count = tceq_results.get("msw", {}).get("count", 0)
        if msw_count > 0:
            tceq_penalty += 3

        score -= min(tceq_penalty, 25)
        details["tceq"] = {"penalty": tceq_penalty, "lpst_count": lpst_count, "ust_count": ust_count}

        # --- USFWS Wetlands & Habitat (max -15 points) ---
        usfws_penalty = 0

        if usfws_results.get("eliminate"):
            eliminate = True
            usfws_penalty = 15

        wetland_pct = usfws_results.get("wetlands", {}).get("intersection_pct", 0)
        if wetland_pct > 25:
            usfws_penalty = max(usfws_penalty, 12)
        elif wetland_pct > 10:
            usfws_penalty = max(usfws_penalty, 8)
        elif wetland_pct > 0:
            usfws_penalty = max(usfws_penalty, 4)

        if usfws_results.get("critical_habitat", {}).get("present"):
            usfws_penalty = max(usfws_penalty, 12)

        score -= min(usfws_penalty, 15)
        details["usfws"] = {
            "penalty": usfws_penalty,
            "wetland_pct": wetland_pct,
            "critical_habitat": usfws_results.get("critical_habitat", {}).get("present", False),
        }

        # --- Compile results ---
        score = max(0, round(score, 1))

        # Letter grade
        if score >= 90:
            grade = "A"
        elif score >= 75:
            grade = "B"
        elif score >= 60:
            grade = "C"
        elif score >= 40:
            grade = "D"
        else:
            grade = "F"

        # Aggregate flags
        for source in [fema_results, epa_results, tceq_results, usfws_results]:
            all_flags.extend(source.get("risk_flags", []))

        if eliminate:
            grade = "F"

        return {
            "score": score,
            "grade": grade,
            "eliminate": eliminate,
            "risk_flags": all_flags,
            "details": details,
        }
