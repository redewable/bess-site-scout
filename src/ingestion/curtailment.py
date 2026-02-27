"""
Renewable Curtailment Data Ingestion

Pulls wind/solar curtailment data from:
  1. CAISO — daily curtailment reports (highest curtailment in US)
  2. ERCOT — wind/solar generation vs HSL (calculate curtailment)
  3. EIA — state-level renewable generation for penetration rates
  4. SPP — wind curtailment reports

High curtailment areas = BESS opportunity:
  - BESS absorbs overgeneration during curtailment events
  - Charges at near-zero or negative prices during curtailment
  - Discharges during peak demand at premium prices
  - The bigger the curtailment problem, the more valuable BESS becomes
"""

import logging
import io
from datetime import datetime, timedelta
from typing import Optional, List, Dict
from pathlib import Path

import pandas as pd
import numpy as np

from ..utils.api_client import APIClient

logger = logging.getLogger(__name__)

# ── Endpoints ─────────────────────────────────────────────────────
CAISO_OASIS_BASE = "https://oasis.caiso.com/oasisapi/SingleZip"
CAISO_CURTAILMENT_BASE = "http://www.caiso.com/outlook/SP/History"
ERCOT_DATA_BASE = "https://www.ercot.com/content/cdr/html"
MISO_MARKET_BASE = "https://docs.misoenergy.org/marketreports"

# ── Reference curtailment data ────────────────────────────────────
# Annual curtailment by ISO (GWh) — from public reports
CURTAILMENT_REFERENCE = {
    "CAISO": {
        "2023": {"solar_gwh": 2436, "wind_gwh": 184, "total_gwh": 2620},
        "2024": {"solar_gwh": 2800, "wind_gwh": 200, "total_gwh": 3000},
        "trend": "increasing",
        "peak_hours": "10am-3pm (solar duck curve)",
        "primary_cause": "Solar overgeneration exceeding demand + exports",
        "bess_value": "Critical — CAISO has highest curtailment in US",
    },
    "ERCOT": {
        "2023": {"solar_gwh": 350, "wind_gwh": 1200, "total_gwh": 1550},
        "2024": {"solar_gwh": 500, "wind_gwh": 1400, "total_gwh": 1900},
        "trend": "increasing rapidly",
        "peak_hours": "Overnight (wind) + midday (solar)",
        "primary_cause": "Wind overgeneration + transmission constraints",
        "bess_value": "High — growing curtailment, energy-only market amplifies value",
    },
    "SPP": {
        "2023": {"wind_gwh": 800, "total_gwh": 800},
        "2024": {"wind_gwh": 950, "total_gwh": 950},
        "trend": "increasing",
        "peak_hours": "Overnight/early morning (wind)",
        "primary_cause": "Wind generation exceeding load + export limits",
        "bess_value": "Moderate — less developed but growing",
    },
    "MISO": {
        "2023": {"wind_gwh": 600, "total_gwh": 600},
        "2024": {"wind_gwh": 700, "total_gwh": 700},
        "trend": "stable to increasing",
        "peak_hours": "Overnight (wind)",
        "primary_cause": "Wind congestion in northern MISO",
        "bess_value": "Moderate",
    },
    "PJM": {
        "2023": {"total_gwh": 150},
        "2024": {"total_gwh": 180},
        "trend": "low but increasing",
        "bess_value": "Lower priority — less curtailment currently",
    },
    "NYISO": {
        "2023": {"total_gwh": 80},
        "2024": {"total_gwh": 100},
        "trend": "increasing with offshore wind buildout",
        "bess_value": "Growing — offshore wind will increase curtailment",
    },
}


class CurtailmentIngestor:
    """
    Ingests renewable curtailment data from US ISOs.

    Provides:
      - get_curtailment_reference(): Reference curtailment data
      - get_caiso_curtailment(): CAISO curtailment from generation data
      - get_ercot_curtailment(): ERCOT curtailment from HSL vs actual
      - get_all_curtailment(): Combined curtailment summary
      - get_curtailment_summary(): Summary for pipeline scoring
      - score_curtailment_opportunity(): Score a location for BESS value
    """

    def __init__(self, config: dict):
        self.config = config
        self.api_keys = config.get("api_keys", {})
        self.client = APIClient(
            cache_dir=config.get("cache", {}).get("directory", "./data/cache"),
            cache_enabled=config.get("cache", {}).get("enabled", True),
        )
        self.cache_dir = Path(
            config.get("cache", {}).get("directory", "./data/cache")
        )
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self._curtailment_data = {}

    # ── CAISO Curtailment ─────────────────────────────────────────

    def get_caiso_curtailment(self, days_back: int = 30) -> pd.DataFrame:
        """
        Fetch CAISO renewable curtailment from OASIS generation data.
        CAISO publishes curtailment in its daily renewable watch reports.
        """
        try:
            # Use CAISO OASIS to get renewable generation and curtailment
            end_dt = datetime.now()
            start_dt = end_dt - timedelta(days=min(days_back, 31))

            params = {
                "resultformat": "6",
                "queryname": "SLD_REN_FCST",  # Renewable forecast vs actual
                "version": "1",
                "startdatetime": start_dt.strftime("%Y%m%dT07:00-0000"),
                "enddatetime": end_dt.strftime("%Y%m%dT07:00-0000"),
                "market_run_id": "ACTUAL",
            }

            cache_file = self.cache_dir / f"caiso_curtailment_{days_back}d.csv"
            import time
            if cache_file.exists():
                age = (time.time() - cache_file.stat().st_mtime) / 3600
                if age < 24:
                    df = pd.read_csv(cache_file)
                    df["iso"] = "CAISO"
                    return df

            import zipfile
            logger.info("  CAISO: fetching renewable curtailment data...")
            response = self.client.session.get(
                CAISO_OASIS_BASE, params=params, timeout=120
            )
            response.raise_for_status()

            z = zipfile.ZipFile(io.BytesIO(response.content))
            with z.open(z.namelist()[0]) as f:
                df = pd.read_csv(f)

            df.to_csv(cache_file, index=False)
            df["iso"] = "CAISO"
            logger.info(f"  CAISO curtailment: {len(df)} records")
            self._curtailment_data["CAISO"] = df
            return df

        except Exception as e:
            logger.warning(f"  CAISO curtailment fetch failed: {e}")
            return pd.DataFrame()

    # ── ERCOT Curtailment ─────────────────────────────────────────

    def get_ercot_curtailment(self, days_back: int = 30) -> pd.DataFrame:
        """
        Fetch ERCOT wind/solar generation data.
        Curtailment = HSL (High Sustained Limit) - Actual Generation.
        ERCOT posts hourly wind/solar reports.
        """
        try:
            frames = []
            end_date = datetime.now()

            for i in range(min(days_back, 30)):
                dt = end_date - timedelta(days=i)
                date_str = dt.strftime("%Y%m%d")

                # ERCOT wind generation report
                url = f"{ERCOT_DATA_BASE}/{date_str}_wind_gen.csv"

                cache_file = self.cache_dir / f"ercot_wind_{date_str}.csv"
                if cache_file.exists():
                    try:
                        frames.append(pd.read_csv(cache_file))
                        continue
                    except Exception:
                        pass

                try:
                    response = self.client.session.get(url, timeout=30)
                    if response.status_code == 200:
                        df = pd.read_csv(io.StringIO(response.text))
                        df["date"] = date_str
                        df.to_csv(cache_file, index=False)
                        frames.append(df)
                except Exception:
                    continue

            if not frames:
                logger.warning("  ERCOT: no wind generation data retrieved")
                return pd.DataFrame()

            df = pd.concat(frames, ignore_index=True)

            # Calculate curtailment if HSL columns present
            hsl_col = None
            gen_col = None
            for c in df.columns:
                cl = c.lower()
                if "hsl" in cl or "high sustained" in cl or "capacity" in cl:
                    hsl_col = c
                if "actual" in cl or "generation" in cl or "output" in cl:
                    gen_col = c

            if hsl_col and gen_col:
                df["curtailed_mw"] = (
                    pd.to_numeric(df[hsl_col], errors="coerce")
                    - pd.to_numeric(df[gen_col], errors="coerce")
                ).clip(lower=0)

            df["iso"] = "ERCOT"
            logger.info(f"  ERCOT wind generation: {len(df)} records")
            self._curtailment_data["ERCOT"] = df
            return df

        except Exception as e:
            logger.warning(f"  ERCOT curtailment fetch failed: {e}")
            return pd.DataFrame()

    # ── Public interface ──────────────────────────────────────────

    def get_curtailment_reference(self) -> Dict:
        """Return reference curtailment data by ISO."""
        return CURTAILMENT_REFERENCE

    def get_all_curtailment(self, days_back: int = 30) -> Dict:
        """Fetch curtailment data from all ISOs where available."""
        results = {}

        caiso = self.get_caiso_curtailment(days_back=days_back)
        if not caiso.empty:
            results["CAISO"] = caiso

        ercot = self.get_ercot_curtailment(days_back=days_back)
        if not ercot.empty:
            results["ERCOT"] = ercot

        return results

    def score_curtailment_opportunity(self, iso: str) -> Dict:
        """
        Score curtailment-based BESS opportunity for an ISO.

        Returns:
          - curtailment_score: 0-100 (higher = more BESS opportunity)
          - annual_gwh: Estimated annual curtailment
          - trend: increasing/stable/decreasing
          - revenue_potential: qualitative assessment
        """
        iso_upper = iso.upper()
        ref = CURTAILMENT_REFERENCE.get(iso_upper, {})

        if not ref:
            return {
                "iso": iso_upper,
                "curtailment_score": 0,
                "note": "No curtailment data available",
            }

        # Get latest year data
        latest_year = "2024"
        yearly = ref.get(latest_year, ref.get("2023", {}))
        total_gwh = yearly.get("total_gwh", 0)
        trend = ref.get("trend", "unknown")

        # Score: CAISO (highest) = 100, scale others proportionally
        # Max reference is ~3000 GWh (CAISO)
        raw_score = min(100, (total_gwh / 3000) * 100)

        # Trend bonus
        trend_bonus = {
            "increasing rapidly": 15,
            "increasing": 10,
            "stable to increasing": 5,
            "stable": 0,
            "decreasing": -10,
        }.get(trend, 0)

        score = min(100, max(0, raw_score + trend_bonus))

        return {
            "iso": iso_upper,
            "curtailment_score": round(score, 1),
            "annual_curtailment_gwh": total_gwh,
            "trend": trend,
            "peak_hours": ref.get("peak_hours", ""),
            "primary_cause": ref.get("primary_cause", ""),
            "bess_value": ref.get("bess_value", ""),
            "solar_gwh": yearly.get("solar_gwh", 0),
            "wind_gwh": yearly.get("wind_gwh", 0),
        }

    def get_curtailment_summary(self) -> Dict:
        """Generate curtailment summary for pipeline output."""
        summary = {
            "reference_data": CURTAILMENT_REFERENCE,
            "iso_scores": {},
            "live_data_isos": list(self._curtailment_data.keys()),
        }

        for iso in CURTAILMENT_REFERENCE:
            summary["iso_scores"][iso] = self.score_curtailment_opportunity(iso)

        # Rank by opportunity
        ranked = sorted(
            summary["iso_scores"].items(),
            key=lambda x: x[1].get("curtailment_score", 0),
            reverse=True,
        )
        summary["opportunity_ranking"] = [
            {"iso": iso, "score": data["curtailment_score"]}
            for iso, data in ranked
        ]

        return summary
