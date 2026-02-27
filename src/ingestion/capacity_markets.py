"""
Capacity Market Price Ingestion

Pulls capacity auction results and pricing from:
  1. PJM RPM (Reliability Pricing Model) — Base Residual Auction results
  2. NYISO ICAP (Installed Capacity) — monthly/strip auction results
  3. ISO-NE FCM (Forward Capacity Market) — annual auction results
  4. ERCOT — no capacity market (energy-only); uses ORDC adder data
  5. MISO — Planning Resource Auction (PRA) results
  6. CAISO — Resource Adequacy (RA) bilateral market data

Capacity payments ($/MW-day) are a major BESS revenue stream,
especially in PJM, NYISO, and ISO-NE where capacity markets are
most established.
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

# Known capacity auction clearing prices ($/MW-day)
# Updated manually from public ISO filings — these change annually
# Sources: PJM RPM BRA results, NYISO ICAP, ISO-NE FCM
CAPACITY_PRICES_REFERENCE = {
    "PJM": {
        "auction_type": "RPM Base Residual Auction",
        "delivery_years": {
            "2025/2026": {
                "RTO_avg": 28.92,
                "MAAC": 49.49,
                "EMAAC": 54.95,
                "SWMAAC": 30.68,
                "ATSI": 28.92,
                "ComEd": 28.92,
                "BGE": 30.68,
                "DPL_South": 30.68,
                "PEPCO": 30.68,
                "units": "$/MW-day",
                "note": "2025/2026 BRA cleared Feb 2024",
            },
            "2026/2027": {
                "RTO_avg": 269.92,
                "EMAAC": 269.92,
                "MAAC": 269.92,
                "units": "$/MW-day",
                "note": "2026/2027 BRA — significant price increase",
            },
        },
    },
    "NYISO": {
        "auction_type": "ICAP Monthly/Strip",
        "zones": {
            "NYC_Zone_J": {
                "summer_2025": 13.50,
                "winter_2025": 5.50,
                "units": "$/kW-month",
                "note": "Zone J (NYC) commands highest premiums",
            },
            "LI_Zone_K": {
                "summer_2025": 11.00,
                "winter_2025": 4.50,
                "units": "$/kW-month",
            },
            "Rest_of_State": {
                "summer_2025": 3.50,
                "winter_2025": 1.50,
                "units": "$/kW-month",
            },
        },
    },
    "ISONE": {
        "auction_type": "Forward Capacity Market (FCM)",
        "auctions": {
            "FCA_18": {
                "commitment_period": "2027-2028",
                "clearing_price": 3.58,
                "units": "$/kW-month",
                "note": "FCA 18 cleared Feb 2024",
            },
            "FCA_17": {
                "commitment_period": "2026-2027",
                "clearing_price": 3.24,
                "units": "$/kW-month",
            },
        },
    },
    "MISO": {
        "auction_type": "Planning Resource Auction (PRA)",
        "zones": {
            "Zone_1": {"2025": 30.00, "units": "$/MW-day"},
            "Zone_2": {"2025": 30.00, "units": "$/MW-day"},
            "Zone_3": {"2025": 30.00, "units": "$/MW-day"},
            "Zone_4": {"2025": 72.94, "units": "$/MW-day"},
            "Zone_5": {"2025": 30.00, "units": "$/MW-day"},
            "Zone_6": {"2025": 30.00, "units": "$/MW-day"},
            "Zone_7": {"2025": 30.00, "units": "$/MW-day"},
            "Zone_8": {"2025": 72.94, "units": "$/MW-day"},
            "Zone_9": {"2025": 72.94, "units": "$/MW-day"},
            "Zone_10": {"2025": 72.94, "units": "$/MW-day"},
        },
    },
    "ERCOT": {
        "auction_type": "Energy-Only (no capacity market)",
        "mechanism": "ORDC (Operating Reserve Demand Curve)",
        "note": (
            "ERCOT uses ORDC adder instead of capacity market. "
            "Revenue captured through scarcity pricing during peaks."
        ),
        "avg_ordc_adder_2024": 2.50,
        "units": "$/MWh (adder to energy price)",
    },
    "CAISO": {
        "auction_type": "Resource Adequacy (bilateral)",
        "mechanism": "RA bilateral contracts + CPUC mandates",
        "note": (
            "CAISO uses bilateral RA contracts mandated by CPUC. "
            "No centralized capacity auction. Prices negotiated privately."
        ),
        "estimated_ra_value_2025": {
            "system_avg": 8.50,
            "local_avg": 12.00,
            "units": "$/kW-month",
        },
    },
    "SPP": {
        "auction_type": "No formal capacity market",
        "note": "SPP uses bilateral arrangements and resource adequacy requirements.",
    },
}


class CapacityMarketIngestor:
    """
    Ingests capacity market pricing data from US ISOs.

    Provides:
      - get_capacity_prices(): Reference capacity prices by ISO/zone
      - get_pjm_rpm_data(): PJM RPM auction results (via Data Miner 2)
      - get_annual_capacity_revenue(): Estimated annual capacity revenue per MW
      - get_capacity_summary(): Summary for pipeline scoring
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

    def get_capacity_prices(self) -> Dict:
        """
        Return reference capacity prices across all ISOs.
        This is the primary lookup for BESS revenue modeling.
        """
        return CAPACITY_PRICES_REFERENCE

    def get_pjm_rpm_data(self) -> pd.DataFrame:
        """
        Fetch PJM RPM auction data from Data Miner 2.
        Returns zone-level capacity clearing prices.
        """
        pjm_key = self.api_keys.get("pjm", "")
        if not pjm_key:
            logger.info("  PJM RPM: No API key — using reference prices only")
            # Return reference data as DataFrame
            rows = []
            for dy, zones in CAPACITY_PRICES_REFERENCE["PJM"]["delivery_years"].items():
                for zone, price in zones.items():
                    if zone in ("units", "note"):
                        continue
                    rows.append({
                        "delivery_year": dy,
                        "zone": zone,
                        "clearing_price_mw_day": price,
                        "iso": "PJM",
                    })
            return pd.DataFrame(rows)

        try:
            url = "https://dataminer2.pjm.com/feed/rpm_auction_results"
            headers = {"Ocp-Apim-Subscription-Key": pjm_key}
            params = {"rowCount": 5000}

            response = self.client.session.get(
                url, params=params, headers=headers, timeout=120
            )
            response.raise_for_status()
            data = response.json()

            records = data if isinstance(data, list) else data.get("items", [])
            if records:
                df = pd.DataFrame(records)
                df["iso"] = "PJM"
                logger.info(f"  PJM RPM: {len(df)} auction result records")
                return df

        except Exception as e:
            logger.warning(f"  PJM RPM data fetch failed: {e}")

        # Fallback to reference
        rows = []
        for dy, zones in CAPACITY_PRICES_REFERENCE["PJM"]["delivery_years"].items():
            for zone, price in zones.items():
                if zone in ("units", "note"):
                    continue
                rows.append({
                    "delivery_year": dy,
                    "zone": zone,
                    "clearing_price_mw_day": price,
                    "iso": "PJM",
                })
        return pd.DataFrame(rows)

    def get_annual_capacity_revenue(self, iso: str, zone: Optional[str] = None) -> Dict:
        """
        Estimate annual capacity revenue for 1 MW of BESS in a given ISO/zone.

        Returns dict with:
          - capacity_price: $/MW-day or $/kW-month
          - annual_revenue_per_mw: Estimated annual $ per MW installed
          - price_trend: Direction (increasing/stable/decreasing)
        """
        iso_upper = iso.upper()
        ref = CAPACITY_PRICES_REFERENCE.get(iso_upper, {})

        if not ref:
            return {"iso": iso_upper, "has_capacity_market": False}

        result = {
            "iso": iso_upper,
            "auction_type": ref.get("auction_type", "Unknown"),
            "has_capacity_market": iso_upper in ("PJM", "NYISO", "ISONE", "MISO"),
        }

        if iso_upper == "PJM":
            # Use most recent delivery year
            latest = list(ref.get("delivery_years", {}).values())
            if latest:
                dy = latest[-1]
                price = dy.get("RTO_avg", 0)
                if zone and zone in dy:
                    price = dy[zone]
                result["capacity_price_mw_day"] = price
                result["annual_revenue_per_mw"] = round(price * 365, 0)
                result["units"] = "$/MW-day"

        elif iso_upper == "NYISO":
            zones_data = ref.get("zones", {})
            target_zone = zone or "Rest_of_State"
            zd = zones_data.get(target_zone, {})
            summer = zd.get("summer_2025", 0)
            winter = zd.get("winter_2025", 0)
            # Average monthly * 12 months * 1000 (kW to MW)
            avg_monthly = (summer + winter) / 2
            result["capacity_price_kw_month"] = avg_monthly
            result["annual_revenue_per_mw"] = round(avg_monthly * 12 * 1000, 0)
            result["units"] = "$/kW-month"

        elif iso_upper == "ISONE":
            auctions = ref.get("auctions", {})
            latest = list(auctions.values())[-1] if auctions else {}
            price = latest.get("clearing_price", 0)
            result["capacity_price_kw_month"] = price
            result["annual_revenue_per_mw"] = round(price * 12 * 1000, 0)
            result["units"] = "$/kW-month"

        elif iso_upper == "MISO":
            zones_data = ref.get("zones", {})
            target_zone = zone or "Zone_1"
            zd = zones_data.get(target_zone, {})
            price = zd.get("2025", 0)
            result["capacity_price_mw_day"] = price
            result["annual_revenue_per_mw"] = round(price * 365, 0)
            result["units"] = "$/MW-day"

        elif iso_upper == "ERCOT":
            result["mechanism"] = "ORDC"
            result["note"] = ref.get("note", "")
            result["avg_ordc_adder"] = ref.get("avg_ordc_adder_2024", 0)

        elif iso_upper == "CAISO":
            ra = ref.get("estimated_ra_value_2025", {})
            price = ra.get("system_avg", 0)
            result["capacity_price_kw_month"] = price
            result["annual_revenue_per_mw"] = round(price * 12 * 1000, 0)
            result["mechanism"] = "bilateral RA"
            result["units"] = "$/kW-month"

        return result

    def get_capacity_summary(self) -> Dict:
        """Generate capacity market summary for pipeline output."""
        summary = {
            "isos_with_capacity_markets": ["PJM", "NYISO", "ISONE", "MISO"],
            "isos_energy_only": ["ERCOT", "CAISO", "SPP"],
            "annual_revenue_estimates": {},
        }

        for iso in ["PJM", "NYISO", "ISONE", "MISO", "ERCOT", "CAISO"]:
            rev = self.get_annual_capacity_revenue(iso)
            summary["annual_revenue_estimates"][iso] = rev

        # Rank by revenue potential
        ranked = sorted(
            [
                (iso, data.get("annual_revenue_per_mw", 0))
                for iso, data in summary["annual_revenue_estimates"].items()
            ],
            key=lambda x: x[1],
            reverse=True,
        )
        summary["revenue_ranking"] = [
            {"iso": iso, "annual_$/MW": rev} for iso, rev in ranked
        ]

        return summary
