"""
Ancillary Services Market Data Ingestion

Pulls frequency regulation, spinning reserve, and other ancillary
services pricing from US ISOs:
  1. CAISO OASIS — AS market prices (Reg Up, Reg Down, Spin, Non-Spin)
  2. PJM Data Miner 2 — regulation and reserve market results
  3. ERCOT — responsive/regulating reserve data
  4. MISO — regulation and reserve reports
  5. NYISO — ancillary services pricing
  6. SPP — reserve market data

BESS excels at frequency regulation (fast response) and spinning
reserves. These markets provide additional revenue stacking on top
of energy arbitrage and capacity payments.
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
PJM_DATAMINER_BASE = "https://dataminer2.pjm.com/feed"
MISO_MARKET_BASE = "https://docs.misoenergy.org/marketreports"
ERCOT_DATA_BASE = "https://www.ercot.com/content/cdr/html"

# ── AS product definitions ────────────────────────────────────────
AS_PRODUCTS = {
    "reg_up": {
        "name": "Regulation Up",
        "description": "Fast upward frequency response",
        "bess_suitability": "Excellent — BESS ramp rate is ideal",
    },
    "reg_down": {
        "name": "Regulation Down",
        "description": "Fast downward frequency response",
        "bess_suitability": "Excellent — BESS can absorb power instantly",
    },
    "spin": {
        "name": "Spinning Reserve",
        "description": "Online capacity ready in 10 minutes",
        "bess_suitability": "Good — instant response advantage",
    },
    "non_spin": {
        "name": "Non-Spinning Reserve",
        "description": "Offline capacity available in 30 minutes",
        "bess_suitability": "Good — fast start advantage over gas CTs",
    },
    "rrs": {
        "name": "Responsive Reserve Service (ERCOT)",
        "description": "Primary frequency response within 1 second",
        "bess_suitability": "Excellent — BESS fastest responder",
    },
    "ecrs": {
        "name": "ERCOT Contingency Reserve Service",
        "description": "10-minute contingency reserve",
        "bess_suitability": "Good — fast ramp advantage",
    },
}

# Reference AS prices by ISO ($/MW, recent averages)
# These are updated from public market reports
AS_REFERENCE_PRICES = {
    "CAISO": {
        "reg_up": {"avg_price": 15.00, "peak_price": 75.00, "units": "$/MW"},
        "reg_down": {"avg_price": 8.00, "peak_price": 35.00, "units": "$/MW"},
        "spin": {"avg_price": 6.00, "peak_price": 45.00, "units": "$/MW"},
        "non_spin": {"avg_price": 3.00, "peak_price": 25.00, "units": "$/MW"},
    },
    "ERCOT": {
        "reg_up": {"avg_price": 12.00, "peak_price": 100.00, "units": "$/MW"},
        "reg_down": {"avg_price": 5.00, "peak_price": 40.00, "units": "$/MW"},
        "rrs": {"avg_price": 8.00, "peak_price": 60.00, "units": "$/MW"},
        "ecrs": {"avg_price": 4.00, "peak_price": 30.00, "units": "$/MW"},
    },
    "PJM": {
        "reg_up": {"avg_price": 18.00, "peak_price": 90.00, "units": "$/MW"},
        "reg_down": {"avg_price": 10.00, "peak_price": 50.00, "units": "$/MW"},
        "spin": {"avg_price": 5.00, "peak_price": 35.00, "units": "$/MW"},
        "note": "PJM uses performance-based regulation (mileage)",
    },
    "MISO": {
        "reg_up": {"avg_price": 10.00, "peak_price": 50.00, "units": "$/MW"},
        "reg_down": {"avg_price": 6.00, "peak_price": 30.00, "units": "$/MW"},
        "spin": {"avg_price": 4.00, "peak_price": 25.00, "units": "$/MW"},
    },
    "NYISO": {
        "reg_up": {"avg_price": 14.00, "peak_price": 60.00, "units": "$/MW"},
        "reg_down": {"avg_price": 7.00, "peak_price": 35.00, "units": "$/MW"},
        "spin": {"avg_price": 5.00, "peak_price": 30.00, "units": "$/MW"},
    },
    "SPP": {
        "reg_up": {"avg_price": 8.00, "peak_price": 40.00, "units": "$/MW"},
        "reg_down": {"avg_price": 4.00, "peak_price": 20.00, "units": "$/MW"},
        "spin": {"avg_price": 3.00, "peak_price": 20.00, "units": "$/MW"},
    },
}


class AncillaryServicesIngestor:
    """
    Ingests ancillary services market data from US ISOs.

    Provides:
      - get_as_prices(): Reference ancillary services prices
      - get_as_by_iso(): ISO-specific AS market data
      - get_all_as(): Combined AS data across ISOs
      - estimate_as_revenue(): Estimate annual AS revenue for BESS
      - get_as_summary(): Summary for pipeline scoring
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
        self._as_data = {}

    # ── CAISO AS Prices ───────────────────────────────────────────

    def _get_caiso_as(self, days_back: int = 7) -> pd.DataFrame:
        """Fetch CAISO ancillary services prices from OASIS."""
        try:
            end_dt = datetime.now()
            start_dt = end_dt - timedelta(days=days_back)

            params = {
                "resultformat": "6",
                "queryname": "PRC_AS",
                "version": "12",
                "startdatetime": start_dt.strftime("%Y%m%dT07:00-0000"),
                "enddatetime": end_dt.strftime("%Y%m%dT07:00-0000"),
                "market_run_id": "DAM",
                "anc_type": "ALL",
            }

            cache_file = self.cache_dir / f"caiso_as_{days_back}d.csv"
            import time
            if cache_file.exists():
                age = (time.time() - cache_file.stat().st_mtime) / 3600
                if age < 24:
                    return pd.read_csv(cache_file)

            import zipfile
            logger.info("  CAISO: fetching ancillary services prices...")
            response = self.client.session.get(
                CAISO_OASIS_BASE, params=params, timeout=120
            )
            response.raise_for_status()

            z = zipfile.ZipFile(io.BytesIO(response.content))
            with z.open(z.namelist()[0]) as f:
                df = pd.read_csv(f)

            df.to_csv(cache_file, index=False)
            df["iso"] = "CAISO"
            logger.info(f"  CAISO AS: {len(df)} records")
            return df

        except Exception as e:
            logger.warning(f"  CAISO AS fetch failed: {e}")
            return pd.DataFrame()

    # ── PJM Regulation ────────────────────────────────────────────

    def _get_pjm_as(self, days_back: int = 7) -> pd.DataFrame:
        """Fetch PJM regulation/reserve market results from Data Miner 2."""
        pjm_key = self.api_keys.get("pjm", "")
        if not pjm_key:
            logger.info("  PJM AS: No API key — using reference prices")
            return pd.DataFrame()

        try:
            end_dt = datetime.now()
            start_dt = end_dt - timedelta(days=days_back)

            headers = {"Ocp-Apim-Subscription-Key": pjm_key}
            url = f"{PJM_DATAMINER_BASE}/reserve_market_results"
            params = {
                "startRow": 1,
                "rowCount": 5000,
                "datetime_beginning_ept": start_dt.strftime("%Y-%m-%dT00:00:00"),
            }

            response = self.client.session.get(
                url, params=params, headers=headers, timeout=120
            )
            response.raise_for_status()
            data = response.json()

            records = data if isinstance(data, list) else data.get("items", [])
            if records:
                df = pd.DataFrame(records)
                df["iso"] = "PJM"
                logger.info(f"  PJM AS: {len(df)} regulation/reserve records")
                return df

        except Exception as e:
            logger.warning(f"  PJM AS fetch failed: {e}")

        return pd.DataFrame()

    # ── ERCOT AS ──────────────────────────────────────────────────

    def _get_ercot_as(self, days_back: int = 7) -> pd.DataFrame:
        """Fetch ERCOT ancillary services data from public reports."""
        try:
            frames = []
            end_date = datetime.now()

            for i in range(min(days_back, 7)):
                dt = end_date - timedelta(days=i)
                date_str = dt.strftime("%Y%m%d")

                # ERCOT posts DAM AS clearing prices
                url = f"{ERCOT_DATA_BASE}/{date_str}_dam_as.csv"

                cache_file = self.cache_dir / f"ercot_as_{date_str}.csv"
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
                        df.to_csv(cache_file, index=False)
                        frames.append(df)
                except Exception:
                    continue

            if not frames:
                return pd.DataFrame()

            df = pd.concat(frames, ignore_index=True)
            df["iso"] = "ERCOT"
            logger.info(f"  ERCOT AS: {len(df)} records")
            return df

        except Exception as e:
            logger.warning(f"  ERCOT AS fetch failed: {e}")
            return pd.DataFrame()

    # ── MISO AS ───────────────────────────────────────────────────

    def _get_miso_as(self, days_back: int = 7) -> pd.DataFrame:
        """Fetch MISO ancillary services reports."""
        try:
            frames = []
            end_date = datetime.now()

            for i in range(min(days_back, 7)):
                dt = end_date - timedelta(days=i)
                date_str = dt.strftime("%Y%m%d")

                url = f"{MISO_MARKET_BASE}/{date_str}_asm_expost_damcp.csv"

                cache_file = self.cache_dir / f"miso_as_{date_str}.csv"
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
                        df.to_csv(cache_file, index=False)
                        frames.append(df)
                except Exception:
                    continue

            if not frames:
                return pd.DataFrame()

            df = pd.concat(frames, ignore_index=True)
            df["iso"] = "MISO"
            logger.info(f"  MISO AS: {len(df)} records")
            return df

        except Exception as e:
            logger.warning(f"  MISO AS fetch failed: {e}")
            return pd.DataFrame()

    # ── Public interface ──────────────────────────────────────────

    def get_as_prices(self) -> Dict:
        """Return reference ancillary services prices by ISO."""
        return AS_REFERENCE_PRICES

    def get_as_by_iso(self, iso_name: str, days_back: int = 7) -> pd.DataFrame:
        """Fetch AS market data for a single ISO."""
        iso_upper = iso_name.upper()
        logger.info(f"Fetching {iso_upper} ancillary services ({days_back} days)...")

        methods = {
            "CAISO": lambda: self._get_caiso_as(days_back=days_back),
            "PJM": lambda: self._get_pjm_as(days_back=days_back),
            "ERCOT": lambda: self._get_ercot_as(days_back=days_back),
            "MISO": lambda: self._get_miso_as(days_back=days_back),
        }

        method = methods.get(iso_upper)
        if not method:
            logger.info(f"  {iso_upper}: using reference AS prices only")
            return pd.DataFrame()

        df = method()
        if not df.empty:
            self._as_data[iso_upper] = df
        return df

    def get_all_as(
        self,
        isos: Optional[List[str]] = None,
        days_back: int = 7,
    ) -> pd.DataFrame:
        """Fetch AS data from all ISOs."""
        if isos is None:
            isos = ["CAISO", "PJM", "ERCOT", "MISO"]

        frames = []
        for iso in isos:
            df = self.get_as_by_iso(iso, days_back=days_back)
            if not df.empty:
                frames.append(df)

        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

    def estimate_as_revenue(
        self,
        iso: str,
        capacity_mw: float = 100,
        hours_per_year: int = 8760,
        reg_pct: float = 0.30,
        spin_pct: float = 0.15,
    ) -> Dict:
        """
        Estimate annual ancillary services revenue for BESS.

        Args:
            iso: ISO name
            capacity_mw: BESS capacity in MW
            hours_per_year: Available hours (default 8760)
            reg_pct: % of time providing regulation
            spin_pct: % of time providing spinning reserve

        Returns:
            Dict with revenue estimates by AS product.
        """
        iso_upper = iso.upper()
        ref = AS_REFERENCE_PRICES.get(iso_upper, {})

        if not ref:
            return {"iso": iso_upper, "total_annual_revenue": 0}

        revenues = {}

        # Regulation revenue
        for product in ["reg_up", "reg_down"]:
            if product in ref:
                avg_price = ref[product].get("avg_price", 0)
                hours = hours_per_year * reg_pct
                rev = avg_price * capacity_mw * hours
                revenues[product] = {
                    "avg_price_mw": avg_price,
                    "hours_committed": round(hours),
                    "annual_revenue": round(rev),
                }

        # Spinning reserve
        for product in ["spin", "rrs"]:
            if product in ref:
                avg_price = ref[product].get("avg_price", 0)
                hours = hours_per_year * spin_pct
                rev = avg_price * capacity_mw * hours
                revenues[product] = {
                    "avg_price_mw": avg_price,
                    "hours_committed": round(hours),
                    "annual_revenue": round(rev),
                }

        total = sum(r.get("annual_revenue", 0) for r in revenues.values())

        return {
            "iso": iso_upper,
            "capacity_mw": capacity_mw,
            "products": revenues,
            "total_annual_revenue": round(total),
            "revenue_per_mw": round(total / capacity_mw) if capacity_mw else 0,
        }

    def get_as_summary(self) -> Dict:
        """Generate ancillary services summary for pipeline output."""
        summary = {
            "isos_with_live_data": list(self._as_data.keys()),
            "reference_prices": AS_REFERENCE_PRICES,
            "products": AS_PRODUCTS,
            "revenue_estimates": {},
        }

        for iso in AS_REFERENCE_PRICES:
            rev = self.estimate_as_revenue(iso, capacity_mw=100)
            summary["revenue_estimates"][iso] = rev

        # Rank by revenue
        ranked = sorted(
            summary["revenue_estimates"].items(),
            key=lambda x: x[1].get("total_annual_revenue", 0),
            reverse=True,
        )
        summary["best_as_markets"] = [
            {"iso": iso, "annual_$/100MW": data.get("total_annual_revenue", 0)}
            for iso, data in ranked
        ]

        return summary
