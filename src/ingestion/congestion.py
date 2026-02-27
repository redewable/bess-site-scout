"""
Transmission Congestion Data Ingestion

Pulls congestion metrics from:
  1. LMP congestion component — extracted from LMP data (MCC/MLC)
  2. CAISO OASIS — binding constraints and shadow prices
  3. ERCOT — congestion revenue rights (CRR) auction data
  4. PJM — transmission constraint data via Data Miner 2
  5. MISO — binding constraint reports
  6. NYISO — TCC auction results (Transmission Congestion Contracts)

Congestion tells us which corridors are bottlenecked:
  - BESS sited behind a constraint earns PREMIUM pricing
  - High-congestion nodes = higher LMP = better BESS revenue
  - Persistent congestion = reliable revenue stream
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

# ── Endpoint constants ────────────────────────────────────────────
CAISO_OASIS_BASE = "https://oasis.caiso.com/oasisapi/SingleZip"
PJM_DATAMINER_BASE = "https://dataminer2.pjm.com/feed"
MISO_MARKET_BASE = "https://docs.misoenergy.org/marketreports"


class CongestionIngestor:
    """
    Ingests transmission congestion data from US ISOs.

    Provides:
      - get_congestion_from_lmp(): Extract congestion component from LMP data
      - get_binding_constraints(): Active transmission constraints
      - get_congestion_summary(): Summary stats for scoring
      - get_congestion_by_iso(): ISO-specific congestion data
      - get_all_congestion(): Combined congestion across ISOs
      - identify_congested_corridors(): Find persistently congested areas
    """

    def __init__(self, config: dict):
        self.config = config
        self.api_keys = config.get("api_keys", {})
        self.client = APIClient(
            cache_dir=config.get("cache", {}).get("directory", "./data/cache"),
            cache_enabled=config.get("cache", {}).get("enabled", True),
        )
        self.cache_hours = config.get("cache", {}).get("grid_data_expiry_hours", 720)
        self.cache_dir = Path(
            config.get("cache", {}).get("directory", "./data/cache")
        )
        self.cache_dir.mkdir(parents=True, exist_ok=True)

        self._congestion_data = {}

    # ── Extract congestion from LMP data ──────────────────────────

    def get_congestion_from_lmp(self, lmp_df: pd.DataFrame) -> pd.DataFrame:
        """
        Extract and analyze the congestion component from LMP data.

        The congestion component (MCC) of LMP indicates:
          - Positive MCC = node is export-constrained (generation > line capacity)
          - Negative MCC = node is import-constrained (load > inflow capacity)
          - High |MCC| = significant congestion = BESS opportunity
        """
        if lmp_df.empty:
            return pd.DataFrame()

        cong_col = None
        for c in ["congestion", "MCC", "mcc", "Marginal Cost Congestion ($/MWHr)"]:
            if c in lmp_df.columns:
                cong_col = c
                break

        if not cong_col:
            logger.info("  No congestion column in LMP data")
            return pd.DataFrame()

        df = lmp_df.copy()
        df["congestion_value"] = pd.to_numeric(df[cong_col], errors="coerce")
        df = df.dropna(subset=["congestion_value"])

        if df.empty:
            return pd.DataFrame()

        # Flag significant congestion (|MCC| > $5/MWh is notable)
        df["is_congested"] = df["congestion_value"].abs() > 5
        df["congestion_severity"] = pd.cut(
            df["congestion_value"].abs(),
            bins=[0, 2, 5, 15, 50, float("inf")],
            labels=["Minimal", "Low", "Moderate", "High", "Severe"],
        )

        return df

    # ── CAISO Binding Constraints ─────────────────────────────────

    def _get_caiso_constraints(self, days_back: int = 7) -> pd.DataFrame:
        """
        Fetch CAISO binding transmission constraints from OASIS.
        Uses PRC_CNSTR query for constraint shadow prices.
        """
        try:
            end_dt = datetime.now()
            start_dt = end_dt - timedelta(days=days_back)

            params = {
                "resultformat": "6",
                "queryname": "PRC_NOMOGRAM",  # Nomogram constraints
                "version": "1",
                "startdatetime": start_dt.strftime("%Y%m%dT07:00-0000"),
                "enddatetime": end_dt.strftime("%Y%m%dT07:00-0000"),
                "market_run_id": "DAM",
            }

            cache_file = self.cache_dir / f"caiso_constraints_{days_back}d.csv"
            import time
            if cache_file.exists():
                age_hours = (time.time() - cache_file.stat().st_mtime) / 3600
                if age_hours < 24:
                    logger.info("  CAISO constraints: using cached data")
                    return pd.read_csv(cache_file)

            import zipfile
            logger.info("  CAISO: fetching binding constraints...")
            response = self.client.session.get(
                CAISO_OASIS_BASE, params=params, timeout=120
            )
            response.raise_for_status()

            z = zipfile.ZipFile(io.BytesIO(response.content))
            csv_name = z.namelist()[0]
            with z.open(csv_name) as f:
                df = pd.read_csv(f)

            df.to_csv(cache_file, index=False)

            df["iso"] = "CAISO"
            logger.info(f"  CAISO constraints: {len(df)} records")
            return df

        except Exception as e:
            logger.warning(f"  CAISO constraint fetch failed: {e}")
            return pd.DataFrame()

    # ── CAISO Congestion Component ────────────────────────────────

    def _get_caiso_congestion(self, days_back: int = 7) -> pd.DataFrame:
        """
        Fetch CAISO congestion price component directly from OASIS.
        """
        try:
            end_dt = datetime.now()
            start_dt = end_dt - timedelta(days=days_back)

            params = {
                "resultformat": "6",
                "queryname": "PRC_LMP",
                "version": "12",
                "startdatetime": start_dt.strftime("%Y%m%dT07:00-0000"),
                "enddatetime": end_dt.strftime("%Y%m%dT07:00-0000"),
                "market_run_id": "DAM",
                "grp_type": "ALL_APNODES",
            }

            cache_file = self.cache_dir / f"caiso_cong_comp_{days_back}d.csv"
            import time
            if cache_file.exists():
                age = (time.time() - cache_file.stat().st_mtime) / 3600
                if age < 24:
                    return pd.read_csv(cache_file)

            import zipfile
            response = self.client.session.get(
                CAISO_OASIS_BASE, params=params, timeout=120
            )
            response.raise_for_status()

            z = zipfile.ZipFile(io.BytesIO(response.content))
            with z.open(z.namelist()[0]) as f:
                df = pd.read_csv(f)

            # Filter to congestion component only
            if "LMP_TYPE" in df.columns:
                cong = df[df["LMP_TYPE"] == "MCC"].copy()
            else:
                cong = df

            cong.to_csv(cache_file, index=False)
            cong["iso"] = "CAISO"
            return cong

        except Exception as e:
            logger.warning(f"  CAISO congestion component fetch failed: {e}")
            return pd.DataFrame()

    # ── PJM Binding Constraints ───────────────────────────────────

    def _get_pjm_constraints(self, days_back: int = 7) -> pd.DataFrame:
        """
        Fetch PJM transmission constraint data from Data Miner 2.
        """
        pjm_key = self.api_keys.get("pjm", "")
        if not pjm_key:
            logger.info("  PJM: No API key — skipping constraint fetch")
            return pd.DataFrame()

        try:
            end_dt = datetime.now()
            start_dt = end_dt - timedelta(days=days_back)

            headers = {"Ocp-Apim-Subscription-Key": pjm_key}
            url = f"{PJM_DATAMINER_BASE}/da_binding_constraints"
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
            if not records:
                return pd.DataFrame()

            df = pd.DataFrame(records)
            df["iso"] = "PJM"
            logger.info(f"  PJM constraints: {len(df)} binding constraint records")
            return df

        except Exception as e:
            logger.warning(f"  PJM constraint fetch failed: {e}")
            return pd.DataFrame()

    # ── MISO Binding Constraints ──────────────────────────────────

    def _get_miso_constraints(self, days_back: int = 7) -> pd.DataFrame:
        """
        Fetch MISO binding constraint reports from public market reports.
        """
        try:
            frames = []
            end_date = datetime.now()

            for i in range(min(days_back, 7)):
                dt = end_date - timedelta(days=i)
                date_str = dt.strftime("%Y%m%d")

                url = f"{MISO_MARKET_BASE}/{date_str}_da_bc.csv"

                cache_file = self.cache_dir / f"miso_bc_{date_str}.csv"
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
            logger.info(f"  MISO constraints: {len(df)} binding constraint records")
            return df

        except Exception as e:
            logger.warning(f"  MISO constraint fetch failed: {e}")
            return pd.DataFrame()

    # ── Public interface ──────────────────────────────────────────

    def get_congestion_by_iso(self, iso_name: str, days_back: int = 7) -> pd.DataFrame:
        """Fetch congestion/constraint data for a single ISO."""
        iso_upper = iso_name.upper()
        logger.info(f"Fetching {iso_upper} congestion data ({days_back} days)...")

        methods = {
            "CAISO": lambda: self._get_caiso_congestion(days_back=days_back),
            "PJM": lambda: self._get_pjm_constraints(days_back=days_back),
            "MISO": lambda: self._get_miso_constraints(days_back=days_back),
        }

        method = methods.get(iso_upper)
        if not method:
            logger.info(f"  {iso_upper}: congestion extracted from LMP data only")
            return pd.DataFrame()

        df = method()
        if not df.empty:
            self._congestion_data[iso_upper] = df
        return df

    def get_all_congestion(
        self,
        isos: Optional[List[str]] = None,
        days_back: int = 7,
    ) -> pd.DataFrame:
        """Fetch congestion data from all ISOs."""
        if isos is None:
            isos = ["CAISO", "PJM", "MISO"]

        frames = []
        for iso in isos:
            df = self.get_congestion_by_iso(iso, days_back=days_back)
            if not df.empty:
                frames.append(df)

        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

    def identify_congested_corridors(
        self,
        lmp_df: pd.DataFrame,
        threshold_pct: float = 10.0,
    ) -> pd.DataFrame:
        """
        Identify persistently congested corridors from LMP data.

        Args:
            lmp_df: LMP DataFrame with congestion component
            threshold_pct: % of hours that must be congested to qualify

        Returns:
            DataFrame of corridors/nodes with persistent congestion.
        """
        cong_df = self.get_congestion_from_lmp(lmp_df)
        if cong_df.empty or "node" not in cong_df.columns:
            return pd.DataFrame()

        # Group by node and calculate congestion frequency
        node_stats = (
            cong_df.groupby(["iso", "node"])
            .agg(
                total_hours=("congestion_value", "count"),
                congested_hours=("is_congested", "sum"),
                avg_congestion=("congestion_value", "mean"),
                max_congestion=("congestion_value", "max"),
                min_congestion=("congestion_value", "min"),
                std_congestion=("congestion_value", "std"),
            )
            .reset_index()
        )

        node_stats["congestion_pct"] = (
            node_stats["congested_hours"] / node_stats["total_hours"] * 100
        ).round(1)

        # Filter to persistently congested
        congested = node_stats[
            node_stats["congestion_pct"] >= threshold_pct
        ].sort_values("congestion_pct", ascending=False)

        logger.info(
            f"  Found {len(congested)} persistently congested nodes "
            f"(>{threshold_pct}% of hours)"
        )
        return congested

    def get_congestion_summary(self, lmp_df: Optional[pd.DataFrame] = None) -> Dict:
        """Generate congestion summary for pipeline output."""
        summary = {
            "constraint_data_isos": list(self._congestion_data.keys()),
            "total_constraint_records": sum(
                len(df) for df in self._congestion_data.values()
            ),
        }

        if lmp_df is not None and not lmp_df.empty:
            corridors = self.identify_congested_corridors(lmp_df)
            if not corridors.empty:
                summary["persistently_congested_nodes"] = len(corridors)
                summary["top_congested_nodes"] = (
                    corridors.head(20)
                    .to_dict("records")
                )

        return summary
