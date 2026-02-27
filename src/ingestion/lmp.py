"""
Locational Marginal Price (LMP) Ingestion — All Major US ISOs

Pulls historical and recent LMP data from:
  1. EIA Open Data API v2 — aggregate wholesale prices by region
  2. CAISO OASIS — day-ahead & real-time nodal LMPs (free, no auth)
  3. NYISO Public Data — zonal LBMP CSV downloads (free, no auth)
  4. ERCOT Data Products — settlement point prices (free, no auth)
  5. PJM Data Miner 2 — DA/RT LMPs (free account + API key)
  6. MISO — real-time LMP reports (free, no auth for public reports)
  7. SPP Marketplace — DA/RT LMPs (free, no auth)

LMPs are the key revenue signal for BESS — they determine arbitrage
spreads (buy low / sell high) at each grid node.

LMP = Energy Component + Congestion Component + Loss Component
"""

import logging
import csv
import io
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from typing import Optional, List, Dict
from pathlib import Path

import pandas as pd
import numpy as np

from ..utils.api_client import APIClient

logger = logging.getLogger(__name__)

# ── EIA API v2 endpoints ──────────────────────────────────────────
EIA_API_BASE = "https://api.eia.gov/v2"
# Wholesale electricity prices by region
EIA_WHOLESALE_ENDPOINT = f"{EIA_API_BASE}/electricity/wholesale-markets/data/"
# RTO interchange and demand
EIA_RTO_ENDPOINT = f"{EIA_API_BASE}/electricity/rto/daily-region-data/data/"

# ── ISO-specific endpoints ────────────────────────────────────────
# CAISO OASIS (no auth needed)
CAISO_OASIS_BASE = "https://oasis.caiso.com/oasisapi/SingleZip"

# NYISO public data (no auth)
NYISO_DATA_BASE = "http://mis.nyiso.com/public"

# ERCOT public data products (no auth)
ERCOT_DATA_BASE = "https://www.ercot.com/mp/data-products"

# PJM Data Miner 2 (free account + API key)
PJM_DATAMINER_BASE = "https://dataminer2.pjm.com/feed"

# SPP Marketplace (no auth)
SPP_MARKET_BASE = "https://marketplace.spp.org/file-browser-api/v1"

# MISO public reports (no auth)
MISO_MARKET_BASE = "https://docs.misoenergy.org/marketreports"


# ── ISO zone/hub definitions ─────────────────────────────────────
ISO_ZONES = {
    "ERCOT": {
        "hubs": ["HB_BUSAVG", "HB_HOUSTON", "HB_NORTH", "HB_SOUTH", "HB_WEST"],
        "load_zones": ["LZ_HOUSTON", "LZ_NORTH", "LZ_SOUTH", "LZ_WEST",
                        "LZ_AEN", "LZ_CPS", "LZ_LCRA", "LZ_RAYBN"],
    },
    "CAISO": {
        "hubs": ["TH_NP15_GEN-APND", "TH_SP15_GEN-APND", "TH_ZP26_GEN-APND"],
        "load_zones": ["DLAP_PGAE-APND", "DLAP_SCE-APND", "DLAP_SDGE-APND"],
    },
    "PJM": {
        "hubs": ["WESTERN HUB", "EASTERN HUB", "AEP GEN HUB", "APS GEN HUB",
                  "ATSI GEN HUB", "CHICAGO GEN HUB", "DOMINION HUB"],
        "load_zones": [],
    },
    "MISO": {
        "hubs": ["Indiana Hub", "Michigan Hub", "Minnesota Hub",
                  "Illinois Hub", "Arkansas Hub", "Louisiana Hub", "Texas Hub"],
        "load_zones": [],
    },
    "NYISO": {
        "zones": ["A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K"],
        "zone_names": {
            "A": "West", "B": "Genesee", "C": "Central", "D": "North",
            "E": "Mohawk Valley", "F": "Capital", "G": "Hudson Valley",
            "H": "Millwood", "I": "Dunwoodie", "J": "NYC", "K": "Long Island",
        },
    },
    "SPP": {
        "hubs": ["SPP North Hub", "SPP South Hub"],
        "load_zones": [],
    },
    "ISONE": {
        "zones": [".H.INTERNAL_HUB", "MAINE", "NEWHAMPSHIRE", "VERMONT",
                   "CONNECTICUT", "RHODEISLAND", "SEMASS", "WCMASS", "NEMA"],
    },
}


class LMPIngestor:
    """
    Ingests Locational Marginal Price data from all major US ISOs.

    Provides:
      - get_eia_wholesale_prices(): EIA aggregate wholesale prices
      - get_lmp_by_iso(): LMP data for a specific ISO
      - get_all_lmps(): Combined LMP summary across ISOs
      - get_price_spreads(): Peak/off-peak arbitrage spread analysis
      - get_lmp_near_point(): Find LMP nodes near a coordinate
      - get_lmp_summary(): Summary statistics for scoring
    """

    def __init__(self, config: dict):
        self.config = config
        self.market_config = config.get("market_data", {}).get("lmp", {})
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

        # Accumulated LMP data
        self._lmp_data = {}

    # ── EIA Aggregate Wholesale Prices ────────────────────────────

    def get_eia_wholesale_prices(self, days_back: int = 365) -> pd.DataFrame:
        """
        Fetch aggregate wholesale electricity prices from EIA API v2.
        Returns regional average prices — good for broad market overview.
        """
        eia_key = self.api_keys.get("eia", "")
        if not eia_key:
            logger.warning("No EIA API key — skipping wholesale price fetch")
            return pd.DataFrame()

        try:
            end_date = datetime.now().strftime("%Y-%m-%d")
            start_date = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")

            params = {
                "api_key": eia_key,
                "frequency": "daily",
                "data[0]": "value",
                "start": start_date,
                "end": end_date,
                "sort[0][column]": "period",
                "sort[0][direction]": "desc",
                "length": 5000,
            }

            data = self.client.get(
                EIA_RTO_ENDPOINT,
                params=params,
                cache_hours=min(self.cache_hours, 24),
            )

            records = data.get("response", {}).get("data", [])
            if not records:
                logger.warning("EIA wholesale prices: no data returned")
                return pd.DataFrame()

            df = pd.DataFrame(records)
            logger.info(f"EIA wholesale prices: {len(df)} daily records")
            return df

        except Exception as e:
            logger.warning(f"EIA wholesale price fetch failed: {e}")
            return pd.DataFrame()

    # ── CAISO OASIS ───────────────────────────────────────────────

    def _get_caiso_lmp(
        self,
        market: str = "DAM",
        days_back: int = 7,
        nodes: Optional[List[str]] = None,
    ) -> pd.DataFrame:
        """
        Fetch LMP data from CAISO OASIS API.

        Args:
            market: DAM (Day-Ahead), RTM (Real-Time), HASP
            days_back: Number of days of history
            nodes: Specific pnodes (None = default trading hubs)
        """
        try:
            end_dt = datetime.now()
            start_dt = end_dt - timedelta(days=days_back)

            # CAISO OASIS date format
            start_str = start_dt.strftime("%Y%m%dT07:00-0000")
            end_str = end_dt.strftime("%Y%m%dT07:00-0000")

            node_str = ",".join(nodes) if nodes else ",".join(
                ISO_ZONES["CAISO"]["hubs"] + ISO_ZONES["CAISO"]["load_zones"]
            )

            params = {
                "resultformat": "6",  # CSV
                "queryname": "PRC_LMP",
                "version": "12",
                "startdatetime": start_str,
                "enddatetime": end_str,
                "market_run_id": market,
                "node": node_str,
            }

            # CAISO returns a zip with CSV inside
            cache_file = self.cache_dir / f"caiso_lmp_{market}_{days_back}d.csv"
            age_ok = False
            if cache_file.exists():
                import time
                age_hours = (time.time() - cache_file.stat().st_mtime) / 3600
                age_ok = age_hours < min(self.cache_hours, 24)

            if age_ok:
                logger.info(f"  CAISO {market} LMP: using cached data")
                df = pd.read_csv(cache_file)
            else:
                import zipfile
                logger.info(f"  CAISO {market} LMP: fetching from OASIS...")
                response = self.client.session.get(
                    CAISO_OASIS_BASE, params=params, timeout=120
                )
                response.raise_for_status()

                # Response is a zip file containing CSV
                z = zipfile.ZipFile(io.BytesIO(response.content))
                csv_name = z.namelist()[0]
                with z.open(csv_name) as f:
                    df = pd.read_csv(f)

                df.to_csv(cache_file, index=False)

            if df.empty:
                return df

            # Standardize columns
            df_out = pd.DataFrame()
            col_map = {
                "INTERVALSTARTTIME_GMT": "timestamp",
                "OPR_DT": "date",
                "NODE": "node",
                "NODE_ID": "node_id",
                "LMP_TYPE": "lmp_type",
                "MW": "lmp_value",
                "VALUE": "lmp_value",
            }
            for old, new in col_map.items():
                if old in df.columns:
                    df_out[new] = df[old]

            # Pivot LMP components if present
            if "lmp_type" in df_out.columns and "lmp_value" in df_out.columns:
                # CAISO returns separate rows for LMP, CONG, LOSS, ENERGY
                pivot_cols = ["timestamp", "node"] if "timestamp" in df_out.columns else ["date", "node"]
                available_pivot = [c for c in pivot_cols if c in df_out.columns]
                if available_pivot:
                    try:
                        pivoted = df_out.pivot_table(
                            index=available_pivot,
                            columns="lmp_type",
                            values="lmp_value",
                            aggfunc="mean",
                        ).reset_index()
                        pivoted.columns.name = None
                        # Rename LMP component columns
                        rename = {"LMP": "lmp", "MCC": "congestion", "MCE": "energy", "MCL": "loss"}
                        pivoted.rename(columns=rename, inplace=True)
                        pivoted["iso"] = "CAISO"
                        pivoted["market"] = market
                        return pivoted
                    except Exception:
                        pass

            df_out["iso"] = "CAISO"
            df_out["market"] = market
            logger.info(f"  CAISO {market}: {len(df_out)} LMP records")
            return df_out

        except Exception as e:
            logger.warning(f"  CAISO LMP fetch failed: {e}")
            return pd.DataFrame()

    # ── NYISO ─────────────────────────────────────────────────────

    def _get_nyiso_lmp(self, days_back: int = 30) -> pd.DataFrame:
        """
        Fetch NYISO zonal LBMP data from public CSV archives.
        NYISO publishes daily CSV files at mis.nyiso.com/public/.
        """
        try:
            frames = []
            end_date = datetime.now()

            for i in range(min(days_back, 30)):
                dt = end_date - timedelta(days=i)
                date_str = dt.strftime("%Y%m%d")
                year_month = dt.strftime("%Y%m01")

                # NYISO file naming: YYYYMMDD followed by zone type
                # Day-ahead zonal LBMP
                url = (
                    f"{NYISO_DATA_BASE}/csv/damlbmp/{date_str}damlbmp_zone.csv"
                )

                cache_file = self.cache_dir / f"nyiso_lbmp_{date_str}.csv"
                if cache_file.exists():
                    try:
                        df = pd.read_csv(cache_file)
                        frames.append(df)
                        continue
                    except Exception:
                        pass

                try:
                    response = self.client.session.get(url, timeout=30)
                    if response.status_code == 200:
                        df = pd.read_csv(io.StringIO(response.text))
                        df.to_csv(cache_file, index=False)
                        frames.append(df)
                    else:
                        # Try archived zip format
                        zip_url = (
                            f"{NYISO_DATA_BASE}/csv/damlbmp/{year_month}damlbmp_zone_csv.zip"
                        )
                        # Skip archive downloads for now — too large
                        pass
                except Exception:
                    continue

            if not frames:
                logger.warning("  NYISO: no LBMP data retrieved")
                return pd.DataFrame()

            df = pd.concat(frames, ignore_index=True)

            # Standardize
            col_map = {
                "Name": "node",
                "Zone Name": "node",
                "LBMP ($/MWHr)": "lmp",
                "Marginal Cost Losses ($/MWHr)": "loss",
                "Marginal Cost Congestion ($/MWHr)": "congestion",
                "Time Stamp": "timestamp",
            }
            for old, new in col_map.items():
                if old in df.columns:
                    df.rename(columns={old: new}, inplace=True)

            df["iso"] = "NYISO"
            df["market"] = "DAM"
            logger.info(f"  NYISO: {len(df)} LBMP records")
            return df

        except Exception as e:
            logger.warning(f"  NYISO LBMP fetch failed: {e}")
            return pd.DataFrame()

    # ── ERCOT ─────────────────────────────────────────────────────

    def _get_ercot_lmp(self, days_back: int = 30) -> pd.DataFrame:
        """
        Fetch ERCOT settlement point prices from public data products.
        Uses the publicly posted CSV/XML reports.
        """
        try:
            frames = []
            end_date = datetime.now()

            for i in range(min(days_back, 30)):
                dt = end_date - timedelta(days=i)
                date_str = dt.strftime("%Y%m%d")

                # ERCOT posts DAM SPP as CSV
                # Try the common archive location
                url = (
                    f"https://www.ercot.com/content/cdr/html/{date_str}_dam_spp.csv"
                )

                cache_file = self.cache_dir / f"ercot_spp_{date_str}.csv"
                if cache_file.exists():
                    try:
                        df = pd.read_csv(cache_file)
                        frames.append(df)
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
                logger.warning("  ERCOT: no SPP data retrieved")
                return pd.DataFrame()

            df = pd.concat(frames, ignore_index=True)

            # Standardize columns
            col_map = {
                "Settlement Point Name": "node",
                "Settlement Point": "node",
                "Settlement Point Price": "lmp",
                "DSTFlag": "dst_flag",
                "DeliveryDate": "date",
                "Delivery Date": "date",
                "HourEnding": "hour",
                "Hour Ending": "hour",
                "Repeated Hour Flag": "repeated_hour",
            }
            for old, new in col_map.items():
                if old in df.columns:
                    df.rename(columns={old: new}, inplace=True)

            df["iso"] = "ERCOT"
            df["market"] = "DAM"
            logger.info(f"  ERCOT: {len(df)} SPP records")
            return df

        except Exception as e:
            logger.warning(f"  ERCOT SPP fetch failed: {e}")
            return pd.DataFrame()

    # ── PJM ───────────────────────────────────────────────────────

    def _get_pjm_lmp(self, days_back: int = 7) -> pd.DataFrame:
        """
        Fetch PJM LMP data from Data Miner 2.
        Requires free PJM API key (set in config).
        """
        pjm_key = self.api_keys.get("pjm", "")
        if not pjm_key:
            logger.info("  PJM: No API key configured — skipping (free at apiportal.pjm.com)")
            return pd.DataFrame()

        try:
            end_dt = datetime.now()
            start_dt = end_dt - timedelta(days=days_back)

            headers = {
                "Ocp-Apim-Subscription-Key": pjm_key,
            }

            url = f"{PJM_DATAMINER_BASE}/da_hrl_lmps"
            params = {
                "startRow": 1,
                "rowCount": 5000,
                "datetime_beginning_ept": start_dt.strftime("%Y-%m-%dT00:00:00"),
                "datetime_ending_ept": end_dt.strftime("%Y-%m-%dT00:00:00"),
                "type": "zone",  # zone-level (less data than node)
            }

            cache_file = self.cache_dir / f"pjm_lmp_{days_back}d.json"
            import time as _time
            if cache_file.exists():
                age_hours = (_time.time() - cache_file.stat().st_mtime) / 3600
                if age_hours < min(self.cache_hours, 24):
                    import json
                    with open(cache_file) as f:
                        records = json.load(f)
                    df = pd.DataFrame(records)
                    df["iso"] = "PJM"
                    df["market"] = "DAM"
                    logger.info(f"  PJM (cached): {len(df)} LMP records")
                    return df

            response = self.client.session.get(
                url, params=params, headers=headers, timeout=120
            )
            response.raise_for_status()
            data = response.json()

            records = data if isinstance(data, list) else data.get("items", data.get("data", []))
            if not records:
                logger.warning("  PJM: no LMP data returned")
                return pd.DataFrame()

            import json
            with open(cache_file, "w") as f:
                json.dump(records, f)

            df = pd.DataFrame(records)

            # Standardize
            col_map = {
                "pnode_name": "node",
                "pnode_id": "node_id",
                "total_lmp_da": "lmp",
                "congestion_price_da": "congestion",
                "marginal_loss_price_da": "loss",
                "system_energy_price_da": "energy",
                "datetime_beginning_ept": "timestamp",
            }
            for old, new in col_map.items():
                if old in df.columns:
                    df.rename(columns={old: new}, inplace=True)

            df["iso"] = "PJM"
            df["market"] = "DAM"
            logger.info(f"  PJM: {len(df)} LMP records")
            return df

        except Exception as e:
            logger.warning(f"  PJM LMP fetch failed: {e}")
            return pd.DataFrame()

    # ── MISO ──────────────────────────────────────────────────────

    def _get_miso_lmp(self, days_back: int = 7) -> pd.DataFrame:
        """
        Fetch MISO LMP data from public market reports.
        MISO posts daily CSV reports.
        """
        try:
            frames = []
            end_date = datetime.now()

            for i in range(min(days_back, 14)):
                dt = end_date - timedelta(days=i)
                date_str = dt.strftime("%Y%m%d")

                # MISO DA LMP summary (publicly posted)
                url = f"{MISO_MARKET_BASE}/{date_str}_da_expost_lmp.csv"

                cache_file = self.cache_dir / f"miso_lmp_{date_str}.csv"
                if cache_file.exists():
                    try:
                        df = pd.read_csv(cache_file)
                        frames.append(df)
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
                logger.warning("  MISO: no LMP data retrieved")
                return pd.DataFrame()

            df = pd.concat(frames, ignore_index=True)

            # Standardize
            col_map = {
                "Node": "node",
                "CPNODE": "node",
                "LMP": "lmp",
                "MLC": "loss",
                "MCC": "congestion",
                "HourEnding": "hour",
                "MKTHOUR": "hour",
            }
            for old, new in col_map.items():
                if old in df.columns:
                    df.rename(columns={old: new}, inplace=True)

            df["iso"] = "MISO"
            df["market"] = "DAM"
            logger.info(f"  MISO: {len(df)} LMP records")
            return df

        except Exception as e:
            logger.warning(f"  MISO LMP fetch failed: {e}")
            return pd.DataFrame()

    # ── SPP ───────────────────────────────────────────────────────

    def _get_spp_lmp(self, days_back: int = 7) -> pd.DataFrame:
        """
        Fetch SPP LMP data from marketplace portal.
        SPP posts daily CSV/Excel market reports.
        """
        try:
            frames = []
            end_date = datetime.now()

            for i in range(min(days_back, 14)):
                dt = end_date - timedelta(days=i)
                date_str = dt.strftime("%Y%m%d")

                # SPP DA LMP
                url = (
                    f"https://marketplace.spp.org/file-browser-api/v1/"
                    f"download/da-lmp-by-location?path=/"
                    f"{dt.strftime('%Y')}/{dt.strftime('%m')}/"
                    f"By_Day/DA-LMP-SL-{date_str}0100.csv"
                )

                cache_file = self.cache_dir / f"spp_lmp_{date_str}.csv"
                if cache_file.exists():
                    try:
                        df = pd.read_csv(cache_file)
                        frames.append(df)
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
                logger.warning("  SPP: no LMP data retrieved")
                return pd.DataFrame()

            df = pd.concat(frames, ignore_index=True)

            col_map = {
                "Settlement Location": "node",
                "Pnode": "node",
                "LMP": "lmp",
                "MLC": "loss",
                "MCC": "congestion",
                "MEC": "energy",
                "GMTIntervalEnd": "timestamp",
            }
            for old, new in col_map.items():
                if old in df.columns:
                    df.rename(columns={old: new}, inplace=True)

            df["iso"] = "SPP"
            df["market"] = "DAM"
            logger.info(f"  SPP: {len(df)} LMP records")
            return df

        except Exception as e:
            logger.warning(f"  SPP LMP fetch failed: {e}")
            return pd.DataFrame()

    # ── Public interface ──────────────────────────────────────────

    def get_lmp_by_iso(self, iso_name: str, days_back: int = 30) -> pd.DataFrame:
        """
        Fetch LMP data for a single ISO.

        Args:
            iso_name: CAISO, ERCOT, PJM, MISO, NYISO, SPP, ISONE
            days_back: Days of history to fetch

        Returns:
            DataFrame with columns: node, lmp, congestion, loss, timestamp, iso, market
        """
        iso_upper = iso_name.upper()
        logger.info(f"Fetching {iso_upper} LMP data ({days_back} days)...")

        methods = {
            "CAISO": lambda: self._get_caiso_lmp(days_back=days_back),
            "ERCOT": lambda: self._get_ercot_lmp(days_back=days_back),
            "PJM": lambda: self._get_pjm_lmp(days_back=days_back),
            "MISO": lambda: self._get_miso_lmp(days_back=days_back),
            "NYISO": lambda: self._get_nyiso_lmp(days_back=days_back),
            "SPP": lambda: self._get_spp_lmp(days_back=days_back),
        }

        method = methods.get(iso_upper)
        if not method:
            logger.warning(f"  {iso_upper}: no LMP method available")
            return pd.DataFrame()

        df = method()
        if not df.empty:
            self._lmp_data[iso_upper] = df
        return df

    def get_all_lmps(
        self,
        isos: Optional[List[str]] = None,
        days_back: int = 30,
    ) -> pd.DataFrame:
        """
        Fetch LMP data from all (or specified) ISOs.

        Returns:
            Combined DataFrame with LMP data across all ISOs.
        """
        if isos is None:
            isos = self.market_config.get(
                "isos", ["ERCOT", "CAISO", "PJM", "MISO", "NYISO", "SPP"]
            )

        frames = []
        for iso in isos:
            df = self.get_lmp_by_iso(iso, days_back=days_back)
            if not df.empty:
                frames.append(df)

        if not frames:
            return pd.DataFrame()

        combined = pd.concat(frames, ignore_index=True)
        logger.info(f"Total LMP records across {len(frames)} ISOs: {len(combined)}")
        return combined

    def get_price_spreads(self, df: Optional[pd.DataFrame] = None) -> Dict:
        """
        Calculate peak/off-peak price spreads by ISO and node.
        This is the key BESS arbitrage metric.

        Peak hours: 7am–11pm (HE 8–23)
        Off-peak: 11pm–7am (HE 24, 1–7)
        """
        if df is None:
            # Combine all collected data
            if not self._lmp_data:
                return {}
            df = pd.concat(self._lmp_data.values(), ignore_index=True)

        if df.empty or "lmp" not in df.columns:
            return {}

        # Ensure numeric LMP
        df["lmp"] = pd.to_numeric(df["lmp"], errors="coerce")
        df = df.dropna(subset=["lmp"])

        spreads = {}

        for iso in df["iso"].unique():
            iso_df = df[df["iso"] == iso]

            # Try to determine hour for peak/off-peak split
            hour_col = None
            for c in ["hour", "timestamp"]:
                if c in iso_df.columns:
                    hour_col = c
                    break

            if hour_col == "timestamp":
                try:
                    iso_df = iso_df.copy()
                    iso_df["_hour"] = pd.to_datetime(
                        iso_df["timestamp"], errors="coerce"
                    ).dt.hour
                    hour_col = "_hour"
                except Exception:
                    hour_col = None
            elif hour_col == "hour":
                iso_df = iso_df.copy()
                iso_df["_hour"] = pd.to_numeric(iso_df["hour"], errors="coerce")
                hour_col = "_hour"

            avg_lmp = float(iso_df["lmp"].mean())
            median_lmp = float(iso_df["lmp"].median())
            max_lmp = float(iso_df["lmp"].max())
            min_lmp = float(iso_df["lmp"].min())
            p95_lmp = float(iso_df["lmp"].quantile(0.95))
            p5_lmp = float(iso_df["lmp"].quantile(0.05))

            iso_spread = {
                "avg_lmp": round(avg_lmp, 2),
                "median_lmp": round(median_lmp, 2),
                "max_lmp": round(max_lmp, 2),
                "min_lmp": round(min_lmp, 2),
                "p95_lmp": round(p95_lmp, 2),
                "p5_lmp": round(p5_lmp, 2),
                "spread_p95_p5": round(p95_lmp - p5_lmp, 2),
                "spread_max_min": round(max_lmp - min_lmp, 2),
                "records": len(iso_df),
            }

            # Peak/off-peak if we have hour data
            if hour_col and hour_col in iso_df.columns:
                peak = iso_df[
                    (iso_df[hour_col] >= 7) & (iso_df[hour_col] <= 22)
                ]
                offpeak = iso_df[
                    (iso_df[hour_col] < 7) | (iso_df[hour_col] > 22)
                ]

                if not peak.empty and not offpeak.empty:
                    peak_avg = float(peak["lmp"].mean())
                    offpeak_avg = float(offpeak["lmp"].mean())
                    iso_spread["peak_avg"] = round(peak_avg, 2)
                    iso_spread["offpeak_avg"] = round(offpeak_avg, 2)
                    iso_spread["peak_offpeak_spread"] = round(
                        peak_avg - offpeak_avg, 2
                    )

            # Congestion analysis if available
            if "congestion" in iso_df.columns:
                cong = pd.to_numeric(iso_df["congestion"], errors="coerce").dropna()
                if not cong.empty:
                    iso_spread["avg_congestion"] = round(float(cong.mean()), 2)
                    iso_spread["max_congestion"] = round(float(cong.max()), 2)
                    iso_spread["congestion_pct_of_lmp"] = round(
                        float(cong.mean() / avg_lmp * 100) if avg_lmp != 0 else 0, 1
                    )

            # Per-node analysis
            if "node" in iso_df.columns:
                node_stats = (
                    iso_df.groupby("node")["lmp"]
                    .agg(["mean", "std", "max", "min"])
                    .round(2)
                )
                node_stats["spread"] = node_stats["max"] - node_stats["min"]
                node_stats = node_stats.sort_values("spread", ascending=False)
                iso_spread["top_spread_nodes"] = (
                    node_stats.head(10).to_dict("index")
                )

            spreads[iso] = iso_spread

        return spreads

    def get_lmp_summary(self) -> Dict:
        """
        Generate a summary of LMP data for pipeline output.
        """
        if not self._lmp_data:
            return {}

        spreads = self.get_price_spreads()

        summary = {
            "isos_covered": list(self._lmp_data.keys()),
            "total_records": sum(len(df) for df in self._lmp_data.values()),
            "spreads_by_iso": spreads,
        }

        # Overall best arbitrage opportunities
        best_spreads = []
        for iso, data in spreads.items():
            spread_val = data.get("peak_offpeak_spread") or data.get("spread_p95_p5", 0)
            best_spreads.append({
                "iso": iso,
                "spread_$/MWh": spread_val,
                "avg_lmp": data.get("avg_lmp", 0),
            })

        best_spreads.sort(key=lambda x: x["spread_$/MWh"], reverse=True)
        summary["best_arbitrage_isos"] = best_spreads

        return summary
