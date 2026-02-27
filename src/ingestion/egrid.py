"""
EPA eGRID Data Ingestion — Emissions & Generation Data

Downloads and parses EPA's Emissions & Generation Resource Integrated
Database (eGRID). Provides plant-level emissions profiles (CO2, NOx, SO2),
generation data (MWh), heat rates, capacity factors, and subregion info.

Data source: https://www.epa.gov/egrid/download-data
The eGRID workbook is an Excel file updated ~annually. The ORIS plant code
matches EIA's plant_id for cross-referencing.

eGRID 2022 is the latest available as of Feb 2026.
"""

import logging
from pathlib import Path
from typing import Optional

import pandas as pd

from ..utils.api_client import APIClient

logger = logging.getLogger(__name__)

# eGRID Excel download URL (2022 data, latest available)
EGRID_DOWNLOAD_URL = (
    "https://www.epa.gov/system/files/documents/2024-01/"
    "egrid2022_data.xlsx"
)

# Key sheets in the eGRID workbook
EGRID_SHEETS = {
    "PLNT": "PLNT22",   # Plant-level data (2022)
    "GEN": "GEN22",     # Generator-level data
    "SRL": "SRL22",     # eGRID subregion-level
    "ST": "ST22",       # State-level
    "US": "US22",       # National-level
}

# Key plant-level columns to extract
PLANT_COLUMNS = {
    "ORISPL": "plant_id",           # DOE/EIA ORIS plant code
    "PNAME": "plant_name",          # Plant name
    "OPRNAME": "operator_name",     # Operator name
    "PSTATABB": "state",            # State abbreviation
    "CNTYNAME": "county",           # County name
    "LAT": "lat",                   # Latitude
    "LON": "lon",                   # Longitude
    "PLPRMFL": "primary_fuel",      # Primary fuel
    "PLFUELCT": "fuel_category",    # Fuel category (broad)
    "NAMEPCAP": "nameplate_mw",     # Nameplate capacity (MW)
    "PLNGENAN": "annual_gen_mwh",   # Annual net generation (MWh)
    "PLCO2AN": "annual_co2_tons",   # Annual CO2 emissions (tons)
    "PLNOXAN": "annual_nox_tons",   # Annual NOx emissions (tons)
    "PLSO2AN": "annual_so2_tons",   # Annual SO2 emissions (tons)
    "PLCO2RTA": "co2_rate_lb_mwh",  # CO2 emission rate (lb/MWh)
    "PLNOXRTA": "nox_rate_lb_mwh",  # NOx emission rate (lb/MWh)
    "PLSO2RTA": "so2_rate_lb_mwh",  # SO2 emission rate (lb/MWh)
    "PLHTRT": "heat_rate_btu_kwh",  # Heat rate (Btu/kWh)
    "PLCPFCT": "capacity_factor",   # Capacity factor
    "SUBRGN": "egrid_subregion",    # eGRID subregion
    "NETEFX": "nerc_region",        # NERC region
}


class EGRIDIngestor:
    """
    Ingests EPA eGRID data for plant-level emissions and generation metrics.

    Provides:
      - load_egrid_data(): Parse plant-level data from eGRID workbook
      - get_plant_emissions(): Emissions profile for a single plant
      - enrich_eia_plants(): Join emissions data to EIA plant inventory
      - get_clean_vs_dirty(): Classify plants by carbon intensity
    """

    def __init__(self, config: dict):
        self.config = config
        self.egrid_config = config.get("generation_assets", {}).get("egrid", {})
        self.client = APIClient(
            cache_dir=config.get("cache", {}).get("directory", "./data/cache"),
            cache_enabled=config.get("cache", {}).get("enabled", True),
        )
        self.cache_dir = Path(
            config.get("cache", {}).get("directory", "./data/cache")
        )
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self._plant_data: Optional[pd.DataFrame] = None

    def _download_egrid(self) -> Path:
        """
        Download the eGRID Excel workbook if not cached.

        Returns path to the local Excel file.
        """
        filename = "egrid2022_data.xlsx"
        filepath = self.cache_dir / filename

        if filepath.exists():
            logger.info(f"eGRID data cached at {filepath}")
            return filepath

        logger.info("Downloading eGRID 2022 data from EPA (~30 MB)...")
        try:
            response = self.client.session.get(EGRID_DOWNLOAD_URL, timeout=120)
            response.raise_for_status()
            with open(filepath, "wb") as f:
                f.write(response.content)
            logger.info(f"eGRID data saved to {filepath}")
            return filepath
        except Exception as e:
            logger.error(f"Failed to download eGRID data: {e}")
            raise

    def load_egrid_data(
        self,
        state_filter: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Load plant-level data from the eGRID workbook.

        Caches the parsed DataFrame in memory for repeated access.

        Args:
            state_filter: Two-letter state code or None for all

        Returns:
            DataFrame with standardized emissions and generation columns.
        """
        if self._plant_data is not None:
            df = self._plant_data
            if state_filter and state_filter != "ALL":
                return df[df["state"] == state_filter].copy()
            return df

        filepath = self._download_egrid()

        logger.info("Parsing eGRID plant-level data...")

        try:
            # eGRID has header rows we need to skip — row 0 is a label,
            # row 1 has column names
            df = pd.read_excel(
                filepath,
                sheet_name=EGRID_SHEETS["PLNT"],
                header=1,  # Second row is the header
            )
        except Exception as e:
            logger.error(f"Failed to parse eGRID Excel: {e}")
            return pd.DataFrame()

        if df.empty:
            logger.warning("eGRID plant sheet is empty")
            return df

        # Select and rename columns
        available_cols = {
            old: new for old, new in PLANT_COLUMNS.items() if old in df.columns
        }
        df = df[list(available_cols.keys())].rename(columns=available_cols)

        # Clean numeric columns
        numeric_cols = [
            "nameplate_mw", "annual_gen_mwh", "annual_co2_tons",
            "annual_nox_tons", "annual_so2_tons", "co2_rate_lb_mwh",
            "nox_rate_lb_mwh", "so2_rate_lb_mwh", "heat_rate_btu_kwh",
            "capacity_factor", "lat", "lon",
        ]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        # Clean plant_id
        if "plant_id" in df.columns:
            df["plant_id"] = pd.to_numeric(df["plant_id"], errors="coerce").astype("Int64")

        logger.info(f"Loaded {len(df)} plants from eGRID 2022")

        # Summary
        total_mw = df["nameplate_mw"].sum() if "nameplate_mw" in df.columns else 0
        total_gen = df["annual_gen_mwh"].sum() if "annual_gen_mwh" in df.columns else 0
        total_co2 = df["annual_co2_tons"].sum() if "annual_co2_tons" in df.columns else 0

        logger.info(
            f"  Total: {total_mw:,.0f} MW nameplate, "
            f"{total_gen:,.0f} MWh generation, "
            f"{total_co2:,.0f} tons CO2"
        )

        self._plant_data = df

        if state_filter and state_filter != "ALL":
            return df[df["state"] == state_filter].copy()

        return df

    def get_plant_emissions(self, plant_id: int) -> dict:
        """
        Get emissions profile for a single plant by ORIS code.

        Returns dict with emissions rates, generation, and capacity factor.
        """
        df = self.load_egrid_data()

        if df.empty or "plant_id" not in df.columns:
            return {"found": False}

        match = df[df["plant_id"] == plant_id]

        if match.empty:
            return {"found": False, "plant_id": plant_id}

        row = match.iloc[0]
        return {
            "found": True,
            "plant_id": plant_id,
            "plant_name": row.get("plant_name", ""),
            "state": row.get("state", ""),
            "primary_fuel": row.get("primary_fuel", ""),
            "nameplate_mw": row.get("nameplate_mw", 0),
            "annual_gen_mwh": row.get("annual_gen_mwh", 0),
            "capacity_factor": row.get("capacity_factor", 0),
            "co2_rate_lb_mwh": row.get("co2_rate_lb_mwh", 0),
            "nox_rate_lb_mwh": row.get("nox_rate_lb_mwh", 0),
            "so2_rate_lb_mwh": row.get("so2_rate_lb_mwh", 0),
            "annual_co2_tons": row.get("annual_co2_tons", 0),
            "egrid_subregion": row.get("egrid_subregion", ""),
        }

    def enrich_eia_plants(
        self,
        eia_plants: pd.DataFrame,
        plant_id_col: str = "plant_id",
    ) -> pd.DataFrame:
        """
        Enrich EIA plant data with eGRID emissions metrics.

        Joins on plant_id (ORIS code).
        """
        egrid_df = self.load_egrid_data()

        if egrid_df.empty or eia_plants.empty:
            return eia_plants

        if plant_id_col not in eia_plants.columns:
            logger.warning(f"Column '{plant_id_col}' not in EIA plants — skipping eGRID enrichment")
            return eia_plants

        # Select enrichment columns
        enrich_cols = [
            "plant_id", "annual_gen_mwh", "annual_co2_tons",
            "co2_rate_lb_mwh", "nox_rate_lb_mwh", "so2_rate_lb_mwh",
            "heat_rate_btu_kwh", "capacity_factor", "egrid_subregion",
        ]
        available = [c for c in enrich_cols if c in egrid_df.columns]
        egrid_subset = egrid_df[available].drop_duplicates(subset=["plant_id"])

        merged = eia_plants.merge(
            egrid_subset,
            left_on=plant_id_col,
            right_on="plant_id",
            how="left",
            suffixes=("", "_egrid"),
        )

        matched = merged["annual_co2_tons"].notna().sum() if "annual_co2_tons" in merged.columns else 0
        logger.info(
            f"eGRID enrichment: matched {matched}/{len(eia_plants)} plants "
            f"({100*matched/len(eia_plants):.0f}%)"
        )

        return merged

    def get_clean_vs_dirty(
        self,
        state_filter: Optional[str] = None,
        co2_threshold_lb_mwh: float = 100.0,
    ) -> dict:
        """
        Classify plants as clean (<threshold CO2) or dirty (>=threshold).

        Default threshold 100 lb CO2/MWh separates renewables/nuclear
        from fossil fuel plants.

        Returns summary dict with capacity and generation breakdowns.
        """
        df = self.load_egrid_data(state_filter=state_filter)

        if df.empty:
            return {"clean": {}, "dirty": {}, "unknown": {}}

        if "co2_rate_lb_mwh" not in df.columns:
            return {"clean": {}, "dirty": {}, "unknown": {"count": len(df)}}

        clean = df[df["co2_rate_lb_mwh"] <= co2_threshold_lb_mwh]
        dirty = df[df["co2_rate_lb_mwh"] > co2_threshold_lb_mwh]
        unknown = df[df["co2_rate_lb_mwh"].isna()]

        def _summarize(subset):
            return {
                "count": len(subset),
                "capacity_mw": round(subset["nameplate_mw"].sum(), 1) if "nameplate_mw" in subset.columns else 0,
                "generation_mwh": round(subset["annual_gen_mwh"].sum(), 1) if "annual_gen_mwh" in subset.columns else 0,
                "co2_tons": round(subset["annual_co2_tons"].sum(), 1) if "annual_co2_tons" in subset.columns else 0,
            }

        return {
            "clean": _summarize(clean),
            "dirty": _summarize(dirty),
            "unknown": _summarize(unknown),
            "threshold_lb_mwh": co2_threshold_lb_mwh,
        }
