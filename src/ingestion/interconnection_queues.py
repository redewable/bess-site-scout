"""
ISO/RTO Interconnection Queue Ingestion — All 7 Major US Grid Operators

Pulls interconnection queue data from ERCOT, CAISO, PJM, MISO, SPP,
NYISO, and ISO-NE. These queues contain every project that has applied
to connect to the grid — showing capacity, fuel type, status, POI
(point of interconnection/substation), and developer.

Primary method: `gridstatus` open-source library (pip install gridstatus)
  which standardizes queue data from all ISOs into a common DataFrame.
  NOTE: gridstatus requires Python 3.10+.

Fallback: Direct downloads from each ISO's public data portal (works
  on any Python 3.8+). Covers MISO (JSON API), ERCOT, CAISO, NYISO,
  PJM, and SPP (Excel/CSV downloads).

No API key needed for most ISOs. PJM requires a free API key (set
PJM_API_KEY env var or in config).
"""

import logging
import os
from typing import Optional, List
from io import BytesIO
from pathlib import Path

import pandas as pd
import geopandas as gpd
from shapely.geometry import Point

from ..utils.api_client import APIClient
from ..utils.geo import haversine_distance, WGS84

logger = logging.getLogger(__name__)

# Direct download URLs for each ISO queue
# These are public data portals — no API key needed (except PJM)
ISO_QUEUE_URLS = {
    "MISO": "https://www.misoenergy.org/api/giqueue/getprojects",
    "ERCOT": (
        "https://www.ercot.com/files/docs/2024/02/05/"
        "GIS_Report.xlsx"
    ),
    "CAISO": (
        "https://rimspub.caiso.com/rimsui/logon.do"  # CAISO requires login — use CSV export
    ),
    "PJM": "https://services.pjm.com/PJMPlanningApi/api/Queue/ExcelExport",
    "NYISO": (
        "https://www.nyiso.com/documents/20142/1407078/"
        "NYISO-Interconnection-Queue.xlsx"
    ),
    "SPP": (
        "https://opsportal.spp.org/Studies/GenerationInterconnectionQueue"
    ),
}

# Fuel type normalization for interconnection queue entries
QUEUE_FUEL_MAP = {
    # Solar
    "solar": "Solar", "photovoltaic": "Solar", "pv": "Solar",
    "solar photovoltaic": "Solar",
    # Wind
    "wind": "Wind", "onshore wind": "Wind", "offshore wind": "Wind",
    # Battery / Storage
    "battery": "Battery Storage", "storage": "Battery Storage",
    "energy storage": "Battery Storage", "bess": "Battery Storage",
    "battery storage": "Battery Storage", "battery energy storage": "Battery Storage",
    "es": "Battery Storage",
    # Hybrid
    "solar + storage": "Hybrid (Solar+Storage)",
    "solar/storage": "Hybrid (Solar+Storage)",
    "wind + storage": "Hybrid (Wind+Storage)",
    "wind/storage": "Hybrid (Wind+Storage)",
    "hybrid": "Hybrid",
    # Gas
    "gas": "Natural Gas", "natural gas": "Natural Gas",
    "ng": "Natural Gas", "ct": "Natural Gas", "cc": "Natural Gas",
    "combustion turbine": "Natural Gas", "combined cycle": "Natural Gas",
    # Nuclear
    "nuclear": "Nuclear",
    # Hydro
    "hydro": "Hydro", "hydroelectric": "Hydro",
    "pumped storage": "Pumped Storage",
    # Other
    "biomass": "Biomass", "geothermal": "Geothermal",
    "coal": "Coal", "hydrogen": "Hydrogen",
}

# Queue status normalization
QUEUE_STATUS_MAP = {
    "active": "Active",
    "under study": "Under Study",
    "facility study": "Facility Study",
    "system impact study": "System Impact Study",
    "ia executed": "IA Executed",
    "ia in progress": "IA in Progress",
    "engineering & procurement": "Engineering & Procurement",
    "under construction": "Under Construction",
    "operational": "Operational",
    "completed": "Completed",
    "withdrawn": "Withdrawn",
    "suspended": "Suspended",
    "cancelled": "Cancelled",
    "deactivated": "Deactivated",
}


def _check_gridstatus_available() -> bool:
    """Check if gridstatus library is installed and compatible with this Python version."""
    try:
        import gridstatus  # noqa: F401
        return True
    except (ImportError, SyntaxError, Exception) as e:
        # SyntaxError: gridstatus uses match/case (Python 3.10+)
        # ImportError: gridstatus not installed
        logger.info(f"gridstatus not available ({type(e).__name__}), using direct ISO downloads")
        return False


class InterconnectionQueueIngestor:
    """
    Ingests interconnection queue data from all major US ISOs.

    Provides:
      - get_all_queues(): Combined queue from all ISOs
      - get_queue_by_iso(): Single ISO queue
      - get_projects_near_point(): Spatial query for projects near a coordinate
      - get_queue_at_substation(): Match queued projects to a substation name
    """

    SUPPORTED_ISOS = ["CAISO", "ERCOT", "ISONE", "MISO", "NYISO", "PJM", "SPP"]

    def __init__(self, config: dict):
        self.config = config
        self.queue_config = config.get("generation_assets", {}).get(
            "interconnection_queues", {}
        )
        self.client = APIClient(
            cache_dir=config.get("cache", {}).get("directory", "./data/cache"),
            cache_enabled=config.get("cache", {}).get("enabled", True),
        )
        self.cache_hours = config.get("cache", {}).get("grid_data_expiry_hours", 720)
        self._gridstatus_available = _check_gridstatus_available()

        # PJM API key
        pjm_key = config.get("api_keys", {}).get("pjm", "")
        if pjm_key:
            os.environ["PJM_API_KEY"] = pjm_key

        self.cache_dir = Path(
            config.get("cache", {}).get("directory", "./data/cache")
        )
        self.cache_dir.mkdir(parents=True, exist_ok=True)

        if self._gridstatus_available:
            logger.info("gridstatus library available — using standardized queue access")
        else:
            logger.info(
                "gridstatus not available (requires Python 3.10+). "
                "Using direct ISO downloads instead."
            )

    def _normalize_fuel(self, fuel_str: str) -> str:
        """Normalize queue fuel type descriptions to standard categories."""
        if not fuel_str or pd.isna(fuel_str):
            return "Other"
        fuel_lower = str(fuel_str).strip().lower()
        # Try exact match
        if fuel_lower in QUEUE_FUEL_MAP:
            return QUEUE_FUEL_MAP[fuel_lower]
        # Try substring match
        for key, val in QUEUE_FUEL_MAP.items():
            if key in fuel_lower:
                return val
        return "Other"

    def _normalize_status(self, status_str: str) -> str:
        """Normalize queue status to standard categories."""
        if not status_str or pd.isna(status_str):
            return "Unknown"
        status_lower = str(status_str).strip().lower()
        if status_lower in QUEUE_STATUS_MAP:
            return QUEUE_STATUS_MAP[status_lower]
        for key, val in QUEUE_STATUS_MAP.items():
            if key in status_lower:
                return val
        return str(status_str).strip()

    def _get_queue_gridstatus(self, iso_name: str) -> pd.DataFrame:
        """Fetch queue from a single ISO using gridstatus library."""
        import gridstatus

        iso_map = {
            "CAISO": gridstatus.CAISO,
            "ERCOT": gridstatus.Ercot,
            "ISONE": gridstatus.ISONE,
            "MISO": gridstatus.MISO,
            "NYISO": gridstatus.NYISO,
            "PJM": gridstatus.PJM,
            "SPP": gridstatus.SPP,
        }

        iso_class = iso_map.get(iso_name.upper())
        if not iso_class:
            logger.warning(f"Unknown ISO: {iso_name}")
            return pd.DataFrame()

        try:
            iso = iso_class()
            df = iso.get_interconnection_queue()
            if df is not None and not df.empty:
                df["iso"] = iso_name.upper()
                logger.info(f"  {iso_name}: {len(df)} projects in queue")
                return df
            else:
                logger.warning(f"  {iso_name}: No queue data returned")
                return pd.DataFrame()
        except Exception as e:
            logger.warning(f"  {iso_name} queue fetch failed: {e}")
            return pd.DataFrame()

    def _get_miso_queue_direct(self) -> pd.DataFrame:
        """Fetch MISO queue directly from their JSON API (fallback)."""
        try:
            data = self.client.get(
                ISO_QUEUE_URLS["MISO"],
                cache_hours=self.cache_hours,
            )

            if not data:
                return pd.DataFrame()

            # MISO returns a list of project dicts
            projects = data if isinstance(data, list) else data.get("data", data)
            df = pd.DataFrame(projects)

            if df.empty:
                return df

            # Standardize column names
            col_map = {
                "projectName": "project_name",
                "poiName": "poi_name",
                "county": "county",
                "state": "state",
                "fuelType": "fuel_type",
                "queueDate": "queue_date",
                "summerMW": "capacity_mw",
                "winterMW": "winter_capacity_mw",
                "studyPhase": "status",
                "inService": "in_service_date",
                "applicationStatus": "application_status",
            }
            for old, new in col_map.items():
                if old in df.columns:
                    df.rename(columns={old: new}, inplace=True)

            df["iso"] = "MISO"
            logger.info(f"  MISO (direct): {len(df)} projects")
            return df

        except Exception as e:
            logger.warning(f"  MISO direct fetch failed: {e}")
            return pd.DataFrame()

    def _download_excel_queue(self, url: str, iso_name: str, **read_kwargs) -> pd.DataFrame:
        """Download an Excel file from an ISO and parse it into a DataFrame."""
        cache_file = self.cache_dir / f"queue_{iso_name.lower()}.xlsx"

        # Check cache
        if cache_file.exists():
            import time
            age_hours = (time.time() - cache_file.stat().st_mtime) / 3600
            if age_hours < self.cache_hours:
                logger.info(f"  {iso_name}: Using cached queue data ({age_hours:.0f}h old)")
                try:
                    df = pd.read_excel(cache_file, **read_kwargs)
                    df["iso"] = iso_name
                    return df
                except Exception:
                    pass  # Re-download if cache is corrupt

        logger.info(f"  {iso_name}: Downloading queue data...")
        try:
            response = self.client.session.get(url, timeout=120)
            response.raise_for_status()

            with open(cache_file, "wb") as f:
                f.write(response.content)

            df = pd.read_excel(BytesIO(response.content), **read_kwargs)
            df["iso"] = iso_name
            logger.info(f"  {iso_name}: {len(df)} projects downloaded")
            return df

        except Exception as e:
            logger.warning(f"  {iso_name} download failed: {e}")
            return pd.DataFrame()

    def _get_ercot_queue_direct(self) -> pd.DataFrame:
        """Fetch ERCOT interconnection queue from their Excel download."""
        url = ISO_QUEUE_URLS.get("ERCOT", "")
        if not url:
            return pd.DataFrame()

        try:
            df = self._download_excel_queue(url, "ERCOT")
            if df.empty:
                return df

            # ERCOT column mapping — they use various column names
            col_map = {
                "INR": "queue_id",
                "Project Name": "project_name",
                "Fuel": "fuel_type",
                "Technology": "fuel_type",
                "County": "county",
                "Capacity (MW)": "capacity_mw",
                "Summer Capacity (MW)": "capacity_mw",
                "MW": "capacity_mw",
                "Status": "status",
                "Screening Study Started": "queue_date",
                "POI Location": "poi_name",
                "Interconnection Location": "poi_name",
                "GIM Study Phase": "status",
            }
            for old, new in col_map.items():
                if old in df.columns and new not in df.columns:
                    df.rename(columns={old: new}, inplace=True)

            df["state"] = "TX"  # ERCOT is Texas-only
            return df

        except Exception as e:
            logger.warning(f"  ERCOT direct download failed: {e}")
            return pd.DataFrame()

    def _get_pjm_queue_direct(self) -> pd.DataFrame:
        """Fetch PJM interconnection queue from their Excel export."""
        url = ISO_QUEUE_URLS.get("PJM", "")
        if not url:
            return pd.DataFrame()

        try:
            df = self._download_excel_queue(url, "PJM")
            if df.empty:
                return df

            col_map = {
                "Queue Number": "queue_id",
                "Project Name": "project_name",
                "Name": "project_name",
                "County": "county",
                "State": "state",
                "Fuel": "fuel_type",
                "MFO": "capacity_mw",
                "MW Capacity": "capacity_mw",
                "MW In Service": "capacity_mw",
                "Status": "status",
                "Queue Date": "queue_date",
                "Transmission Owner": "poi_name",
                "Substation": "poi_name",
            }
            for old, new in col_map.items():
                if old in df.columns and new not in df.columns:
                    df.rename(columns={old: new}, inplace=True)

            return df

        except Exception as e:
            logger.warning(f"  PJM direct download failed: {e}")
            return pd.DataFrame()

    def _get_nyiso_queue_direct(self) -> pd.DataFrame:
        """Fetch NYISO interconnection queue from their Excel download."""
        url = ISO_QUEUE_URLS.get("NYISO", "")
        if not url:
            return pd.DataFrame()

        try:
            df = self._download_excel_queue(url, "NYISO")
            if df.empty:
                return df

            col_map = {
                "Queue Pos.": "queue_id",
                "Project Name": "project_name",
                "County": "county",
                "State": "state",
                "Type/ Fuel": "fuel_type",
                "Fuel Type": "fuel_type",
                "SP (MW)": "capacity_mw",
                "MW": "capacity_mw",
                "Status": "status",
                "Queue Date": "queue_date",
                "Date of IR": "queue_date",
                "POI": "poi_name",
                "Proposed In-Service": "in_service_date",
                "Developer": "developer",
            }
            for old, new in col_map.items():
                if old in df.columns and new not in df.columns:
                    df.rename(columns={old: new}, inplace=True)

            if "state" not in df.columns:
                df["state"] = "NY"

            return df

        except Exception as e:
            logger.warning(f"  NYISO direct download failed: {e}")
            return pd.DataFrame()

    def _get_spp_queue_direct(self) -> pd.DataFrame:
        """Fetch SPP interconnection queue from their portal."""
        # SPP provides queue data as a CSV/Excel from their ops portal
        url = ISO_QUEUE_URLS.get("SPP", "")
        if not url:
            return pd.DataFrame()

        try:
            df = self._download_excel_queue(url, "SPP")
            if df.empty:
                # Try CSV fallback
                cache_file = self.cache_dir / "queue_spp.csv"
                try:
                    response = self.client.session.get(url, timeout=120)
                    response.raise_for_status()
                    df = pd.read_csv(BytesIO(response.content))
                    df["iso"] = "SPP"
                except Exception:
                    return pd.DataFrame()

            col_map = {
                "Generation Interconnection Number": "queue_id",
                "Project Name": "project_name",
                "County": "county",
                "State": "state",
                "Fuel Type": "fuel_type",
                "Capacity": "capacity_mw",
                "MW": "capacity_mw",
                "Status": "status",
                "Queue Date": "queue_date",
                "POI": "poi_name",
                "In-Service Date": "in_service_date",
            }
            for old, new in col_map.items():
                if old in df.columns and new not in df.columns:
                    df.rename(columns={old: new}, inplace=True)

            return df

        except Exception as e:
            logger.warning(f"  SPP direct download failed: {e}")
            return pd.DataFrame()

    def _standardize_queue_df(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Standardize queue DataFrame columns to a common schema.

        Output columns: project_name, developer, fuel_type, fuel_category,
        capacity_mw, status, status_normalized, queue_date, poi_name,
        county, state, lat, lon, iso
        """
        if df.empty:
            return df

        result = pd.DataFrame()

        # Map common column names (gridstatus uses various names)
        name_candidates = [
            "Project Name", "project_name", "projectName", "Facility Name",
            "Queue ID", "queue_id",
        ]
        for col in name_candidates:
            if col in df.columns:
                result["project_name"] = df[col].astype(str)
                break
        if "project_name" not in result.columns:
            result["project_name"] = "Unknown"

        # Developer
        dev_candidates = [
            "Developer", "developer", "Interconnection Customer",
            "Entity Name", "entity_name", "entityName",
        ]
        for col in dev_candidates:
            if col in df.columns:
                result["developer"] = df[col].astype(str)
                break
        if "developer" not in result.columns:
            result["developer"] = ""

        # Fuel type
        fuel_candidates = [
            "Generation Type", "Fuel", "fuel_type", "fuelType",
            "Type", "Technology", "Fuel Type", "Resource",
        ]
        for col in fuel_candidates:
            if col in df.columns:
                result["fuel_type"] = df[col].astype(str)
                break
        if "fuel_type" not in result.columns:
            result["fuel_type"] = ""

        result["fuel_category"] = result["fuel_type"].apply(self._normalize_fuel)

        # Capacity
        cap_candidates = [
            "Capacity (MW)", "capacity_mw", "Summer Capacity (MW)",
            "summerMW", "MW", "Nameplate Capacity (MW)", "Proposed MW",
        ]
        for col in cap_candidates:
            if col in df.columns:
                result["capacity_mw"] = pd.to_numeric(df[col], errors="coerce").fillna(0)
                break
        if "capacity_mw" not in result.columns:
            result["capacity_mw"] = 0

        # Status
        status_candidates = [
            "Status", "status", "Queue Status", "studyPhase",
            "application_status", "Study Phase",
        ]
        for col in status_candidates:
            if col in df.columns:
                result["status"] = df[col].astype(str)
                break
        if "status" not in result.columns:
            result["status"] = "Unknown"

        result["status_normalized"] = result["status"].apply(self._normalize_status)

        # Queue date
        date_candidates = [
            "Queue Date", "queue_date", "queueDate",
            "Request Date", "Application Date",
        ]
        for col in date_candidates:
            if col in df.columns:
                result["queue_date"] = pd.to_datetime(df[col], errors="coerce")
                break
        if "queue_date" not in result.columns:
            result["queue_date"] = pd.NaT

        # Point of Interconnection
        poi_candidates = [
            "POI Name", "poi_name", "poiName",
            "Point of Interconnection", "Substation",
            "Interconnection Location", "Transmission Owner",
        ]
        for col in poi_candidates:
            if col in df.columns:
                result["poi_name"] = df[col].astype(str)
                break
        if "poi_name" not in result.columns:
            result["poi_name"] = ""

        # Location
        for col in ["county", "County"]:
            if col in df.columns:
                result["county"] = df[col].astype(str)
                break
        if "county" not in result.columns:
            result["county"] = ""

        for col in ["state", "State"]:
            if col in df.columns:
                result["state"] = df[col].astype(str)
                break
        if "state" not in result.columns:
            result["state"] = ""

        # Coordinates (if available)
        for col in ["Latitude", "latitude", "lat"]:
            if col in df.columns:
                result["lat"] = pd.to_numeric(df[col], errors="coerce")
                break
        if "lat" not in result.columns:
            result["lat"] = None

        for col in ["Longitude", "longitude", "lon"]:
            if col in df.columns:
                result["lon"] = pd.to_numeric(df[col], errors="coerce")
                break
        if "lon" not in result.columns:
            result["lon"] = None

        # ISO
        if "iso" in df.columns:
            result["iso"] = df["iso"]
        else:
            result["iso"] = ""

        return result

    def get_queue_by_iso(self, iso_name: str) -> pd.DataFrame:
        """
        Fetch interconnection queue for a single ISO.

        Args:
            iso_name: ISO identifier (ERCOT, CAISO, PJM, MISO, SPP, NYISO, ISONE)

        Returns:
            Standardized DataFrame with queue projects.
        """
        iso_upper = iso_name.upper()
        logger.info(f"Fetching {iso_upper} interconnection queue...")

        if self._gridstatus_available:
            raw_df = self._get_queue_gridstatus(iso_upper)
        else:
            # Direct download fallback — route to the right method
            direct_methods = {
                "MISO": self._get_miso_queue_direct,
                "ERCOT": self._get_ercot_queue_direct,
                "PJM": self._get_pjm_queue_direct,
                "NYISO": self._get_nyiso_queue_direct,
                "SPP": self._get_spp_queue_direct,
            }
            method = direct_methods.get(iso_upper)
            if method:
                raw_df = method()
            else:
                logger.warning(
                    f"No direct download available for {iso_upper}. "
                    f"Install gridstatus (requires Python 3.10+) for full coverage."
                )
                return pd.DataFrame()

        return self._standardize_queue_df(raw_df)

    def get_all_queues(
        self,
        isos: Optional[List[str]] = None,
    ) -> pd.DataFrame:
        """
        Fetch interconnection queues from all (or specified) ISOs.

        Args:
            isos: List of ISO names. If None, uses config or all supported.

        Returns:
            Combined standardized DataFrame.
        """
        if isos is None:
            isos = self.queue_config.get("isos", self.SUPPORTED_ISOS)

        logger.info(f"Fetching interconnection queues from {len(isos)} ISOs...")

        all_queues = []
        for iso in isos:
            try:
                df = self.get_queue_by_iso(iso)
                if not df.empty:
                    all_queues.append(df)
            except Exception as e:
                logger.warning(f"Failed to fetch {iso} queue: {e}")

        if not all_queues:
            logger.warning("No interconnection queue data retrieved from any ISO")
            return pd.DataFrame()

        combined = pd.concat(all_queues, ignore_index=True)

        # Filter by config statuses if specified
        include_statuses = self.queue_config.get("include_statuses")
        if include_statuses:
            status_lower = [s.lower() for s in include_statuses]
            mask = combined["status_normalized"].str.lower().isin(status_lower)
            before = len(combined)
            combined = combined[mask].copy()
            logger.info(
                f"Filtered to {len(combined)} active projects "
                f"(from {before} total, statuses: {include_statuses})"
            )

        logger.info(f"\nCombined queue: {len(combined)} projects across {len(isos)} ISOs")
        logger.info(
            f"Total proposed capacity: {combined['capacity_mw'].sum():,.0f} MW"
        )

        # Fuel breakdown
        if "fuel_category" in combined.columns:
            fuel_summary = (
                combined.groupby("fuel_category")["capacity_mw"]
                .sum()
                .sort_values(ascending=False)
            )
            logger.info(f"Queue fuel mix (MW):\n{fuel_summary.to_string()}")

        # ISO breakdown
        iso_summary = (
            combined.groupby("iso")["capacity_mw"]
            .agg(["count", "sum"])
            .rename(columns={"count": "projects", "sum": "capacity_mw"})
            .sort_values("capacity_mw", ascending=False)
        )
        logger.info(f"Queue by ISO:\n{iso_summary.to_string()}")

        return combined

    def get_projects_near_point(
        self,
        all_queues: pd.DataFrame,
        lat: float,
        lon: float,
        radius_miles: float = 25.0,
    ) -> pd.DataFrame:
        """
        Filter queue projects to those within a radius of a point.

        Only works for projects that have lat/lon coordinates.
        """
        if all_queues.empty:
            return all_queues

        # Filter to projects with coordinates
        with_coords = all_queues.dropna(subset=["lat", "lon"])
        if with_coords.empty:
            return pd.DataFrame()

        # Calculate distances
        with_coords = with_coords.copy()
        with_coords["distance_mi"] = with_coords.apply(
            lambda row: haversine_distance(lat, lon, row["lat"], row["lon"]),
            axis=1,
        )

        nearby = with_coords[with_coords["distance_mi"] <= radius_miles].copy()
        return nearby.sort_values("distance_mi")

    def get_queue_at_substation(
        self,
        all_queues: pd.DataFrame,
        substation_name: str,
    ) -> pd.DataFrame:
        """
        Find queued projects whose POI matches a substation name.

        Uses fuzzy string matching on POI name.
        """
        if all_queues.empty or not substation_name:
            return pd.DataFrame()

        sub_upper = substation_name.upper().strip()

        # Exact or substring match on POI name
        mask = all_queues["poi_name"].str.upper().str.strip().apply(
            lambda poi: sub_upper in str(poi) or str(poi) in sub_upper
            if poi and not pd.isna(poi) else False
        )

        matches = all_queues[mask].copy()

        if not matches.empty:
            logger.debug(
                f"Found {len(matches)} queued projects at '{substation_name}'"
            )

        return matches

    def to_geodataframe(self, queue_df: pd.DataFrame) -> gpd.GeoDataFrame:
        """Convert queue DataFrame to GeoDataFrame (for projects with coordinates)."""
        if queue_df.empty:
            return gpd.GeoDataFrame()

        with_coords = queue_df.dropna(subset=["lat", "lon"]).copy()
        if with_coords.empty:
            return gpd.GeoDataFrame()

        geometry = [
            Point(row["lon"], row["lat"]) for _, row in with_coords.iterrows()
        ]
        return gpd.GeoDataFrame(with_coords, geometry=geometry, crs=WGS84)

    def get_queue_summary(self, queue_df: pd.DataFrame) -> dict:
        """
        Generate a summary of the interconnection queue.

        Returns dict suitable for dashboard display.
        """
        if queue_df.empty:
            return {
                "total_projects": 0,
                "total_capacity_mw": 0,
                "by_fuel": {},
                "by_iso": {},
                "by_status": {},
            }

        by_fuel = {}
        total_mw = queue_df["capacity_mw"].sum()
        for fuel, group in queue_df.groupby("fuel_category"):
            mw = group["capacity_mw"].sum()
            by_fuel[fuel] = {
                "count": len(group),
                "capacity_mw": round(mw, 1),
                "pct": round(100 * mw / total_mw, 1) if total_mw > 0 else 0,
            }

        by_iso = {}
        for iso, group in queue_df.groupby("iso"):
            by_iso[iso] = {
                "count": len(group),
                "capacity_mw": round(group["capacity_mw"].sum(), 1),
            }

        by_status = {}
        for status, group in queue_df.groupby("status_normalized"):
            by_status[status] = {
                "count": len(group),
                "capacity_mw": round(group["capacity_mw"].sum(), 1),
            }

        return {
            "total_projects": len(queue_df),
            "total_capacity_mw": round(total_mw, 1),
            "by_fuel": dict(sorted(by_fuel.items(), key=lambda x: x[1]["capacity_mw"], reverse=True)),
            "by_iso": dict(sorted(by_iso.items(), key=lambda x: x[1]["capacity_mw"], reverse=True)),
            "by_status": dict(sorted(by_status.items(), key=lambda x: x[1]["count"], reverse=True)),
        }
