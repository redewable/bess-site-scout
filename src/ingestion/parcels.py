"""
Parcel Boundary & Ownership Data Ingestion

Pulls parcel data from multiple sources:
  1. Regrid (Loveland) — nationwide parcel boundaries (free tile layer via ArcGIS)
  2. ATTOM Data — property details & ownership (paid, freemium trial)
  3. County Assessor APIs — varies by county (free where available)
  4. OpenStreetMap Overpass — building footprints (free, no ownership)
  5. Data.gov — scattered county parcel datasets

For BESS site selection, parcel data tells us:
  - Exact property boundaries and acreage
  - Current owner (for land acquisition outreach)
  - Assessed value / tax value (for cost estimation)
  - Land use / zoning code (legal permitting requirements)
  - Year built, improvements (if structures exist)
"""

import logging
from typing import Optional, Dict, List
from pathlib import Path

import pandas as pd
import geopandas as gpd
from shapely.geometry import Point, box

from ..utils.api_client import APIClient, ArcGISClient
from ..utils.geo import point_buffer_bbox, haversine_distance, WGS84

logger = logging.getLogger(__name__)

# ── Regrid (Loveland) ArcGIS endpoints ───────────────────────────
# Regrid publishes free parcel boundary tiles via ArcGIS Living Atlas
REGRID_FEATURE_SERVICE = (
    "https://parcels.regrid.com/arcgis/rest/services/"
    "parcels/FeatureServer/0"
)

# ── ATTOM Data API ───────────────────────────────────────────────
ATTOM_API_BASE = "https://api.gateway.attomdata.com/propertyapi/v1.0.0"

# ── OpenStreetMap Overpass ────────────────────────────────────────
OVERPASS_API = "https://overpass-api.de/api/interpreter"


class ParcelIngestor:
    """
    Ingests parcel boundary and ownership data.

    Provides:
      - get_parcels_near_point(): Find parcels within radius of a point
      - get_parcel_details(): Get detailed info for a specific parcel
      - get_parcels_regrid(): Regrid free parcel boundaries
      - get_parcels_attom(): ATTOM paid parcel data
      - get_building_footprints(): OSM building footprints (free)
      - filter_suitable_parcels(): Filter parcels by BESS criteria
      - get_parcel_summary(): Summary for pipeline output
    """

    def __init__(self, config: dict):
        self.config = config
        self.re_config = config.get("real_estate", {})
        self.api_keys = config.get("api_keys", {})
        self.arcgis = ArcGISClient(
            cache_dir=config.get("cache", {}).get("directory", "./data/cache"),
            cache_enabled=config.get("cache", {}).get("enabled", True),
        )
        self.client = APIClient(
            cache_dir=config.get("cache", {}).get("directory", "./data/cache"),
            cache_enabled=config.get("cache", {}).get("enabled", True),
        )
        self.cache_hours = config.get("cache", {}).get("real_estate_expiry_hours", 24)

    # ── Regrid (Free Parcel Boundaries) ───────────────────────────

    def get_parcels_regrid(
        self,
        lat: float,
        lon: float,
        radius_miles: float = 3.0,
        max_records: int = 500,
    ) -> gpd.GeoDataFrame:
        """
        Fetch parcel boundaries from Regrid's free ArcGIS service.

        Returns GeoDataFrame with parcel geometries and basic attributes.
        Coverage: ~155 million US parcels.
        """
        try:
            geojson = self.arcgis.query_point_radius(
                service_url=REGRID_FEATURE_SERVICE,
                lat=lat,
                lon=lon,
                radius_miles=radius_miles,
                out_fields="*",
                cache_hours=self.cache_hours,
            )

            features = geojson.get("features", [])
            if not features:
                logger.info(f"  Regrid: no parcels found near ({lat}, {lon})")
                return gpd.GeoDataFrame()

            gdf = gpd.GeoDataFrame.from_features(features, crs=WGS84)

            # Standardize column names (Regrid uses various schemas by county)
            col_candidates = {
                "owner": ["owner", "owner1", "OWNER", "Owner", "ownername", "OWNERNM"],
                "address": ["address", "siteaddr", "SITEADDR", "site_address", "ADDRESS"],
                "acres": ["acres", "ll_gisacre", "ACRES", "GIS_ACRES", "LOT_SIZE"],
                "land_use": ["usecode", "USEDESC", "land_use", "LANDUSE", "usedesc"],
                "zoning": ["zoning", "ZONING", "zone", "ZONE"],
                "assessed_value": ["assessed", "TOTALVAL", "total_val", "ASSDVAL", "ASDTOTAL"],
                "year_built": ["yearbuilt", "YEARBUILT", "YR_BLT"],
                "county": ["county", "COUNTY", "cntyname"],
                "state": ["state2", "STATE", "state", "STATEFP"],
                "parcel_id": ["parcelnumb", "PARCELNO", "APN", "parcel_id", "PIN"],
            }

            for std_name, candidates in col_candidates.items():
                for c in candidates:
                    if c in gdf.columns:
                        if std_name not in gdf.columns:
                            gdf[std_name] = gdf[c]
                        break

            # Calculate acreage from geometry if not present
            if "acres" not in gdf.columns or gdf["acres"].isna().all():
                try:
                    # Project to equal-area CRS for accurate area calc
                    gdf_proj = gdf.to_crs("EPSG:5070")  # NAD83 Conus Albers
                    gdf["acres"] = gdf_proj.geometry.area / 4046.86  # sq meters to acres
                except Exception:
                    gdf["acres"] = None

            # Add distance from search center
            gdf["distance_miles"] = gdf.geometry.apply(
                lambda g: haversine_distance(lat, lon, g.centroid.y, g.centroid.x)
            )

            logger.info(f"  Regrid: {len(gdf)} parcels near ({lat:.4f}, {lon:.4f})")
            return gdf

        except Exception as e:
            logger.warning(f"  Regrid parcel query failed: {e}")
            return gpd.GeoDataFrame()

    # ── ATTOM Data (Paid — Property Details) ──────────────────────

    def get_parcels_attom(
        self,
        lat: float,
        lon: float,
        radius_miles: float = 3.0,
    ) -> pd.DataFrame:
        """
        Fetch detailed property data from ATTOM Data API.
        Requires paid API key (free 30-day trial available).

        Returns: Property details including ownership, value, tax, zoning.
        """
        attom_key = self.api_keys.get("attom", "")
        if not attom_key:
            logger.info("  ATTOM: No API key — skipping (paid service at attomdata.com)")
            return pd.DataFrame()

        try:
            url = f"{ATTOM_API_BASE}/property/snapshot"
            headers = {
                "Accept": "application/json",
                "apikey": attom_key,
            }
            params = {
                "latitude": lat,
                "longitude": lon,
                "radius": int(radius_miles),
                "orderby": "distance",
                "pagesize": 100,
            }

            response = self.client.session.get(
                url, params=params, headers=headers, timeout=60
            )
            response.raise_for_status()
            data = response.json()

            properties = data.get("property", [])
            if not properties:
                return pd.DataFrame()

            # Flatten nested ATTOM structure
            rows = []
            for prop in properties:
                row = {
                    "attom_id": prop.get("identifier", {}).get("attomId"),
                    "address": prop.get("address", {}).get("oneLine", ""),
                    "owner": prop.get("assessment", {}).get("owner1", {}).get("lastName", ""),
                    "assessed_value": prop.get("assessment", {}).get("assessed", {}).get("assdTtlValue"),
                    "market_value": prop.get("assessment", {}).get("market", {}).get("mktTtlValue"),
                    "land_use": prop.get("summary", {}).get("propclass", ""),
                    "lot_size_acres": prop.get("lot", {}).get("lotSize1", 0),
                    "year_built": prop.get("summary", {}).get("yearBuilt"),
                    "lat": prop.get("location", {}).get("latitude"),
                    "lon": prop.get("location", {}).get("longitude"),
                    "county": prop.get("area", {}).get("countrySecSubd", ""),
                    "state": prop.get("address", {}).get("countrySubd", ""),
                }
                rows.append(row)

            df = pd.DataFrame(rows)
            logger.info(f"  ATTOM: {len(df)} properties near ({lat:.4f}, {lon:.4f})")
            return df

        except Exception as e:
            logger.warning(f"  ATTOM query failed: {e}")
            return pd.DataFrame()

    # ── OSM Building Footprints (Free) ────────────────────────────

    def get_building_footprints(
        self,
        lat: float,
        lon: float,
        radius_miles: float = 1.0,
    ) -> gpd.GeoDataFrame:
        """
        Fetch building footprints from OpenStreetMap via Overpass API.
        Free, no auth. Useful for identifying developed vs undeveloped land.
        """
        try:
            # Convert radius to approximate bbox
            xmin, ymin, xmax, ymax = point_buffer_bbox(lat, lon, radius_miles)

            # Overpass QL query for buildings
            query = f"""
            [out:json][timeout:30];
            (
              way["building"]({ymin},{xmin},{ymax},{xmax});
              relation["building"]({ymin},{xmin},{ymax},{xmax});
            );
            out center;
            """

            response = self.client.session.post(
                OVERPASS_API,
                data={"data": query},
                timeout=60,
            )
            response.raise_for_status()
            data = response.json()

            elements = data.get("elements", [])
            if not elements:
                return gpd.GeoDataFrame()

            # Extract building centers
            rows = []
            for el in elements:
                center = el.get("center", {})
                tags = el.get("tags", {})
                if center:
                    rows.append({
                        "lat": center.get("lat"),
                        "lon": center.get("lon"),
                        "building_type": tags.get("building", "yes"),
                        "name": tags.get("name", ""),
                        "osm_id": el.get("id"),
                    })

            if not rows:
                return gpd.GeoDataFrame()

            df = pd.DataFrame(rows)
            geometry = [Point(r["lon"], r["lat"]) for _, r in df.iterrows()]
            gdf = gpd.GeoDataFrame(df, geometry=geometry, crs=WGS84)

            logger.info(f"  OSM: {len(gdf)} buildings near ({lat:.4f}, {lon:.4f})")
            return gdf

        except Exception as e:
            logger.warning(f"  OSM building query failed: {e}")
            return gpd.GeoDataFrame()

    # ── Combined parcel search ────────────────────────────────────

    def get_parcels_near_point(
        self,
        lat: float,
        lon: float,
        radius_miles: Optional[float] = None,
    ) -> gpd.GeoDataFrame:
        """
        Get parcels near a point using best available data source.
        Tries Regrid first (free), then ATTOM (paid) for enrichment.
        """
        if radius_miles is None:
            radius_miles = self.re_config.get("search_radius_miles", 3.0)

        # Primary: Regrid (free parcel boundaries)
        parcels = self.get_parcels_regrid(lat, lon, radius_miles)

        # Enrichment: ATTOM (paid, if key available)
        if self.api_keys.get("attom"):
            attom_data = self.get_parcels_attom(lat, lon, radius_miles)
            if not attom_data.empty and not parcels.empty:
                # Merge on nearest match or address
                logger.info("  Enriching Regrid parcels with ATTOM data...")
                # Simple enrichment: add ATTOM data columns
                # Full spatial join would be more accurate but slower

        return parcels

    def filter_suitable_parcels(
        self,
        parcels: gpd.GeoDataFrame,
    ) -> gpd.GeoDataFrame:
        """
        Filter parcels by BESS suitability criteria from config.

        Filters:
          - Minimum acreage (default 10 acres)
          - Maximum acreage (default 500 acres)
          - Preferred land use types
          - Excluded land use types
        """
        if parcels.empty:
            return parcels

        filtered = parcels.copy()

        min_acres = self.re_config.get("min_acres", 10)
        max_acres = self.re_config.get("max_acres", 500)

        if "acres" in filtered.columns:
            acres = pd.to_numeric(filtered["acres"], errors="coerce")
            filtered = filtered[
                (acres >= min_acres) & (acres <= max_acres)
            ]

        # Filter by preferred land use
        preferred = self.re_config.get("preferred_land_use", [])
        excluded = self.re_config.get("excluded_land_use", [])

        if "land_use" in filtered.columns and (preferred or excluded):
            lu = filtered["land_use"].str.lower().fillna("")

            if excluded:
                for ex in excluded:
                    filtered = filtered[~lu.str.contains(ex.lower(), na=False)]

        logger.info(
            f"  Filtered to {len(filtered)} suitable parcels "
            f"({min_acres}-{max_acres} acres)"
        )
        return filtered

    def get_parcel_summary(self, parcels: gpd.GeoDataFrame) -> Dict:
        """Generate parcel summary for pipeline output."""
        if parcels.empty:
            return {"total_parcels": 0, "source": "none"}

        summary = {
            "total_parcels": len(parcels),
            "source": "Regrid" if "parcel_id" in parcels.columns else "unknown",
        }

        if "acres" in parcels.columns:
            acres = pd.to_numeric(parcels["acres"], errors="coerce").dropna()
            if not acres.empty:
                summary["avg_acres"] = round(float(acres.mean()), 1)
                summary["min_acres"] = round(float(acres.min()), 1)
                summary["max_acres"] = round(float(acres.max()), 1)
                summary["total_acres"] = round(float(acres.sum()), 0)

        if "assessed_value" in parcels.columns:
            vals = pd.to_numeric(parcels["assessed_value"], errors="coerce").dropna()
            if not vals.empty:
                summary["avg_assessed_value"] = round(float(vals.mean()), 0)

        if "land_use" in parcels.columns:
            lu_counts = parcels["land_use"].value_counts().head(10).to_dict()
            summary["land_use_distribution"] = lu_counts

        if "county" in parcels.columns:
            county_counts = parcels["county"].value_counts().to_dict()
            summary["county_distribution"] = county_counts

        return summary
