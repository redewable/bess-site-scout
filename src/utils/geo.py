"""
Geospatial utility functions for coordinate transforms,
distance calculations, and geometry operations.
"""

import math
from typing import Tuple, Optional

import geopandas as gpd
import pandas as pd
from shapely.geometry import Point, Polygon, box, shape
from shapely.ops import unary_union
from pyproj import Transformer


# WGS84 to Texas State Plane (meters) for accurate distance calcs
WGS84 = "EPSG:4326"
TX_STATE_PLANE = "EPSG:3081"  # Texas State Mapping System (Lambert Conformal Conic)

_transformer_to_meters = Transformer.from_crs(WGS84, TX_STATE_PLANE, always_xy=True)
_transformer_to_wgs84 = Transformer.from_crs(TX_STATE_PLANE, WGS84, always_xy=True)


def miles_to_meters(miles: float) -> float:
    return miles * 1609.344


def meters_to_miles(meters: float) -> float:
    return meters / 1609.344


def haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    Calculate the great-circle distance between two points (in miles).
    """
    R = 3958.8  # Earth radius in miles
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)

    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c


def point_buffer_bbox(lat: float, lon: float, radius_miles: float) -> Tuple[float, float, float, float]:
    """
    Create a bounding box around a point.
    Returns (xmin, ymin, xmax, ymax) in WGS84.
    """
    # Approximate degrees per mile at this latitude
    lat_per_mile = 1 / 69.0
    lon_per_mile = 1 / (69.0 * math.cos(math.radians(lat)))

    xmin = lon - radius_miles * lon_per_mile
    xmax = lon + radius_miles * lon_per_mile
    ymin = lat - radius_miles * lat_per_mile
    ymax = lat + radius_miles * lat_per_mile

    return (xmin, ymin, xmax, ymax)


def point_buffer_circle(lat: float, lon: float, radius_miles: float, n_points: int = 64) -> Polygon:
    """
    Create a circular buffer polygon around a point.
    Returns a Shapely Polygon in WGS84.
    """
    # Project to meters, buffer, project back
    x, y = _transformer_to_meters.transform(lon, lat)
    point = Point(x, y)
    buffer = point.buffer(miles_to_meters(radius_miles), resolution=n_points // 4)

    # Transform back to WGS84
    coords = []
    for bx, by in buffer.exterior.coords:
        lon_out, lat_out = _transformer_to_wgs84.transform(bx, by)
        coords.append((lon_out, lat_out))

    return Polygon(coords)


def geojson_to_geodataframe(geojson: dict) -> gpd.GeoDataFrame:
    """Convert a GeoJSON FeatureCollection to a GeoDataFrame."""
    if not geojson.get("features"):
        return gpd.GeoDataFrame()
    return gpd.GeoDataFrame.from_features(geojson["features"], crs=WGS84)


def geodataframe_to_geojson(gdf: gpd.GeoDataFrame) -> dict:
    """Convert a GeoDataFrame to GeoJSON dict."""
    return json.loads(gdf.to_json())


def filter_by_state(gdf: gpd.GeoDataFrame, state_col: str = "STATE", state: str = "TX") -> gpd.GeoDataFrame:
    """Filter a GeoDataFrame to a specific state."""
    return gdf[gdf[state_col].str.upper() == state.upper()].copy()


def calculate_distances(
    points_gdf: gpd.GeoDataFrame,
    target_lat: float,
    target_lon: float,
) -> pd.Series:
    """
    Calculate distances (in miles) from each point in GeoDataFrame
    to a target location.
    """
    target = Point(target_lon, target_lat)
    distances = points_gdf.geometry.apply(
        lambda g: haversine_distance(
            target_lat, target_lon,
            g.centroid.y, g.centroid.x
        )
    )
    return distances


def check_intersection(
    parcel_geometry,
    overlay_gdf: gpd.GeoDataFrame,
) -> Tuple[bool, float]:
    """
    Check if a parcel geometry intersects with any features in overlay GDF.
    Returns (intersects: bool, intersection_pct: float).
    """
    if overlay_gdf.empty:
        return False, 0.0

    # Ensure same CRS
    if hasattr(parcel_geometry, 'crs'):
        parcel_geom = parcel_geometry.geometry.iloc[0]
    else:
        parcel_geom = parcel_geometry

    parcel_area = parcel_geom.area
    if parcel_area == 0:
        return False, 0.0

    intersecting = overlay_gdf[overlay_gdf.intersects(parcel_geom)]
    if intersecting.empty:
        return False, 0.0

    # Calculate intersection area as percentage
    intersection = unary_union(intersecting.geometry).intersection(parcel_geom)
    pct = (intersection.area / parcel_area) * 100

    return True, pct


def count_features_in_radius(
    features_gdf: gpd.GeoDataFrame,
    center_lat: float,
    center_lon: float,
    radius_miles: float,
) -> int:
    """Count features within a radius of a center point."""
    if features_gdf.empty:
        return 0

    distances = calculate_distances(features_gdf, center_lat, center_lon)
    return int((distances <= radius_miles).sum())


# Need this import at module level for geodataframe_to_geojson
import json
