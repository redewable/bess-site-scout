"""
Export & Report Generation

Generates interactive maps (Folium), Excel reports, and summary dashboards.
"""

import logging
import json
from pathlib import Path
from typing import List, Dict, Any, Optional
from datetime import datetime

import pandas as pd

logger = logging.getLogger(__name__)


def export_to_excel(
    results: List[Dict[str, Any]],
    substations,
    output_dir: str = "./output",
    filename: str = None,
) -> str:
    """
    Export ranked results to an Excel workbook with multiple sheets.
    """
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    if filename is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"bess_site_scout_{timestamp}.xlsx"

    filepath = output_path / filename

    with pd.ExcelWriter(filepath, engine="openpyxl") as writer:
        # Sheet 1: Ranked Results
        if results:
            rows = []
            for r in results:
                row = {
                    "Rank": r.get("rank", ""),
                    "Grade": r.get("grade", ""),
                    "Composite Score": r.get("composite_score", ""),
                    "Substation": r.get("substation_name", ""),
                    "Distance (mi)": r.get("distance_to_substation_mi", ""),
                    "Voltage (kV)": r.get("substation_voltage_kv", ""),
                    "Latitude": r.get("lat", ""),
                    "Longitude": r.get("lon", ""),
                    "Env Score": r.get("environmental", {}).get("score", ""),
                    "Env Grade": r.get("environmental", {}).get("grade", ""),
                    "Flood Zone": r.get("flood", {}).get("flood_zone", ""),
                    "Risk Flags": "; ".join(r.get("risk_flags", [])),
                }
                # Add sub-scores
                for key, sub in r.get("sub_scores", {}).items():
                    row[f"Sub: {key}"] = sub.get("score", "")
                rows.append(row)

            df = pd.DataFrame(rows)
            df.to_excel(writer, sheet_name="Ranked Sites", index=False)

        # Sheet 2: Substation Summary
        if substations is not None and len(substations) > 0:
            sub_cols = ["NAME", "CITY", "STATE", "VOLT_CLASS", "OWNER", "lat", "lon"]
            available_cols = [c for c in sub_cols if c in substations.columns]
            substations[available_cols].to_excel(
                writer, sheet_name="Substations", index=False
            )

        # Sheet 3: Configuration
        config_df = pd.DataFrame([{
            "Generated": datetime.now().isoformat(),
            "Total Sites Evaluated": len(results),
            "Viable Sites": len([r for r in results if r.get("grade") != "ELIMINATED"]),
            "Eliminated": len([r for r in results if r.get("grade") == "ELIMINATED"]),
        }])
        config_df.to_excel(writer, sheet_name="Run Summary", index=False)

    logger.info(f"Excel report saved to {filepath}")
    return str(filepath)


def export_to_map(
    results: List[Dict[str, Any]],
    substations_gdf=None,
    transmission_gdf=None,
    output_dir: str = "./output",
    filename: str = None,
) -> str:
    """
    Generate an interactive Folium map with all results.
    """
    try:
        import folium
        from folium.plugins import MarkerCluster
    except ImportError:
        logger.error("Folium not installed. Run: pip install folium")
        return ""

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    if filename is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"bess_site_scout_map_{timestamp}.html"

    filepath = output_path / filename

    # Center map on Texas
    m = folium.Map(location=[31.0, -99.5], zoom_start=6, tiles="OpenStreetMap")

    # Add transmission lines layer
    if transmission_gdf is not None and len(transmission_gdf) > 0:
        line_group = folium.FeatureGroup(name="Transmission Lines (161-345kV)", show=True)
        for _, row in transmission_gdf.iterrows():
            if row.geometry and row.geometry.geom_type in ("LineString", "MultiLineString"):
                try:
                    coords = []
                    if row.geometry.geom_type == "LineString":
                        coords = [(c[1], c[0]) for c in row.geometry.coords]
                    else:
                        for line in row.geometry.geoms:
                            coords.extend([(c[1], c[0]) for c in line.coords])

                    popup_text = f"{row.get('VOLT_CLASS', 'N/A')} kV | {row.get('OWNER', 'N/A')}"
                    folium.PolyLine(
                        coords, weight=2, color="orange", opacity=0.6,
                        popup=popup_text,
                    ).add_to(line_group)
                except Exception:
                    pass
        line_group.add_to(m)

    # Add substations layer
    if substations_gdf is not None and len(substations_gdf) > 0:
        sub_group = folium.FeatureGroup(name="Substations", show=True)
        for _, row in substations_gdf.iterrows():
            lat = row.get("lat", row.geometry.y if row.geometry else None)
            lon = row.get("lon", row.geometry.x if row.geometry else None)
            if lat and lon:
                popup = (
                    f"<b>{row.get('NAME', 'Unknown')}</b><br>"
                    f"Voltage: {row.get('VOLT_CLASS', 'N/A')}<br>"
                    f"Owner: {row.get('OWNER', 'N/A')}<br>"
                    f"City: {row.get('CITY', 'N/A')}"
                )
                folium.CircleMarker(
                    location=[lat, lon],
                    radius=6,
                    color="blue",
                    fill=True,
                    fill_color="blue",
                    fill_opacity=0.7,
                    popup=folium.Popup(popup, max_width=300),
                ).add_to(sub_group)
        sub_group.add_to(m)

    # Add candidate sites
    if results:
        # Grade color mapping
        colors = {"A": "green", "B": "lightgreen", "C": "orange", "D": "red", "F": "darkred"}

        site_group = folium.FeatureGroup(name="Candidate Sites", show=True)
        for r in results:
            if r.get("grade") == "ELIMINATED":
                continue
            lat = r.get("lat")
            lon = r.get("lon")
            if lat and lon:
                color = colors.get(r.get("grade", "C"), "gray")
                popup = (
                    f"<b>Rank #{r.get('rank', '?')}</b> — Grade {r.get('grade')}<br>"
                    f"Score: {r.get('composite_score')}<br>"
                    f"Substation: {r.get('substation_name', 'N/A')}<br>"
                    f"Distance: {r.get('distance_to_substation_mi', '?')} mi<br>"
                    f"Env Score: {r.get('environmental', {}).get('score', '?')}<br>"
                    f"Flood: {r.get('flood', {}).get('flood_zone', '?')}<br>"
                    f"Flags: {'; '.join(r.get('risk_flags', [])[:3])}"
                )
                folium.Marker(
                    location=[lat, lon],
                    popup=folium.Popup(popup, max_width=350),
                    icon=folium.Icon(color=color, icon="bolt", prefix="fa"),
                ).add_to(site_group)
        site_group.add_to(m)

    # Add layer control
    folium.LayerControl().add_to(m)

    m.save(str(filepath))
    logger.info(f"Interactive map saved to {filepath}")
    return str(filepath)


def _flatten_dict(d: dict, prefix: str = "", out: dict = None) -> dict:
    """Flatten a nested dict into dot-separated keys for GeoJSON properties."""
    if out is None:
        out = {}
    for k, v in d.items():
        key = f"{prefix}_{k}" if prefix else k
        if isinstance(v, dict):
            _flatten_dict(v, prefix=key, out=out)
        elif isinstance(v, list):
            out[key] = "; ".join(str(item) for item in v) if v else ""
        elif isinstance(v, (bool,)):
            out[key] = v
        elif v is not None:
            out[key] = v
    return out


def export_geojson(
    results: List[Dict[str, Any]],
    output_dir: str = "./output",
    filename: str = None,
) -> str:
    """
    Export results as GeoJSON for the web dashboard.

    Flattens all nested screening data (sub_scores, environmental, flood, EPA,
    TCEQ, USFWS, EIA, NREL) into flat properties so the dashboard can display
    full scoring breakdowns and screening details.
    """
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    if filename is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"bess_sites_{timestamp}.geojson"

    filepath = output_path / filename

    features = []
    for r in results:
        lat = r.get("lat")
        lon = r.get("lon")
        if lat and lon:
            props = {}
            for k, v in r.items():
                if k in ("lat", "lon", "geometry"):
                    continue
                if isinstance(v, dict):
                    _flatten_dict(v, prefix=k, out=props)
                elif isinstance(v, list):
                    props[k] = "; ".join(str(item) for item in v) if v else ""
                else:
                    props[k] = v

            feature = {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [lon, lat]},
                "properties": props,
            }
            features.append(feature)

    geojson = {"type": "FeatureCollection", "features": features}

    with open(filepath, "w") as f:
        json.dump(geojson, f, indent=2)

    logger.info(f"GeoJSON exported to {filepath} ({len(features)} features)")
    return str(filepath)
