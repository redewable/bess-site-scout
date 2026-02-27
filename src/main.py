"""
BESS Site Scout — Main Orchestrator

Coordinates the full site prospecting pipeline:
1. Pull grid infrastructure data (substations + transmission lines)
2. For each qualifying substation, search for nearby parcels
3. Screen each candidate against environmental databases
4. Assess grid density and solar resource
5. Score and rank all candidates
6. Generate reports and maps
"""

import logging
import sys
from pathlib import Path
from typing import Optional
from datetime import datetime

import yaml

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("bess_site_scout")


def load_config(config_path: str = "config/config.yaml") -> dict:
    """Load configuration from YAML file."""
    path = Path(config_path)
    if not path.exists():
        logger.error(f"Config file not found: {config_path}")
        sys.exit(1)
    with open(path, "r") as f:
        return yaml.safe_load(f)


def run_pipeline(config: dict, test_mode: bool = False):
    """
    Execute the full site prospecting pipeline.

    Args:
        config: Configuration dictionary
        test_mode: If True, limit to first 5 substations for testing
    """
    from .ingestion.hifld import HIFLDIngestor
    from .ingestion.fema import FEMAIngestor
    from .ingestion.epa import EPAIngestor
    from .ingestion.tceq import TCEQIngestor
    from .ingestion.usfws import USFWSIngestor
    from .ingestion.eia import EIAIngestor
    from .ingestion.nrel import NRELIngestor
    from .scoring.environmental import EnvironmentalScorer
    from .scoring.composite import CompositeScorer
    from .utils.export import export_to_excel, export_to_map, export_geojson

    state_filter = config.get("grid", {}).get("state_filter", "ALL")

    logger.info("=" * 60)
    logger.info("BESS Site Scout — Starting Pipeline")
    logger.info(f"Timestamp: {datetime.now().isoformat()}")
    logger.info(f"Coverage: {'Nationwide (CONUS)' if state_filter == 'ALL' else state_filter}")
    logger.info("=" * 60)

    # =========================================================
    # PHASE 1: Grid Infrastructure
    # =========================================================
    logger.info("\n📡 PHASE 1: Fetching Grid Infrastructure Data...")

    hifld = HIFLDIngestor(config)
    grid_data = hifld.get_all_grid_data()

    substations = grid_data["substations_on_lines"]
    transmission_lines = grid_data["transmission_lines"]

    if substations.empty:
        logger.error("No qualifying substations found. Check config voltage filters.")
        return

    logger.info(f"✅ Found {len(substations)} qualifying substations")
    logger.info(f"✅ Found {len(transmission_lines)} qualifying transmission lines")

    # Limit for test mode
    if test_mode:
        substations = substations.head(5)
        logger.info(f"🧪 TEST MODE: Limited to {len(substations)} substations")

    # =========================================================
    # PHASE 2: Real Estate Search (placeholder)
    # =========================================================
    logger.info("\n🏠 PHASE 2: Searching for Nearby Parcels...")

    search_radius = config.get("real_estate", {}).get("search_radius_miles", 3.0)
    logger.info(f"  Search radius: {search_radius} miles per substation")
    logger.info(
        f"  ⚠️  Real estate data integration pending — using substation locations as proxies"
    )

    # =========================================================
    # PHASE 3: Environmental Screening + Grid Assessment
    # =========================================================
    logger.info("\n🔬 PHASE 3: Running Environmental Screening & Grid Assessment...")

    fema_ingestor = FEMAIngestor(config)
    epa_ingestor = EPAIngestor(config)
    tceq_ingestor = TCEQIngestor(config)
    usfws_ingestor = USFWSIngestor(config)
    eia_ingestor = EIAIngestor(config)
    nrel_ingestor = NRELIngestor(config)
    env_scorer = EnvironmentalScorer(config)
    composite_scorer = CompositeScorer(config)

    all_results = []
    total = len(substations)

    for idx, (row_idx, sub) in enumerate(substations.iterrows()):
        sub_name = sub.get("NAME", f"Substation_{row_idx}")
        lat = sub.get("lat", sub.geometry.y if sub.geometry else None)
        lon = sub.get("lon", sub.geometry.x if sub.geometry else None)

        if lat is None or lon is None:
            logger.warning(f"  Skipping {sub_name} — no coordinates")
            continue

        logger.info(
            f"\n  [{idx+1}/{total}] 🔍 Screening: {sub_name} ({lat:.4f}, {lon:.4f})"
        )

        # Determine voltage for scoring
        volt_class = sub.get("VOLT_CLASS", "")
        max_kv = sub.get("max_voltage_kv", 0)
        if max_kv >= 345:
            voltage_kv = 345
        elif max_kv >= 220:
            voltage_kv = 230
        elif max_kv >= 161:
            voltage_kv = 161
        elif "345" in str(volt_class):
            voltage_kv = 345
        elif "220" in str(volt_class) or "287" in str(volt_class):
            voltage_kv = 230
        elif "161" in str(volt_class):
            voltage_kv = 161
        else:
            voltage_kv = 138

        # --- Environmental screens ---
        try:
            fema_result = fema_ingestor.assess_flood_risk(lat, lon)
            logger.info(f"    FEMA: {fema_result['details']}")
        except Exception as e:
            logger.warning(f"    FEMA query failed: {e}")
            fema_result = {"risk_level": "unknown", "eliminate": False, "risk_flags": []}

        try:
            epa_result = epa_ingestor.run_full_screening(lat, lon)
            flag_count = len(epa_result.get("risk_flags", []))
            logger.info(f"    EPA: {flag_count} flag(s)")
        except Exception as e:
            logger.warning(f"    EPA query failed: {e}")
            epa_result = {"eliminate": False, "risk_flags": []}

        try:
            tceq_result = tceq_ingestor.run_full_screening(lat, lon)
            flag_count = len(tceq_result.get("risk_flags", []))
            logger.info(f"    TCEQ: {flag_count} flag(s)")
        except Exception as e:
            logger.warning(f"    TCEQ query failed: {e}")
            tceq_result = {"eliminate": False, "risk_flags": []}

        try:
            usfws_result = usfws_ingestor.run_full_screening(lat, lon)
            flag_count = len(usfws_result.get("risk_flags", []))
            logger.info(f"    USFWS: {flag_count} flag(s)")
        except Exception as e:
            logger.warning(f"    USFWS query failed: {e}")
            usfws_result = {"eliminate": False, "risk_flags": []}

        # --- Grid density assessment ---
        try:
            eia_result = eia_ingestor.assess_grid_density(lat, lon)
            logger.info(
                f"    EIA: {eia_result['nearby_plants']} plants, "
                f"{eia_result['nearby_capacity_mw']:.0f} MW nearby"
            )
        except Exception as e:
            logger.warning(f"    EIA query failed: {e}")
            eia_result = {"grid_density_score": 50, "risk_flags": []}

        # --- Solar resource ---
        try:
            nrel_result = nrel_ingestor.get_solar_resource(lat, lon)
            logger.info(
                f"    NREL: GHI={nrel_result['ghi_annual']} kWh/m²/day, "
                f"co-location={nrel_result['co_location_potential']}"
            )
        except Exception as e:
            logger.warning(f"    NREL query failed: {e}")
            nrel_result = {"solar_score": 50, "ghi_annual": 0}

        # --- Score ---
        env_score = env_scorer.score_parcel(
            fema_result, epa_result, tceq_result, usfws_result
        )

        composite = composite_scorer.score_site(
            distance_to_substation_mi=0.0,
            substation_voltage_kv=voltage_kv,
            environmental_score=env_score["score"],
            environmental_eliminate=env_score["eliminate"],
            price_per_acre=5000,   # Placeholder
            parcel_acres=40,       # Placeholder
            flood_risk_level=fema_result.get("risk_level", "unknown"),
            grid_density_score=eia_result.get("grid_density_score", 50),
            solar_score=nrel_result.get("solar_score", 50),
        )

        # Compile result — include all enrichment fields from HIFLD
        result = {
            "substation_name": sub_name,
            "substation_voltage_kv": voltage_kv,
            "volt_class": volt_class,
            "lat": lat,
            "lon": lon,
            "connected_lines": sub.get("connected_lines", 0),
            "owner": sub.get("OWNER", ""),
            "operator": sub.get("OPERATOR", ""),
            "sub_status": sub.get("STATUS", ""),
            "city": sub.get("CITY", ""),
            "state": sub.get("STATE", ""),
            "county": sub.get("COUNTY", ""),
            "sub_type": sub.get("TYPE", ""),
            "hifld_lines": sub.get("LINES", 0),
            "max_volt": sub.get("MAX_VOLT", 0),
            "min_volt": sub.get("MIN_VOLT", 0),
            "distance_to_substation_mi": 0.0,
            "composite_score": composite["composite_score"],
            "grade": composite["grade"],
            "sub_scores": composite.get("sub_scores", {}),
            "environmental": env_score,
            "flood": fema_result,
            "epa": epa_result,
            "tceq": tceq_result,
            "usfws": usfws_result,
            "eia": eia_result,
            "nrel": nrel_result,
            "risk_flags": env_score.get("risk_flags", []),
            "ghi_annual": nrel_result.get("ghi_annual", 0),
            "solar_co_location": nrel_result.get("co_location_potential", "unknown"),
            "nearby_generation_mw": eia_result.get("nearby_capacity_mw", 0),
        }
        all_results.append(result)

        status = (
            "❌ ELIMINATED"
            if composite["grade"] == "ELIMINATED"
            else f"✅ {composite['grade']} ({composite['composite_score']})"
        )
        logger.info(f"    Result: {status}")

    # =========================================================
    # PHASE 4: Rank & Report
    # =========================================================
    logger.info("\n📊 PHASE 4: Ranking & Generating Reports...")

    ranked = composite_scorer.rank_sites(all_results)
    top_n = config.get("output", {}).get("top_n_results", 100)

    logger.info(f"\n{'='*60}")
    logger.info(f"TOP {min(top_n, len(ranked))} SITES:")
    logger.info(f"{'='*60}")

    for site in ranked[:top_n]:
        logger.info(
            f"  #{site['rank']:3d} | {site['grade']} | "
            f"Score: {site['composite_score']:5.1f} | "
            f"{site['substation_name'][:30]:30s} | "
            f"{site['volt_class']} | "
            f"Env: {site['environmental']['grade']} | "
            f"GHI: {site.get('ghi_annual', 0):.1f}"
        )

    # Export
    output_config = config.get("output", {})
    output_dir = output_config.get("report_dir", "./output")

    if output_config.get("generate_excel", True):
        excel_path = export_to_excel(ranked, substations, output_dir)
        logger.info(f"📄 Excel: {excel_path}")

    if output_config.get("generate_map", True):
        map_path = export_to_map(
            ranked[:top_n],
            substations_gdf=substations,
            transmission_gdf=transmission_lines,
            output_dir=output_dir,
        )
        logger.info(f"🗺️  Map: {map_path}")

    geojson_path = export_geojson(ranked, output_dir)
    logger.info(f"📍 GeoJSON: {geojson_path}")

    # Summary
    eliminated = len(all_results) - len(ranked)
    a_grade = len([r for r in ranked if r["grade"] == "A"])
    b_grade = len([r for r in ranked if r["grade"] == "B"])

    logger.info(f"\n{'='*60}")
    logger.info("SUMMARY")
    logger.info(f"{'='*60}")
    logger.info(f"  Total substations screened: {len(all_results)}")
    logger.info(f"  Eliminated: {eliminated}")
    logger.info(f"  Viable: {len(ranked)}")
    logger.info(f"  Grade A: {a_grade}")
    logger.info(f"  Grade B: {b_grade}")
    logger.info(f"{'='*60}")

    return {
        "ranked_sites": ranked,
        "eliminated_count": eliminated,
        "total_screened": len(all_results),
    }


def main():
    """CLI entry point."""
    import argparse

    parser = argparse.ArgumentParser(
        description="BESS Site Scout — Automated Site Prospecting"
    )
    parser.add_argument(
        "--config", "-c",
        default="config/config.yaml",
        help="Path to config file (default: config/config.yaml)",
    )
    parser.add_argument(
        "--test", "-t",
        action="store_true",
        help="Run in test mode (limit to 5 substations)",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable debug logging",
    )
    parser.add_argument(
        "--state", "-s",
        default=None,
        help="Override state filter (e.g. TX, CA, ALL)",
    )
    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    config = load_config(args.config)

    # CLI override for state
    if args.state:
        config.setdefault("grid", {})["state_filter"] = args.state

    run_pipeline(config, test_mode=args.test)


if __name__ == "__main__":
    main()
