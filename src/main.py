"""
BESS Site Scout — Main Orchestrator

Coordinates the full site prospecting pipeline:
1. Pull grid infrastructure data (substations + transmission lines)
2. For each qualifying substation, search for nearby parcels
3. Screen each candidate against environmental databases
4. Score and rank all candidates
5. Generate reports and maps
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
    from .scoring.environmental import EnvironmentalScorer
    from .scoring.composite import CompositeScorer
    from .utils.export import export_to_excel, export_to_map, export_geojson

    logger.info("=" * 60)
    logger.info("BESS Site Scout — Starting Pipeline")
    logger.info(f"Timestamp: {datetime.now().isoformat()}")
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

    # For now, we use the substation locations themselves as candidate sites.
    # In production, this is where you'd integrate:
    #   - County CAD parcel data
    #   - Land listing APIs (RentCast, LandWatch, etc.)
    #   - Custom parcel boundaries
    #
    # The environmental screening below runs against each substation point.
    # When real estate data is integrated, it will run against each
    # candidate parcel near each substation.

    search_radius = config.get("real_estate", {}).get("search_radius_miles", 3.0)
    logger.info(f"  Search radius: {search_radius} miles per substation")
    logger.info(f"  ⚠️  Real estate data integration pending — using substation locations as proxies")

    # =========================================================
    # PHASE 3: Environmental Screening
    # =========================================================
    logger.info("\n🔬 PHASE 3: Running Environmental Screening...")

    fema_ingestor = FEMAIngestor(config)
    epa_ingestor = EPAIngestor(config)
    tceq_ingestor = TCEQIngestor(config)
    usfws_ingestor = USFWSIngestor(config)
    env_scorer = EnvironmentalScorer(config)
    composite_scorer = CompositeScorer(config)

    all_results = []

    for idx, sub in substations.iterrows():
        sub_name = sub.get("NAME", f"Substation_{idx}")
        lat = sub.get("lat", sub.geometry.y if sub.geometry else None)
        lon = sub.get("lon", sub.geometry.x if sub.geometry else None)

        if lat is None or lon is None:
            logger.warning(f"  Skipping {sub_name} — no coordinates")
            continue

        logger.info(f"\n  🔍 Screening: {sub_name} ({lat:.4f}, {lon:.4f})")

        # Determine voltage for scoring
        volt_class = sub.get("VOLT_CLASS", "")
        if "345" in str(volt_class):
            voltage_kv = 345
        elif "220" in str(volt_class) or "287" in str(volt_class):
            voltage_kv = 230
        elif "161" in str(volt_class):
            voltage_kv = 161
        else:
            voltage_kv = 138

        # --- Run environmental screens ---
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

        # --- Score ---
        env_score = env_scorer.score_parcel(fema_result, epa_result, tceq_result, usfws_result)

        # For now, use placeholder values for real estate fields
        # These get replaced when parcel data is integrated
        composite = composite_scorer.score_site(
            distance_to_substation_mi=0.0,  # AT the substation for now
            substation_voltage_kv=voltage_kv,
            environmental_score=env_score["score"],
            environmental_eliminate=env_score["eliminate"],
            price_per_acre=5000,  # Placeholder
            parcel_acres=40,  # Placeholder
            flood_risk_level=fema_result.get("risk_level", "unknown"),
        )

        # Compile result
        result = {
            "substation_name": sub_name,
            "substation_voltage_kv": voltage_kv,
            "volt_class": volt_class,
            "lat": lat,
            "lon": lon,
            "distance_to_substation_mi": 0.0,
            "composite_score": composite["composite_score"],
            "grade": composite["grade"],
            "sub_scores": composite.get("sub_scores", {}),
            "environmental": env_score,
            "flood": fema_result,
            "epa": epa_result,
            "tceq": tceq_result,
            "usfws": usfws_result,
            "risk_flags": env_score.get("risk_flags", []),
        }
        all_results.append(result)

        status = "❌ ELIMINATED" if composite["grade"] == "ELIMINATED" else f"✅ {composite['grade']} ({composite['composite_score']})"
        logger.info(f"    Result: {status}")

    # =========================================================
    # PHASE 4: Rank & Report
    # =========================================================
    logger.info("\n📊 PHASE 4: Ranking & Generating Reports...")

    ranked = composite_scorer.rank_sites(all_results)
    top_n = config.get("output", {}).get("top_n_results", 50)

    logger.info(f"\n{'='*60}")
    logger.info(f"TOP {min(top_n, len(ranked))} SITES:")
    logger.info(f"{'='*60}")

    for site in ranked[:top_n]:
        logger.info(
            f"  #{site['rank']:3d} | {site['grade']} | "
            f"Score: {site['composite_score']:5.1f} | "
            f"{site['substation_name'][:30]:30s} | "
            f"{site['volt_class']} | "
            f"Env: {site['environmental']['grade']}"
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

    parser = argparse.ArgumentParser(description="BESS Site Scout — Automated Site Prospecting")
    parser.add_argument(
        "--config", "-c",
        default="config/config.yaml",
        help="Path to config file (default: config/config.yaml)"
    )
    parser.add_argument(
        "--test", "-t",
        action="store_true",
        help="Run in test mode (limit to 5 substations)"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable debug logging"
    )
    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    config = load_config(args.config)
    run_pipeline(config, test_mode=args.test)


if __name__ == "__main__":
    main()
