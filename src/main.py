"""
BESS Site Scout — Main Orchestrator

Coordinates the full site prospecting pipeline:
1. Pull grid infrastructure data (substations + transmission lines)
2. Fetch generation asset inventory (EIA-860M, interconnection queues, eGRID)
3. For each qualifying substation, search for nearby parcels
4. Screen each candidate against environmental databases
5. Assess grid density and solar resource
6. Score and rank all candidates
7. Generate reports and maps
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
    from .ingestion.eia_860m import EIA860MIngestor
    from .ingestion.interconnection_queues import InterconnectionQueueIngestor
    from .ingestion.egrid import EGRIDIngestor
    from .ingestion.lmp import LMPIngestor
    from .ingestion.congestion import CongestionIngestor
    from .ingestion.capacity_markets import CapacityMarketIngestor
    from .ingestion.ancillary_services import AncillaryServicesIngestor
    from .ingestion.curtailment import CurtailmentIngestor
    from .ingestion.land_use import LandUseIngestor
    from .ingestion.parcels import ParcelIngestor
    from .ingestion.utility_territories import UtilityTerritoryIngestor
    from .ingestion.incentives import IncentivesIngestor
    from .ingestion.soil import SoilIngestor
    from .scoring.environmental import EnvironmentalScorer
    from .scoring.composite import CompositeScorer
    from .utils.export import export_to_excel, export_to_map, export_geojson, export_generation_geojson

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
    # PHASE 2: Generation Asset Inventory
    # =========================================================
    logger.info("\n⚡ PHASE 2: Fetching Generation Asset Inventory...")

    gen_config = config.get("generation_assets", {})
    generation_data = {}

    # 2a. EIA-860M — All operating + planned power plants
    if gen_config.get("eia", {}).get("enabled", True):
        try:
            eia_860m = EIA860MIngestor(config)
            gen_summary = eia_860m.get_generation_summary(state_filter=state_filter)
            generation_data["plants"] = gen_summary
            logger.info(
                f"✅ EIA Plants: {gen_summary['total_plants']} plants, "
                f"{gen_summary['total_capacity_mw']:,.0f} MW total"
            )
        except Exception as e:
            logger.warning(f"⚠️  EIA-860M ingestion failed: {e}")
            generation_data["plants"] = {"total_plants": 0, "total_capacity_mw": 0, "fuel_mix": {}}
    else:
        logger.info("  EIA plant data disabled in config")

    # 2b. ISO/RTO Interconnection Queues
    if gen_config.get("interconnection_queues", {}).get("enabled", True):
        try:
            queue_ingestor = InterconnectionQueueIngestor(config)
            all_queues = queue_ingestor.get_all_queues()
            queue_summary = queue_ingestor.get_queue_summary(all_queues)
            generation_data["queues"] = {
                "data": all_queues,
                "summary": queue_summary,
            }
            logger.info(
                f"✅ Interconnection Queues: {queue_summary['total_projects']} projects, "
                f"{queue_summary['total_capacity_mw']:,.0f} MW proposed"
            )
        except Exception as e:
            logger.warning(f"⚠️  Interconnection queue ingestion failed: {e}")
            generation_data["queues"] = {"data": None, "summary": {}}
    else:
        logger.info("  Interconnection queue data disabled in config")

    # 2c. EPA eGRID — Emissions data for existing plants
    if gen_config.get("egrid", {}).get("enabled", True):
        try:
            egrid = EGRIDIngestor(config)
            egrid_data = egrid.load_egrid_data(state_filter=state_filter)
            clean_dirty = egrid.get_clean_vs_dirty(state_filter=state_filter)
            generation_data["egrid"] = {
                "plant_count": len(egrid_data),
                "clean_vs_dirty": clean_dirty,
            }
            logger.info(
                f"✅ eGRID: {len(egrid_data)} plants with emissions data "
                f"(clean: {clean_dirty['clean']['count']}, "
                f"fossil: {clean_dirty['dirty']['count']})"
            )

            # Enrich EIA plants with emissions if both available
            if "plants" in generation_data and not egrid_data.empty:
                plants_gdf = generation_data["plants"].get("plants_gdf")
                if plants_gdf is not None and not plants_gdf.empty:
                    # Name-based enrichment since ArcGIS plants don't have ORIS codes
                    logger.info("  Emissions enrichment available for cross-reference")

        except Exception as e:
            logger.warning(f"⚠️  eGRID ingestion failed: {e}")
            generation_data["egrid"] = {"plant_count": 0}
    else:
        logger.info("  eGRID emissions data disabled in config")

    # =========================================================
    # PHASE 3: Market & Revenue Data
    # =========================================================
    logger.info("\n💰 PHASE 3: Fetching Market & Revenue Data...")
    market_data = {}
    market_config = config.get("market_data", {})
    lmp_days = market_config.get("lmp", {}).get("days_back", 30)

    # 3a. Locational Marginal Prices (LMP)
    try:
        lmp_ingestor = LMPIngestor(config)
        lmp_df = lmp_ingestor.get_all_lmps(days_back=lmp_days)
        lmp_summary = lmp_ingestor.get_lmp_summary()
        market_data["lmp"] = lmp_summary
        if not lmp_df.empty:
            logger.info(f"✅ LMP: {len(lmp_df)} price records across {len(lmp_summary.get('isos_covered', []))} ISOs")
        else:
            logger.info("  LMP: No live data (reference prices available)")
    except Exception as e:
        logger.warning(f"⚠️  LMP ingestion failed: {e}")

    # 3b. Transmission Congestion
    try:
        congestion_ingestor = CongestionIngestor(config)
        cong_df = congestion_ingestor.get_all_congestion()
        cong_summary = congestion_ingestor.get_congestion_summary(
            lmp_df if "lmp_df" in dir() and not lmp_df.empty else None
        )
        market_data["congestion"] = cong_summary
        logger.info(f"✅ Congestion: {cong_summary.get('total_constraint_records', 0)} constraint records")
    except Exception as e:
        logger.warning(f"⚠️  Congestion data failed: {e}")

    # 3c. Capacity Market Prices
    try:
        capacity_ingestor = CapacityMarketIngestor(config)
        capacity_summary = capacity_ingestor.get_capacity_summary()
        market_data["capacity"] = capacity_summary
        top_iso = capacity_summary.get("revenue_ranking", [{}])[0]
        logger.info(
            f"✅ Capacity Markets: {len(capacity_summary.get('isos_with_capacity_markets', []))} "
            f"ISOs with capacity markets (top: {top_iso.get('iso', 'N/A')} "
            f"${top_iso.get('annual_$/MW', 0):,.0f}/MW/yr)"
        )
    except Exception as e:
        logger.warning(f"⚠️  Capacity market data failed: {e}")

    # 3d. Ancillary Services Pricing
    try:
        as_ingestor = AncillaryServicesIngestor(config)
        as_summary = as_ingestor.get_as_summary()
        market_data["ancillary_services"] = as_summary
        top_as = as_summary.get("best_as_markets", [{}])[0]
        logger.info(
            f"✅ Ancillary Services: Reference prices for {len(as_summary.get('reference_prices', {}))} ISOs "
            f"(best: {top_as.get('iso', 'N/A')} ${top_as.get('annual_$/100MW', 0):,.0f}/100MW/yr)"
        )
    except Exception as e:
        logger.warning(f"⚠️  Ancillary services data failed: {e}")

    # 3e. Renewable Curtailment
    try:
        curtailment_ingestor = CurtailmentIngestor(config)
        curtailment_summary = curtailment_ingestor.get_curtailment_summary()
        market_data["curtailment"] = curtailment_summary
        top_curt = curtailment_summary.get("opportunity_ranking", [{}])[0]
        logger.info(
            f"✅ Curtailment: {len(curtailment_summary.get('iso_scores', {}))} ISOs scored "
            f"(best opportunity: {top_curt.get('iso', 'N/A')} score={top_curt.get('score', 0):.0f})"
        )
    except Exception as e:
        logger.warning(f"⚠️  Curtailment data failed: {e}")

    # =========================================================
    # PHASE 4: Site Feasibility Data
    # =========================================================
    logger.info("\n🏠 PHASE 4: Fetching Site Feasibility Data...")

    land_use_ingestor = LandUseIngestor(config)
    parcel_ingestor = ParcelIngestor(config)
    utility_ingestor = UtilityTerritoryIngestor(config)
    incentives_ingestor = IncentivesIngestor(config)
    soil_ingestor = SoilIngestor(config)

    search_radius = config.get("real_estate", {}).get("search_radius_miles", 3.0)
    logger.info(f"  Land use, parcels, utilities, incentives, soil — per-site in Phase 5")

    # =========================================================
    # PHASE 5: Environmental Screening + Grid Assessment + Site Enrichment
    # =========================================================
    logger.info("\n🔬 PHASE 5: Running Environmental Screening, Grid Assessment & Site Enrichment...")

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

        # --- Land Use (NLCD) ---
        try:
            land_use_result = land_use_ingestor.score_land_suitability(lat, lon)
            logger.info(
                f"    NLCD: {land_use_result.get('nlcd_class', 'Unknown')} "
                f"(score={land_use_result.get('land_use_score', 0)})"
            )
        except Exception as e:
            logger.warning(f"    NLCD query failed: {e}")
            land_use_result = {"land_use_score": 50, "land_use_tier": "Unknown"}

        # --- Utility Territory ---
        try:
            utility_result = utility_ingestor.get_utility_at_point(lat, lon)
            utility_result["interconnection"] = utility_ingestor.classify_interconnection_process(utility_result)
            logger.info(
                f"    Utility: {utility_result.get('utility_name', 'Unknown')} "
                f"({utility_result.get('ownership_type', '')})"
            )
        except Exception as e:
            logger.warning(f"    Utility query failed: {e}")
            utility_result = {"utility_name": "Unknown"}

        # --- Incentives & ITC ---
        site_state = sub.get("STATE", "")
        try:
            incentive_result = incentives_ingestor.get_incentive_score(lat, lon, site_state)
            logger.info(
                f"    Incentives: ITC={incentive_result.get('federal_itc_pct', 30)}% "
                f"(EC={incentive_result.get('is_energy_community', False)}) "
                f"State={incentive_result.get('state_rating', 'Unknown')}"
            )
        except Exception as e:
            logger.warning(f"    Incentive check failed: {e}")
            incentive_result = {"federal_itc_pct": 30, "combined_incentive_score": 50}

        # --- Soil (SSURGO) ---
        try:
            soil_result = soil_ingestor.get_soil_suitability(lat, lon)
            logger.info(
                f"    Soil: {soil_result.get('soil_name', 'Unknown')} "
                f"(score={soil_result.get('bess_score', 50)}, "
                f"drainage={soil_result.get('drainage_class', 'Unknown')})"
            )
        except Exception as e:
            logger.warning(f"    Soil query failed: {e}")
            soil_result = {"bess_score": 50, "score_tier": "Unknown"}

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
            # New enrichment data
            "land_use": land_use_result,
            "land_use_score": land_use_result.get("land_use_score", 50),
            "nlcd_class": land_use_result.get("nlcd_class", "Unknown"),
            "utility": utility_result,
            "utility_name": utility_result.get("utility_name", "Unknown"),
            "ownership_type": utility_result.get("ownership_type", ""),
            "incentives": incentive_result,
            "federal_itc_pct": incentive_result.get("federal_itc_pct", 30),
            "is_energy_community": incentive_result.get("is_energy_community", False),
            "state_incentive_score": incentive_result.get("state_incentive_score", 30),
            "combined_incentive_score": incentive_result.get("combined_incentive_score", 50),
            "soil": soil_result,
            "soil_score": soil_result.get("bess_score", 50),
            "drainage_class": soil_result.get("drainage_class", "Unknown"),
        }
        all_results.append(result)

        status = (
            "❌ ELIMINATED"
            if composite["grade"] == "ELIMINATED"
            else f"✅ {composite['grade']} ({composite['composite_score']})"
        )
        logger.info(f"    Result: {status}")

    # =========================================================
    # PHASE 6: Rank & Report
    # =========================================================
    logger.info("\n📊 PHASE 6: Ranking & Generating Reports...")

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

    # Export generation assets GeoJSON (for dashboard)
    if generation_data:
        try:
            gen_path = export_generation_geojson(generation_data, output_dir)
            logger.info(f"⚡ Generation Assets GeoJSON: {gen_path}")
        except Exception as e:
            logger.warning(f"Generation assets export failed: {e}")

    # Export market data summary
    if market_data:
        try:
            import json as _json
            market_path = Path(output_dir) / "market_data_summary.json"
            with open(market_path, "w") as f:
                _json.dump(market_data, f, indent=2, default=str)
            logger.info(f"💰 Market Data: {market_path}")
        except Exception as e:
            logger.warning(f"Market data export failed: {e}")

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
        "generation_data": generation_data,
        "market_data": market_data,
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
