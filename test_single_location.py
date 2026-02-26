#!/usr/bin/env python3
"""
Quick Test — Screen a single location against all environmental databases.

Usage:
    python test_single_location.py --lat 30.6 --lon -96.3
    python test_single_location.py  # defaults to Bryan/College Station area
"""

import argparse
import json
import logging
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("test")


def test_single_location(lat: float, lon: float, config_path: str = "config/config.yaml"):
    """Run all environmental screens on a single lat/lon."""
    import yaml

    with open(config_path) as f:
        config = yaml.safe_load(f)

    print(f"\n{'='*60}")
    print(f"BESS Site Scout — Single Location Test")
    print(f"Location: ({lat}, {lon})")
    print(f"{'='*60}\n")

    # --- FEMA ---
    print("🌊 FEMA Flood Zones...")
    try:
        from src.ingestion.fema import FEMAIngestor
        fema = FEMAIngestor(config)
        fema_result = fema.assess_flood_risk(lat, lon)
        print(f"   Zone: {fema_result['flood_zone']}")
        print(f"   Risk: {fema_result['risk_level']}")
        print(f"   SFHA: {fema_result['in_sfha']}")
        print(f"   Detail: {fema_result['details']}")
    except Exception as e:
        print(f"   ❌ Error: {e}")
        fema_result = {"risk_level": "unknown", "eliminate": False, "risk_flags": []}

    # --- EPA ---
    print("\n🏭 EPA Environmental Databases...")
    try:
        from src.ingestion.epa import EPAIngestor
        epa = EPAIngestor(config)
        epa_result = epa.run_full_screening(lat, lon)
        print(f"   Superfund NPL: {epa_result['superfund']['count']} sites")
        print(f"   Brownfields: {epa_result['brownfields']['count']} sites")
        print(f"   TRI: {epa_result['tri']['count']} facilities")
        print(f"   ECHO facilities: {epa_result['echo_summary']['total_facilities']}")
        for flag in epa_result.get("risk_flags", []):
            print(f"   ⚠️  {flag}")
    except Exception as e:
        print(f"   ❌ Error: {e}")
        epa_result = {"eliminate": False, "risk_flags": []}

    # --- TCEQ ---
    print("\n🛢️ TCEQ State Databases...")
    try:
        from src.ingestion.tceq import TCEQIngestor
        tceq = TCEQIngestor(config)
        tceq_result = tceq.run_full_screening(lat, lon)
        print(f"   LPST: {tceq_result['lpst']['count']} sites")
        print(f"   UST: {tceq_result['ust']['count']} tanks")
        print(f"   IHW: {tceq_result['ihw']['count']} facilities")
        print(f"   MSW: {tceq_result['msw']['count']} sites")
        print(f"   Drycleaners: {tceq_result['drycleaners']['count']} sites")
        for flag in tceq_result.get("risk_flags", []):
            print(f"   ⚠️  {flag}")
    except Exception as e:
        print(f"   ❌ Error: {e}")
        tceq_result = {"eliminate": False, "risk_flags": []}

    # --- USFWS ---
    print("\n🦅 USFWS Wetlands & Endangered Species...")
    try:
        from src.ingestion.usfws import USFWSIngestor
        usfws = USFWSIngestor(config)
        usfws_result = usfws.run_full_screening(lat, lon)
        print(f"   Wetlands: {usfws_result['wetlands']['count']} features")
        print(f"   Critical Habitat: {usfws_result['critical_habitat']['present']}")
        if usfws_result['critical_habitat']['species']:
            print(f"   Species: {', '.join(usfws_result['critical_habitat']['species'][:3])}")
        for flag in usfws_result.get("risk_flags", []):
            print(f"   ⚠️  {flag}")
    except Exception as e:
        print(f"   ❌ Error: {e}")
        usfws_result = {"eliminate": False, "risk_flags": []}

    # --- Scoring ---
    print(f"\n{'='*60}")
    print("📊 COMPOSITE SCORE")
    print(f"{'='*60}")
    try:
        from src.scoring.environmental import EnvironmentalScorer
        scorer = EnvironmentalScorer(config)
        env_score = scorer.score_parcel(fema_result, epa_result, tceq_result, usfws_result)
        print(f"   Environmental Score: {env_score['score']}/100")
        print(f"   Environmental Grade: {env_score['grade']}")
        print(f"   Eliminate: {env_score['eliminate']}")
        print(f"\n   Score Breakdown:")
        for source, detail in env_score.get("details", {}).items():
            print(f"     {source}: penalty={detail.get('penalty', 0)}")
        print(f"\n   All Risk Flags:")
        for flag in env_score.get("risk_flags", []):
            print(f"     • {flag}")
    except Exception as e:
        print(f"   ❌ Scoring error: {e}")

    print(f"\n{'='*60}")
    print("Done!")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--lat", type=float, default=30.6280, help="Latitude")
    parser.add_argument("--lon", type=float, default=-96.3344, help="Longitude")
    parser.add_argument("--config", default="config/config.yaml", help="Config path")
    args = parser.parse_args()

    test_single_location(args.lat, args.lon, args.config)
