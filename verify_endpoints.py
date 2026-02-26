#!/usr/bin/env python3
"""
BESS Site Scout — Endpoint Verification Script
Run this locally to confirm all API endpoints are alive and returning data.

Usage:
    python verify_endpoints.py
"""

import requests
import json
import sys
from datetime import datetime


def check_arcgis(name, url, test_query=None):
    """Check an ArcGIS REST service endpoint."""
    try:
        # First check service metadata
        r = requests.get(f"{url}?f=json", timeout=15)
        r.raise_for_status()
        meta = r.json()

        if "error" in meta:
            return {"name": name, "status": "ERROR", "detail": meta["error"].get("message", str(meta["error"]))}

        field_names = [f["name"] for f in meta.get("fields", [])]

        # Now try a small query to confirm data flows
        query_url = f"{url}/query"
        params = {"where": "1=1", "outFields": "*", "f": "geojson", "resultRecordCount": 2}
        if test_query:
            params.update(test_query)

        qr = requests.get(query_url, params=params, timeout=30)
        qr.raise_for_status()
        qdata = qr.json()
        feature_count = len(qdata.get("features", []))

        return {
            "name": name,
            "status": "OK" if feature_count > 0 else "EMPTY",
            "fields": field_names[:8],
            "sample_count": feature_count,
            "detail": f"{len(field_names)} fields, got {feature_count} sample features",
        }

    except Exception as e:
        return {"name": name, "status": "FAILED", "detail": str(e)}


def check_epa_envirofacts(name, url):
    """Check an EPA Envirofacts endpoint."""
    try:
        r = requests.get(url, timeout=15)
        r.raise_for_status()
        data = r.json()
        count = len(data) if isinstance(data, list) else 1
        return {"name": name, "status": "OK", "detail": f"Got {count} records", "sample_count": count}
    except Exception as e:
        return {"name": name, "status": "FAILED", "detail": str(e)}


def check_url(name, url):
    """Simple HTTP check."""
    try:
        r = requests.get(url, timeout=15)
        r.raise_for_status()
        return {"name": name, "status": "OK", "detail": f"HTTP {r.status_code}"}
    except Exception as e:
        return {"name": name, "status": "FAILED", "detail": str(e)}


def main():
    print("=" * 70)
    print("BESS Site Scout — Endpoint Verification")
    print(f"Timestamp: {datetime.now().isoformat()}")
    print("=" * 70)

    results = []

    # --- HIFLD (new server) ---
    print("\n[HIFLD] Checking substations & transmission lines...")

    results.append(check_arcgis(
        "HIFLD Substations (new)",
        "https://services2.arcgis.com/FiaPA4ga0iQKduv3/arcgis/rest/services/US_Electric_Substations/FeatureServer/0",
        test_query={"where": "STATE='TX' AND VOLT_CLASS='345'"},
    ))

    results.append(check_arcgis(
        "HIFLD Transmission Lines (new)",
        "https://services2.arcgis.com/FiaPA4ga0iQKduv3/arcgis/rest/services/US_Electric_Power_Transmission_Lines/FeatureServer/0",
        test_query={"where": "VOLT_CLASS='345'"},
    ))

    # --- Fallback: try to discover substations if the guessed name fails ---
    if results[0]["status"] == "FAILED":
        print("  → Primary substations URL failed. Browsing services directory...")
        try:
            r = requests.get(
                "https://services2.arcgis.com/FiaPA4ga0iQKduv3/arcgis/rest/services/?f=json",
                timeout=15,
            )
            services = r.json().get("services", [])
            sub_matches = [s for s in services if "substation" in s.get("name", "").lower()]
            if sub_matches:
                print(f"  → Found matching services: {[s['name'] for s in sub_matches]}")
                for svc in sub_matches:
                    svc_type = svc.get("type", "FeatureServer")
                    svc_url = f"https://services2.arcgis.com/FiaPA4ga0iQKduv3/arcgis/rest/services/{svc['name']}/{svc_type}/0"
                    result = check_arcgis(f"HIFLD Substations (discovered: {svc['name']})", svc_url)
                    results.append(result)
                    if result["status"] == "OK":
                        print(f"  ✅ FOUND WORKING URL: {svc_url}")
                        break
            else:
                # List ALL services so the user can find it manually
                print(f"  → No 'substation' match. All services on this server:")
                for s in services:
                    print(f"     - {s.get('name')} ({s.get('type')})")
        except Exception as e:
            print(f"  → Could not browse services directory: {e}")

    # --- FEMA ---
    print("\n[FEMA] Checking National Flood Hazard Layer...")
    results.append(check_arcgis(
        "FEMA NFHL Flood Zones",
        "https://hazards.fema.gov/gis/nfhl/rest/services/public/NFHL/MapServer/28",
    ))

    # --- EPA ---
    print("\n[EPA] Checking Envirofacts & ECHO...")
    results.append(check_epa_envirofacts(
        "EPA Envirofacts (SEMS)",
        "https://enviro.epa.gov/enviro/efservice/sems.sems_active_sites/state_code/TX/rows/0:2/json",
    ))
    results.append(check_url(
        "EPA ECHO",
        "https://echo.epa.gov/api/echo_rest_services.get_facilities?output=JSON&p_st=TX&p_lat=30.628&p_long=-96.334&p_radius=1",
    ))

    # --- TCEQ ---
    print("\n[TCEQ] Checking Texas environmental databases...")
    results.append(check_arcgis(
        "TCEQ LPST (Leaking Petroleum Storage)",
        "https://services.arcgis.com/KTcxiTD9dsQw4r7Z/arcgis/rest/services/LPST_Points/FeatureServer/0",
    ))
    results.append(check_arcgis(
        "TCEQ PST (Petroleum Storage Tanks)",
        "https://services.arcgis.com/KTcxiTD9dsQw4r7Z/arcgis/rest/services/PetroleumStorageTanks/FeatureServer/0",
    ))

    # --- USFWS ---
    print("\n[USFWS] Checking wetlands & critical habitat...")
    results.append(check_arcgis(
        "USFWS NWI Wetlands",
        "https://fwspublicservices.wim.usgs.gov/wetlandsmapservice/rest/services/Wetlands/MapServer/0",
    ))
    results.append(check_arcgis(
        "USFWS Critical Habitat",
        "https://services.arcgis.com/QVENGdaPbd4LUkLV/arcgis/rest/services/FWS_HQ_ES_Critical_Habitat/FeatureServer/0",
    ))

    # --- Summary ---
    print("\n" + "=" * 70)
    print("RESULTS SUMMARY")
    print("=" * 70)

    ok = 0
    failed = 0
    for r in results:
        icon = "✅" if r["status"] == "OK" else ("⚠️ " if r["status"] == "EMPTY" else "❌")
        print(f"  {icon} {r['name']:45s} → {r['status']:8s} | {r['detail']}")
        if r["status"] == "OK":
            ok += 1
        else:
            failed += 1

    print(f"\n  {ok}/{ok+failed} endpoints working")

    if failed > 0:
        print("\n  ⚠️  Some endpoints need attention. Check the details above.")
        print("  If HIFLD Substations failed, look at the services directory output")
        print("  to find the correct service name and update src/ingestion/hifld.py")

    print("=" * 70)

    # Save results to JSON
    with open("endpoint_verification.json", "w") as f:
        json.dump({"timestamp": datetime.now().isoformat(), "results": results}, f, indent=2)
    print(f"\nDetailed results saved to endpoint_verification.json")

    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
