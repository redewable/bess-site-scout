#!/usr/bin/env python3
"""
BESS Site Scout — Endpoint Verification (v2)
Tests all confirmed endpoints after Feb 2026 migration fixes.

Usage:
    python3 verify_endpoints.py
"""

import requests
import json
import sys
from datetime import datetime

requests.packages.urllib3.disable_warnings()
TIMEOUT = 30


def test_arcgis(name, url, params=None, is_mapserver=False):
    """Test an ArcGIS endpoint with a real query."""
    default_params = {
        "where": "1=1",
        "outFields": "*",
        "f": "geojson",
        "resultRecordCount": 3,
    }
    if params:
        default_params.update(params)

    query_url = f"{url}/query"
    try:
        r = requests.get(query_url, params=default_params, timeout=TIMEOUT, verify=False)
        r.raise_for_status()
        data = r.json()

        if "error" in data:
            # MapServer may not support geojson — try json
            if is_mapserver:
                default_params["f"] = "json"
                r2 = requests.get(query_url, params=default_params, timeout=TIMEOUT, verify=False)
                data2 = r2.json()
                if "error" not in data2:
                    features = data2.get("features", [])
                    return True, f"{len(features)} features (json format)"
            return False, f"Error: {data['error'].get('message', str(data['error']))}"

        features = data.get("features", [])
        return True, f"{len(features)} features returned"
    except Exception as e:
        return False, str(e)[:100]


def test_rest(name, url):
    """Test a generic REST endpoint."""
    try:
        r = requests.get(url, timeout=TIMEOUT, verify=False)
        r.raise_for_status()
        ct = r.headers.get("content-type", "")
        if "json" in ct:
            data = r.json()
            if isinstance(data, list):
                return True, f"{len(data)} records"
            elif isinstance(data, dict):
                return True, f"Got response ({len(data)} keys)"
        return True, f"HTTP {r.status_code}"
    except Exception as e:
        return False, str(e)[:100]


def main():
    print("=" * 65)
    print("BESS Site Scout — Endpoint Verification v2")
    print(f"{datetime.now().isoformat()}")
    print("=" * 65)

    results = []

    # --- 1. HIFLD Substations (Rutgers MapServer) ---
    print("\n[1] HIFLD Substations (Rutgers MARCO mirror)...")
    # MapServer may need json format instead of geojson
    url = "https://oceandata.rad.rutgers.edu/arcgis/rest/services/RenewableEnergy/HIFLD_Electric_SubstationsTransmissionLines/MapServer/0"

    # Try multiple query approaches
    sub_tests = [
        ("geojson + STATE=TX", {"where": "STATE='TX'", "f": "geojson", "outFields": "*", "resultRecordCount": 3}),
        ("json + STATE=TX", {"where": "STATE='TX'", "f": "json", "outFields": "*", "resultRecordCount": 3}),
        ("json + 1=1", {"where": "1=1", "f": "json", "outFields": "*", "resultRecordCount": 3}),
        ("geojson + 1=1", {"where": "1=1", "f": "geojson", "outFields": "*", "resultRecordCount": 3}),
        ("json + TX + 345kv", {"where": "STATE='TX' AND MAX_VOLT>=345", "f": "json", "outFields": "NAME,STATE,MAX_VOLT,LATITUDE,LONGITUDE", "resultRecordCount": 3}),
    ]
    sub_found = False
    for label, params in sub_tests:
        try:
            r = requests.get(f"{url}/query", params=params, timeout=TIMEOUT, verify=False)
            data = r.json()
            if "error" in data:
                print(f"  ❌ {label}: {data['error'].get('message', '')[:60]}")
            else:
                features = data.get("features", [])
                print(f"  {'✅' if features else '⚠️ '} {label}: {len(features)} features")
                if features and not sub_found:
                    # Show sample
                    sample = features[0]
                    attrs = sample.get("attributes", sample.get("properties", {}))
                    print(f"     Sample: {json.dumps({k: attrs.get(k) for k in ['NAME', 'STATE', 'MAX_VOLT', 'VOLT_CLASS'] if k in attrs})}")
                    sub_found = True
                    results.append(("HIFLD Substations", True, f"{len(features)} features via {label}"))
        except Exception as e:
            print(f"  ❌ {label}: {str(e)[:60]}")

    if not sub_found:
        results.append(("HIFLD Substations", False, "No working query found"))

    # --- 2. HIFLD Transmission Lines ---
    print("\n[2] HIFLD Transmission Lines...")
    ok, detail = test_arcgis(
        "Transmission Lines",
        "https://services2.arcgis.com/FiaPA4ga0iQKduv3/arcgis/rest/services/US_Electric_Power_Transmission_Lines/FeatureServer/0",
        {"where": "VOLT_CLASS='345'"},
    )
    print(f"  {'✅' if ok else '❌'} {detail}")
    results.append(("HIFLD Transmission Lines", ok, detail))

    # --- 3. FEMA Flood Zones ---
    print("\n[3] FEMA NFHL Flood Zones...")
    ok, detail = test_arcgis(
        "FEMA",
        "https://hazards.fema.gov/arcgis/rest/services/public/NFHL/MapServer/28",
        is_mapserver=True,
    )
    print(f"  {'✅' if ok else '❌'} {detail}")
    results.append(("FEMA NFHL", ok, detail))

    # --- 4. EPA ECHO ---
    print("\n[4] EPA ECHO...")
    ok, detail = test_rest(
        "ECHO",
        "https://echodata.epa.gov/echo/echo_rest_services.get_facilities?output=JSON&p_st=TX&p_lat=30.628&p_long=-96.334&p_radius=1",
    )
    print(f"  {'✅' if ok else '❌'} {detail}")
    results.append(("EPA ECHO", ok, detail))

    # --- 5. USFWS Critical Habitat ---
    print("\n[5] USFWS Critical Habitat...")
    ok, detail = test_arcgis(
        "Critical Habitat",
        "https://services.arcgis.com/QVENGdaPbd4LUkLV/arcgis/rest/services/USFWS_Critical_Habitat/FeatureServer/0",
    )
    print(f"  {'✅' if ok else '❌'} {detail}")
    results.append(("USFWS Critical Habitat", ok, detail))

    # --- 6. USFWS Wetlands (longer timeout) ---
    print("\n[6] USFWS NWI Wetlands (may be slow)...")
    try:
        r = requests.get(
            "https://fwspublicservices.wim.usgs.gov/wetlandsmapservice/rest/services/Wetlands/MapServer/0/query",
            params={"where": "1=1", "outFields": "*", "f": "geojson", "resultRecordCount": 2,
                    "geometry": "-96.334,30.628", "geometryType": "esriGeometryPoint",
                    "distance": "1609", "units": "esriSRUnit_Meter"},
            timeout=60,  # extra long timeout
            verify=False,
        )
        data = r.json()
        features = data.get("features", [])
        ok = len(features) > 0
        detail = f"{len(features)} features"
        print(f"  {'✅' if ok else '⚠️ '} {detail}")
    except Exception as e:
        ok = False
        detail = str(e)[:80]
        print(f"  ❌ {detail}")
    results.append(("USFWS NWI Wetlands", ok, detail))

    # --- 7. TCEQ (known broken, just confirm) ---
    print("\n[7] TCEQ LPST (known broken — confirming)...")
    ok, detail = test_arcgis(
        "TCEQ LPST",
        "https://services.arcgis.com/KTcxiTD9dsQw4r7Z/arcgis/rest/services/LPST_Points/FeatureServer/0",
    )
    print(f"  {'❌' if not ok else '✅'} {detail} (expected: broken)")
    results.append(("TCEQ LPST", ok, f"{detail} — covered by EPA ECHO fallback"))

    # --- Summary ---
    print("\n" + "=" * 65)
    print("SUMMARY")
    print("=" * 65)
    working = 0
    for name, ok, detail in results:
        icon = "✅" if ok else "❌"
        print(f"  {icon} {name:30s} → {detail}")
        if ok:
            working += 1

    print(f"\n  {working}/{len(results)} endpoints working")
    if working >= 5:
        print("  ✅ Pipeline is viable — enough endpoints to run full screening")
    print("=" * 65)

    with open("endpoint_verification_v2.json", "w") as f:
        json.dump({"timestamp": datetime.now().isoformat(), "results": [{"name": n, "ok": o, "detail": d} for n, o, d in results]}, f, indent=2)

    return 0 if working >= 5 else 1


if __name__ == "__main__":
    sys.exit(main())
