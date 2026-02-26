#!/usr/bin/env python3
"""
BESS Site Scout — Endpoint Discovery Script
Tries multiple candidate URLs for each service and reports what works.
Also browses ArcGIS service directories to find renamed services.

Usage:
    python3 discover_endpoints.py
"""

import requests
import json
import sys
from datetime import datetime

requests.packages.urllib3.disable_warnings()
TIMEOUT = 20


def try_arcgis_service(url, test_where="1=1", label=""):
    """Test an ArcGIS service URL. Returns (works, detail)."""
    try:
        # Check metadata
        r = requests.get(f"{url}?f=json", timeout=TIMEOUT, verify=False)
        r.raise_for_status()
        meta = r.json()
        if "error" in meta:
            return False, f"Error: {meta['error'].get('message', '')}"

        fields = [f["name"] for f in meta.get("fields", [])]

        # Try a query
        qr = requests.get(f"{url}/query", params={
            "where": test_where, "outFields": "*", "f": "geojson", "resultRecordCount": 2
        }, timeout=TIMEOUT, verify=False)
        qdata = qr.json()
        fc = len(qdata.get("features", []))

        return True, f"{len(fields)} fields, {fc} features | Fields: {', '.join(fields[:10])}"
    except Exception as e:
        return False, str(e)[:100]


def try_rest_api(url, label=""):
    """Test a generic REST endpoint."""
    try:
        r = requests.get(url, timeout=TIMEOUT, verify=False)
        r.raise_for_status()
        data = r.json() if 'json' in r.headers.get('content-type', '') else r.text[:200]
        count = len(data) if isinstance(data, list) else 1
        return True, f"HTTP {r.status_code}, got {count} records"
    except Exception as e:
        return False, str(e)[:100]


def browse_arcgis_directory(server_url, search_terms):
    """Browse an ArcGIS services directory and find matching services."""
    try:
        r = requests.get(f"{server_url}?f=json", timeout=TIMEOUT, verify=False)
        r.raise_for_status()
        data = r.json()
        services = data.get("services", [])
        folders = data.get("folders", [])

        matches = []
        all_names = []
        for svc in services:
            name = svc.get("name", "")
            stype = svc.get("type", "FeatureServer")
            all_names.append(f"{name} ({stype})")
            for term in search_terms:
                if term.lower() in name.lower():
                    matches.append((name, stype))
                    break

        return True, matches, all_names, folders
    except Exception as e:
        return False, [], [], []


def main():
    print("=" * 70)
    print("BESS Site Scout — Endpoint Discovery")
    print(f"Timestamp: {datetime.now().isoformat()}")
    print("=" * 70)

    results = {}

    # ===================================================================
    # 1. HIFLD SUBSTATIONS — try multiple candidates
    # ===================================================================
    print("\n" + "=" * 50)
    print("[1] HIFLD SUBSTATIONS")
    print("=" * 50)

    sub_candidates = [
        ("services2 — US_Electric_Substations",
         "https://services2.arcgis.com/FiaPA4ga0iQKduv3/arcgis/rest/services/US_Electric_Substations/FeatureServer/0"),
        ("services2 — Electric_Substations",
         "https://services2.arcgis.com/FiaPA4ga0iQKduv3/arcgis/rest/services/Electric_Substations/FeatureServer/0"),
        ("services2 — US_Electric_Substations_1",
         "https://services2.arcgis.com/FiaPA4ga0iQKduv3/arcgis/rest/services/US_Electric_Substations_1/FeatureServer/0"),
        ("NASA NCCS — hifld_open energy (layer 2)",
         "https://maps.nccs.nasa.gov/mapping/rest/services/hifld_open/energy/FeatureServer/2"),
        ("NASA NCCS — hifld_open energy (layer 0)",
         "https://maps.nccs.nasa.gov/mapping/rest/services/hifld_open/energy/FeatureServer/0"),
        ("NASA NCCS — hifld_open energy (layer 1)",
         "https://maps.nccs.nasa.gov/mapping/rest/services/hifld_open/energy/FeatureServer/1"),
        ("Rutgers MARCO — MapServer layer 0",
         "https://oceandata.rad.rutgers.edu/arcgis/rest/services/RenewableEnergy/HIFLD_Electric_SubstationsTransmissionLines/MapServer/0"),
    ]

    found_subs = False
    for label, url in sub_candidates:
        ok, detail = try_arcgis_service(url, "STATE='TX'")
        icon = "✅" if ok else "❌"
        print(f"  {icon} {label}")
        print(f"     URL: {url}")
        print(f"     {detail}")
        if ok and not found_subs:
            results["substations"] = {"url": url, "label": label}
            found_subs = True

    # Browse services2 directory
    print("\n  📂 Browsing services2.arcgis.com directory...")
    ok, matches, all_names, folders = browse_arcgis_directory(
        "https://services2.arcgis.com/FiaPA4ga0iQKduv3/arcgis/rest/services/",
        ["substation", "electric_sub", "US_Electric_Sub"]
    )
    if ok:
        if matches:
            print(f"  🔍 Substation matches: {matches}")
            for name, stype in matches:
                url = f"https://services2.arcgis.com/FiaPA4ga0iQKduv3/arcgis/rest/services/{name}/{stype}/0"
                ok2, detail2 = try_arcgis_service(url, "STATE='TX'")
                if ok2 and not found_subs:
                    results["substations"] = {"url": url, "label": f"Discovered: {name}"}
                    found_subs = True
                    print(f"  ✅ DISCOVERED: {url}")
        else:
            print(f"  ⚠️  No 'substation' match. All {len(all_names)} services:")
            for n in sorted(all_names):
                print(f"     - {n}")
        if folders:
            print(f"  📁 Folders: {folders}")

    # Also browse NASA NCCS
    print("\n  📂 Browsing NASA NCCS hifld_open energy layers...")
    try:
        r = requests.get("https://maps.nccs.nasa.gov/mapping/rest/services/hifld_open/energy/FeatureServer?f=json", timeout=TIMEOUT, verify=False)
        meta = r.json()
        layers = meta.get("layers", [])
        for layer in layers:
            print(f"     Layer {layer['id']}: {layer['name']}")
    except Exception as e:
        print(f"     Could not browse: {e}")

    if not found_subs:
        print("\n  ❌ NO WORKING SUBSTATIONS ENDPOINT FOUND")

    # ===================================================================
    # 2. HIFLD TRANSMISSION LINES (already working, just confirm)
    # ===================================================================
    print("\n" + "=" * 50)
    print("[2] HIFLD TRANSMISSION LINES")
    print("=" * 50)
    url = "https://services2.arcgis.com/FiaPA4ga0iQKduv3/arcgis/rest/services/US_Electric_Power_Transmission_Lines/FeatureServer/0"
    ok, detail = try_arcgis_service(url, "VOLT_CLASS='345'")
    print(f"  {'✅' if ok else '❌'} {url}")
    print(f"     {detail}")
    if ok:
        results["transmission_lines"] = {"url": url}

    # ===================================================================
    # 3. FEMA NFHL — try new URL structure
    # ===================================================================
    print("\n" + "=" * 50)
    print("[3] FEMA NFHL FLOOD ZONES")
    print("=" * 50)

    fema_candidates = [
        ("New path — /arcgis/rest/ layer 28",
         "https://hazards.fema.gov/arcgis/rest/services/public/NFHL/MapServer/28"),
        ("New path — /arcgis/rest/ layer 20",
         "https://hazards.fema.gov/arcgis/rest/services/public/NFHL/MapServer/20"),
        ("Old path — /gis/nfhl/rest/ layer 28",
         "https://hazards.fema.gov/gis/nfhl/rest/services/public/NFHL/MapServer/28"),
        ("Old path — /gis/nfhl/rest/ layer 20",
         "https://hazards.fema.gov/gis/nfhl/rest/services/public/NFHL/MapServer/20"),
    ]

    found_fema = False
    for label, url in fema_candidates:
        ok, detail = try_arcgis_service(url)
        icon = "✅" if ok else "❌"
        print(f"  {icon} {label}")
        print(f"     {detail}")
        if ok and not found_fema:
            results["fema_nfhl"] = {"url": url, "label": label}
            found_fema = True

    # Browse FEMA layers
    for base in [
        "https://hazards.fema.gov/arcgis/rest/services/public/NFHL/MapServer",
        "https://hazards.fema.gov/gis/nfhl/rest/services/public/NFHL/MapServer",
    ]:
        print(f"\n  📂 Browsing {base}...")
        try:
            r = requests.get(f"{base}?f=json", timeout=TIMEOUT, verify=False)
            meta = r.json()
            layers = meta.get("layers", [])
            for layer in layers:
                name = layer.get("name", "")
                if "fld_haz" in name.lower() or "flood" in name.lower():
                    print(f"     🎯 Layer {layer['id']}: {name}")
                    if not found_fema:
                        test_url = f"{base}/{layer['id']}"
                        ok2, detail2 = try_arcgis_service(test_url)
                        if ok2:
                            results["fema_nfhl"] = {"url": test_url}
                            found_fema = True
                            print(f"        ✅ WORKS: {test_url}")
            if not layers:
                print(f"     No layers found (got {list(meta.keys())})")
        except Exception as e:
            print(f"     Could not browse: {str(e)[:80]}")

    # ===================================================================
    # 4. EPA ENVIROFACTS
    # ===================================================================
    print("\n" + "=" * 50)
    print("[4] EPA ENVIROFACTS (SEMS)")
    print("=" * 50)

    epa_candidates = [
        ("New domain — data.epa.gov (lowercase)",
         "https://data.epa.gov/efservice/sems.sems_active_sites/state_code/TX/rows/0:2/json"),
        ("New domain — data.epa.gov (uppercase)",
         "https://data.epa.gov/efservice/SEMS.SEMS_ACTIVE_SITES/STATE_CODE/TX/rows/0:2/JSON"),
        ("New domain — data.epa.gov (no prefix)",
         "https://data.epa.gov/efservice/SEMS_ACTIVE_SITES/STATE_CODE/TX/rows/0:2/JSON"),
        ("Old domain — enviro.epa.gov",
         "https://enviro.epa.gov/enviro/efservice/sems.sems_active_sites/state_code/TX/rows/0:2/json"),
    ]

    found_epa = False
    for label, url in epa_candidates:
        ok, detail = try_rest_api(url)
        icon = "✅" if ok else "❌"
        print(f"  {icon} {label}")
        print(f"     {detail}")
        if ok and not found_epa:
            # Extract base URL
            base = url.split("/sems")[0].split("/SEMS")[0]
            results["epa_envirofacts"] = {"url": url, "base": base, "label": label}
            found_epa = True

    # ===================================================================
    # 5. EPA ECHO
    # ===================================================================
    print("\n" + "=" * 50)
    print("[5] EPA ECHO")
    print("=" * 50)

    echo_candidates = [
        ("ofmpub — echo13_rest_services",
         "https://ofmpub.epa.gov/echo/echo13_rest_services.get_facilities?output=JSON&p_st=TX&p_lat=30.628&p_long=-96.334&p_radius=1"),
        ("ofmpub — echo_rest_services",
         "https://ofmpub.epa.gov/echo/echo_rest_services.get_facilities?output=JSON&p_st=TX&p_lat=30.628&p_long=-96.334&p_radius=1"),
        ("echodata — echo_rest_services",
         "https://echodata.epa.gov/echo/echo_rest_services.get_facilities?output=JSON&p_st=TX&p_lat=30.628&p_long=-96.334&p_radius=1"),
        ("Old — echo.epa.gov",
         "https://echo.epa.gov/api/echo_rest_services.get_facilities?output=JSON&p_st=TX&p_lat=30.628&p_long=-96.334&p_radius=1"),
    ]

    found_echo = False
    for label, url in echo_candidates:
        ok, detail = try_rest_api(url)
        icon = "✅" if ok else "❌"
        print(f"  {icon} {label}")
        print(f"     {detail}")
        if ok and not found_echo:
            results["epa_echo"] = {"url": url, "label": label}
            found_echo = True

    # ===================================================================
    # 6. TCEQ — browse directory and try candidates
    # ===================================================================
    print("\n" + "=" * 50)
    print("[6] TCEQ SERVICES")
    print("=" * 50)

    # Browse TCEQ directory
    print("  📂 Browsing TCEQ ArcGIS directory...")
    ok, matches, all_names, folders = browse_arcgis_directory(
        "https://services.arcgis.com/KTcxiTD9dsQw4r7Z/arcgis/rest/services/",
        ["lpst", "petroleum", "storage", "leaking", "hazardous", "waste", "municipal", "spill", "dryclean"]
    )
    if ok:
        print(f"  Found {len(all_names)} total services")
        if matches:
            print(f"  🔍 Matches: {[(n, t) for n, t in matches]}")
            for name, stype in matches:
                url = f"https://services.arcgis.com/KTcxiTD9dsQw4r7Z/arcgis/rest/services/{name}/{stype}/0"
                ok2, detail2 = try_arcgis_service(url)
                icon = "✅" if ok2 else "❌"
                print(f"  {icon} {name}: {detail2[:80]}")
                if ok2:
                    results[f"tceq_{name}"] = {"url": url}
        else:
            print(f"  ⚠️  No keyword matches. Full service list:")
            for n in sorted(all_names):
                print(f"     - {n}")
        if folders:
            print(f"  📁 Folders to check: {folders}")
            for folder in folders:
                print(f"\n  📂 Browsing folder: {folder}")
                ok3, matches3, names3, _ = browse_arcgis_directory(
                    f"https://services.arcgis.com/KTcxiTD9dsQw4r7Z/arcgis/rest/services/{folder}/",
                    ["lpst", "petroleum", "storage", "leaking", "hazardous", "waste"]
                )
                if ok3:
                    for n in sorted(names3):
                        print(f"     - {n}")
    else:
        print("  ❌ Could not browse TCEQ directory")
        # Try direct candidates
        tceq_candidates = [
            ("LPST_Points", "https://services.arcgis.com/KTcxiTD9dsQw4r7Z/arcgis/rest/services/LPST_Points/FeatureServer/0"),
            ("LPST", "https://services.arcgis.com/KTcxiTD9dsQw4r7Z/arcgis/rest/services/LPST/FeatureServer/0"),
            ("PetroleumStorageTanks", "https://services.arcgis.com/KTcxiTD9dsQw4r7Z/arcgis/rest/services/PetroleumStorageTanks/FeatureServer/0"),
            ("PST", "https://services.arcgis.com/KTcxiTD9dsQw4r7Z/arcgis/rest/services/PST/FeatureServer/0"),
        ]
        for label, url in tceq_candidates:
            ok, detail = try_arcgis_service(url)
            icon = "✅" if ok else "❌"
            print(f"  {icon} {label}: {detail[:80]}")
            if ok:
                results[f"tceq_{label}"] = {"url": url}

    # ===================================================================
    # 7. USFWS — wetlands and critical habitat
    # ===================================================================
    print("\n" + "=" * 50)
    print("[7] USFWS SERVICES")
    print("=" * 50)

    # Wetlands
    print("\n  --- NWI Wetlands ---")
    wetland_candidates = [
        ("USGS/FWS MapServer layer 0",
         "https://fwspublicservices.wim.usgs.gov/wetlandsmapservice/rest/services/Wetlands/MapServer/0"),
        ("USGS/FWS WMS (backup)",
         "https://www.fws.gov/wetlands/data/mapper.html"),
    ]
    found_wetlands = False
    for label, url in wetland_candidates:
        if "mapper.html" in url:
            continue  # skip non-API URLs
        ok, detail = try_arcgis_service(url)
        icon = "✅" if ok else "❌"
        print(f"  {icon} {label}")
        print(f"     {detail}")
        if ok:
            results["usfws_nwi"] = {"url": url}
            found_wetlands = True

    if not found_wetlands:
        print("  ℹ️  Wetlands endpoint may just be slow. Try increasing timeout.")

    # Critical Habitat
    print("\n  --- Critical Habitat ---")

    # Browse USFWS directory
    print("  📂 Browsing USFWS ArcGIS directory...")
    ok, matches, all_names, folders = browse_arcgis_directory(
        "https://services.arcgis.com/QVENGdaPbd4LUkLV/arcgis/rest/services/",
        ["critical_habitat", "habitat", "endangered", "threatened", "species"]
    )
    if ok:
        print(f"  Found {len(all_names)} total services")
        if matches:
            print(f"  🔍 Matches:")
            for name, stype in matches:
                url = f"https://services.arcgis.com/QVENGdaPbd4LUkLV/arcgis/rest/services/{name}/{stype}/0"
                ok2, detail2 = try_arcgis_service(url)
                icon = "✅" if ok2 else "❌"
                print(f"  {icon} {name}: {detail2[:80]}")
                if ok2:
                    results["usfws_critical_habitat"] = {"url": url}
        else:
            print(f"  ⚠️  No matches. Services with 'habitat' or 'species':")
            for n in sorted(all_names):
                if any(t in n.lower() for t in ["habitat", "species", "critical", "fws", "endangered"]):
                    print(f"     🎯 {n}")
            print(f"\n  Full list ({len(all_names)} services):")
            for n in sorted(all_names)[:50]:
                print(f"     - {n}")
            if len(all_names) > 50:
                print(f"     ... and {len(all_names) - 50} more")
    else:
        print("  ❌ Could not browse USFWS directory")

    # Try alternative USFWS critical habitat URLs
    ch_candidates = [
        ("ECOS Critical Habitat",
         "https://services.arcgis.com/QVENGdaPbd4LUkLV/arcgis/rest/services/FWS_HQ_ES_Critical_Habitat/FeatureServer/0"),
        ("ECOS CH (v2)",
         "https://services.arcgis.com/QVENGdaPbd4LUkLV/arcgis/rest/services/Critical_Habitat/FeatureServer/0"),
        ("FWS ECOS GIS",
         "https://services.arcgis.com/QVENGdaPbd4LUkLV/arcgis/rest/services/USFWS_Critical_Habitat/FeatureServer/0"),
    ]
    found_ch = "usfws_critical_habitat" in results
    for label, url in ch_candidates:
        if found_ch:
            break
        ok, detail = try_arcgis_service(url)
        icon = "✅" if ok else "❌"
        print(f"  {icon} {label}: {detail[:80]}")
        if ok:
            results["usfws_critical_habitat"] = {"url": url}
            found_ch = True

    # ===================================================================
    # SUMMARY
    # ===================================================================
    print("\n" + "=" * 70)
    print("DISCOVERY SUMMARY")
    print("=" * 70)

    services_needed = [
        "substations", "transmission_lines", "fema_nfhl",
        "epa_envirofacts", "epa_echo",
    ]
    tceq_keys = [k for k in results if k.startswith("tceq_")]
    usfws_keys = [k for k in results if k.startswith("usfws_")]

    for svc in services_needed:
        if svc in results:
            print(f"  ✅ {svc}: {results[svc]['url']}")
        else:
            print(f"  ❌ {svc}: NOT FOUND")

    if tceq_keys:
        for k in tceq_keys:
            print(f"  ✅ {k}: {results[k]['url']}")
    else:
        print(f"  ❌ TCEQ services: NOT FOUND")

    if usfws_keys:
        for k in usfws_keys:
            print(f"  ✅ {k}: {results[k]['url']}")
    else:
        print(f"  ❌ USFWS services: NOT FOUND")

    total_found = len(results)
    print(f"\n  {total_found} endpoints discovered")
    print("=" * 70)

    # Save results
    with open("discovered_endpoints.json", "w") as f:
        json.dump({"timestamp": datetime.now().isoformat(), "results": results}, f, indent=2)
    print(f"\nResults saved to discovered_endpoints.json")
    print("Share the output above so we can update the codebase!")

    return 0


if __name__ == "__main__":
    sys.exit(main())
