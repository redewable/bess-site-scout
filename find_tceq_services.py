#!/usr/bin/env python3
"""
Targeted search of TCEQ's 652 ArcGIS services to find LPST, PST, IHW, MSW.
Dumps all service names and highlights likely matches.

Usage:
    python3 find_tceq_services.py
"""

import requests
import json

requests.packages.urllib3.disable_warnings()
BASE = "https://services.arcgis.com/KTcxiTD9dsQw4r7Z/arcgis/rest/services"

# Keywords to look for (case-insensitive)
KEYWORDS = [
    "lpst", "leak", "petroleum", "storage", "tank", "ust", "ast",
    "hazard", "waste", "ihw", "msw", "landfill", "solid_waste",
    "spill", "dryclean", "contamina", "cleanup", "remediat",
    "superfund", "brownfield", "pst", "corrective",
]

print("Fetching TCEQ service directory...")
r = requests.get(f"{BASE}?f=json", timeout=30, verify=False)
services = r.json().get("services", [])
folders = r.json().get("folders", [])

print(f"Found {len(services)} services, {len(folders)} folders\n")

# Check folders too
for folder in folders:
    try:
        fr = requests.get(f"{BASE}/{folder}?f=json", timeout=15, verify=False)
        folder_svcs = fr.json().get("services", [])
        for s in folder_svcs:
            s["_folder"] = folder
        services.extend(folder_svcs)
        print(f"  Folder '{folder}': {len(folder_svcs)} services")
    except:
        pass

print(f"\nTotal services (inc. folders): {len(services)}")
print("\n" + "=" * 60)
print("MATCHING SERVICES:")
print("=" * 60)

matches = []
for svc in services:
    name = svc.get("name", "")
    stype = svc.get("type", "FeatureServer")
    name_lower = name.lower()

    matched_keywords = [k for k in KEYWORDS if k in name_lower]
    if matched_keywords:
        url = f"{BASE}/{name}/{stype}/0"
        matches.append({"name": name, "type": stype, "url": url, "keywords": matched_keywords})
        print(f"\n  🎯 {name} ({stype})")
        print(f"     Keywords: {matched_keywords}")
        print(f"     URL: {url}")

        # Quick test
        try:
            tr = requests.get(f"{url}?f=json", timeout=10, verify=False)
            meta = tr.json()
            if "error" not in meta:
                fields = [f["name"] for f in meta.get("fields", [])]
                print(f"     ✅ WORKS — {len(fields)} fields: {', '.join(fields[:8])}")
            else:
                print(f"     ❌ Error: {meta['error'].get('message', '')}")
        except Exception as e:
            print(f"     ❌ {str(e)[:60]}")

print(f"\n{'=' * 60}")
print(f"Found {len(matches)} matching services out of {len(services)} total")
print("=" * 60)

# Save full service list
with open("tceq_all_services.json", "w") as f:
    json.dump({
        "total": len(services),
        "matches": matches,
        "all_names": sorted([s.get("name", "") for s in services]),
    }, f, indent=2)
print(f"\nFull list saved to tceq_all_services.json")
