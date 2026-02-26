# BESS Site Scout
### Battery Energy Storage System — Automated Site Prospecting Agent

**ReDewable Energy — Internal Use Only**

An automated agent that identifies optimal parcels for utility-scale battery energy storage (BESS) development by cross-referencing transmission infrastructure, real estate availability, and environmental risk data.

## Architecture

```
┌─────────────────────────────────────────────────────┐
│                   BESS Site Scout                     │
├──────────┬──────────┬──────────┬────────────────────┤
│  GRID    │  REAL    │  ENVIRO  │  SCORING &         │
│  INTEL   │  ESTATE  │  SCREEN  │  RANKING           │
├──────────┼──────────┼──────────┼────────────────────┤
│ HIFLD    │ County   │ FEMA     │ Weighted scoring   │
│ Subs     │ CAD      │ NFHL     │ Hard eliminators   │
│ 161-345  │ Parcel   │ EPA      │ Red/yellow/green   │
│ kV Lines │ Data     │ Enviro   │ flags              │
│ ERCOT    │ Land     │ TCEQ     │ Proximity ranking  │
│ Queue    │ Listings │ NWI/FWS  │ Cost estimation    │
│          │ APIs     │ RRC O&G  │                    │
└──────────┴──────────┴──────────┴────────────────────┘
```

## Data Sources (All Free/Public)

### Grid Infrastructure
| Source | Data | API Type | Cost |
|--------|------|----------|------|
| HIFLD Open Data | Transmission lines, substations | ArcGIS REST | Free |
| EIA Energy Atlas | Transmission lines (validation) | ArcGIS REST | Free |
| ERCOT | Interconnection queue, planning | CSV/Reports | Free |

### Environmental Screening (Phase I ESA Desktop)
| Source | Data | API Type | Cost |
|--------|------|----------|------|
| FEMA NFHL | Flood zones, BFE | ArcGIS REST | Free |
| EPA Envirofacts | SEMS, RCRA, TRI, Brownfields | REST API | Free |
| EPA ECHO | Enforcement & compliance | REST API | Free |
| TCEQ GIS Hub | LPST, UST, IHW, MSW, spills | ArcGIS REST | Free |
| USFWS NWI | Wetlands | WMS/REST | Free |
| USFWS Critical Habitat | T&E species habitat | ArcGIS REST | Free |
| TX RRC | Oil/gas wells, pipelines | GIS Viewer | Free |
| USDA | Soil survey | REST API | Free |

### Real Estate
| Source | Data | API Type | Cost |
|--------|------|----------|------|
| County CAD | Parcels, ownership, values | Varies | Free |
| RentCast | Property records, listings | REST API | Freemium |
| LandWatch/Lands of TX | Land listings | Scraping | Free |

## Quick Start

```bash
pip install -r requirements.txt
cp config/config.example.yaml config/config.yaml
# Edit config.yaml with your parameters
python -m src.main
```

## Project Structure

```
bess-site-scout/
├── config/
│   ├── config.yaml          # User configuration
│   ├── api_endpoints.yaml   # All API endpoint definitions
│   └── scoring_weights.yaml # Scoring model parameters
├── data/
│   ├── raw/                 # Raw API responses
│   ├── processed/           # Cleaned/filtered data
│   └── cache/               # Cached query results
├── src/
│   ├── ingestion/           # Data source connectors
│   │   ├── hifld.py         # HIFLD substations & lines
│   │   ├── fema.py          # FEMA flood data
│   │   ├── epa.py           # EPA Envirofacts + ECHO
│   │   ├── tceq.py          # TCEQ state databases
│   │   ├── usfws.py         # Wetlands + critical habitat
│   │   ├── rrc.py           # TX Railroad Commission
│   │   └── real_estate.py   # Land/parcel data
│   ├── analysis/
│   │   ├── proximity.py     # Spatial proximity calculations
│   │   └── parcel_filter.py # Parcel criteria matching
│   ├── scoring/
│   │   ├── environmental.py # Enviro risk scoring
│   │   ├── grid.py          # Grid connectivity scoring
│   │   └── composite.py     # Final composite score
│   ├── utils/
│   │   ├── geo.py           # Geospatial utilities
│   │   ├── api_client.py    # Base API client w/ caching
│   │   └── export.py        # Report generation
│   └── main.py              # Main orchestrator
├── tests/
├── docs/
├── output/                  # Generated reports & maps
├── requirements.txt
└── README.md
```
