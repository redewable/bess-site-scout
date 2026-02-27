export interface Site {
  rank: number
  grade: string
  composite_score: number
  substation_name: string
  substation_voltage_kv: number
  volt_class: string
  lat: number
  lon: number
  connected_lines: number
  distance_to_substation_mi: number

  // HIFLD enrichment
  owner: string
  operator: string
  sub_status: string
  city: string
  state: string
  county: string
  sub_type: string
  hifld_lines: number
  max_volt: number
  min_volt: number

  // Solar / Grid
  ghi_annual: number
  solar_co_location: string
  nearby_generation_mw: number

  // Sub-scores (flattened from composite scorer)
  sub_scores_proximity_score: number
  sub_scores_proximity_weight: number
  sub_scores_voltage_score: number
  sub_scores_voltage_weight: number
  sub_scores_environmental_score: number
  sub_scores_environmental_weight: number
  sub_scores_land_cost_score: number
  sub_scores_land_cost_weight: number
  sub_scores_parcel_size_score: number
  sub_scores_parcel_size_weight: number
  sub_scores_flood_risk_score: number
  sub_scores_flood_risk_weight: number
  sub_scores_grid_density_score: number
  sub_scores_grid_density_weight: number
  sub_scores_solar_resource_score: number
  sub_scores_solar_resource_weight: number

  // Environmental screening (flattened)
  environmental_score: number
  environmental_grade: string
  environmental_eliminate: boolean

  // Flood screening (flattened)
  flood_flood_zone: string
  flood_in_sfha: boolean
  flood_risk_level: string
  flood_zones_present: string
  flood_floodplain_pct: number
  flood_details: string

  // EPA screening (flattened)
  epa_superfund_count: number
  epa_superfund_nearest_distance_mi: number
  epa_brownfields_count: number
  epa_tri_count: number
  epa_echo_summary_total_facilities: number
  epa_echo_summary_significant_violations: number

  // USFWS screening (flattened)
  usfws_wetlands_count: number
  usfws_wetlands_total_acres: number
  usfws_wetlands_intersection_pct: number
  usfws_critical_habitat_present: boolean
  usfws_critical_habitat_species: string

  // EIA grid assessment (flattened)
  eia_grid_density_score: number
  eia_nearby_plants: number
  eia_nearby_capacity_mw: number

  // NREL solar (flattened)
  nrel_ghi_annual: number
  nrel_dni_annual: number
  nrel_solar_score: number
  nrel_co_location_potential: string

  // Risk flags
  risk_flags: string

  // Allow additional flattened properties
  [key: string]: any
}

export interface Meta {
  total_sites: number
  grades: Record<string, number>
  avg_score: number
  max_score: number
  avg_ghi: number
  voltage_distribution: Record<string, number>
  state_distribution: Record<string, number>
  generated: string
  filename: string
}
