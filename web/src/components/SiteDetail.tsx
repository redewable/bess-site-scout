'use client'

import { Site } from '@/types'

interface SiteDetailProps {
  site: Site
  onClose: () => void
}

const GRADE_COLORS: Record<string, string> = {
  A: 'text-green-400',
  B: 'text-lime-400',
  C: 'text-amber-400',
  D: 'text-red-400',
  F: 'text-red-700',
}

const GRADE_BG: Record<string, string> = {
  A: 'bg-green-500/20 border-green-500/40',
  B: 'bg-lime-500/20 border-lime-500/40',
  C: 'bg-amber-500/20 border-amber-500/40',
  D: 'bg-red-500/20 border-red-500/40',
  F: 'bg-red-900/20 border-red-700/40',
}

// Scoring factor metadata for explanations
const SCORING_FACTORS = [
  {
    key: 'proximity',
    label: 'Grid Proximity',
    icon: '\u26A1',
    description: 'Distance to nearest qualifying substation. Closer = lower interconnection costs.',
    scale: '0 mi = 100, exponential decay',
  },
  {
    key: 'voltage',
    label: 'Voltage Class',
    icon: '\u{1F50B}',
    description: 'Higher voltage substations support larger BESS capacity with lower losses.',
    scale: '345kV+ = 100, 220kV = 75, 161kV = 50, <161kV = 25',
  },
  {
    key: 'environmental',
    label: 'Environmental Risk',
    icon: '\u{1F33F}',
    description: 'Composite of FEMA flood, EPA Superfund/TRI, TCEQ, USFWS wetlands/habitat.',
    scale: '100 = clean site, penalties for each risk factor',
  },
  {
    key: 'land_cost',
    label: 'Land Cost',
    icon: '\u{1F4B0}',
    description: 'Estimated land acquisition cost per acre. Lower cost = higher score.',
    scale: 'Linear from $0 (100) to max config price (0)',
  },
  {
    key: 'parcel_size',
    label: 'Parcel Size',
    icon: '\u{1F4CF}',
    description: 'How close the parcel is to ideal BESS footprint (40 acres default).',
    scale: 'Gaussian curve centered on ideal size',
  },
  {
    key: 'flood_risk',
    label: 'Flood Risk',
    icon: '\u{1F30A}',
    description: 'FEMA National Flood Hazard Layer assessment.',
    scale: 'Low = 100, Moderate = 50, Undetermined = 30, High = 0',
  },
  {
    key: 'grid_density',
    label: 'Grid Density',
    icon: '\u{1F3ED}',
    description: 'Nearby power generation capacity (EIA). More generation = stronger grid.',
    scale: 'Based on MW within 15mi radius',
  },
  {
    key: 'solar_resource',
    label: 'Solar Resource',
    icon: '\u2600\uFE0F',
    description: 'NREL Global Horizontal Irradiance (GHI). Higher = better solar co-location.',
    scale: '5.5+ kWh/m\u00B2/day = 100, <3.5 = 15',
  },
]

function ScoreBar({ score, maxScore = 100, color }: { score: number; maxScore?: number; color: string }) {
  const pct = Math.min(100, (score / maxScore) * 100)
  return (
    <div className="w-full h-2 bg-gray-800 rounded-full overflow-hidden">
      <div
        className={`h-full rounded-full transition-all ${color}`}
        style={{ width: `${pct}%` }}
      />
    </div>
  )
}

function getScoreColor(score: number): string {
  if (score >= 80) return 'bg-green-500'
  if (score >= 60) return 'bg-lime-500'
  if (score >= 40) return 'bg-amber-500'
  if (score >= 20) return 'bg-red-500'
  return 'bg-red-900'
}

export default function SiteDetail({ site, onClose }: SiteDetailProps) {
  const gradeColor = GRADE_COLORS[site.grade] || 'text-gray-400'
  const gradeBg = GRADE_BG[site.grade] || 'bg-gray-800 border-gray-700'

  return (
    <div className="h-full overflow-y-auto bg-gray-900 border-l border-gray-800">
      {/* Header */}
      <div className="sticky top-0 bg-gray-900/95 backdrop-blur-sm border-b border-gray-800 px-4 py-3 z-10">
        <div className="flex items-start justify-between">
          <div className="flex-1 min-w-0">
            <div className="flex items-center gap-2 mb-1">
              <span className="text-gray-500 text-xs font-mono">#{site.rank}</span>
              <span className={`text-sm font-bold px-2 py-0.5 rounded border ${gradeBg} ${gradeColor}`}>
                Grade {site.grade}
              </span>
              <span className="text-white font-bold text-lg">{site.composite_score}</span>
              <span className="text-gray-500 text-xs">/ 100</span>
            </div>
            <h2 className="text-white font-bold text-base truncate">{site.substation_name}</h2>
            {(site.city || site.state) && (
              <p className="text-gray-500 text-xs">
                {[site.city, site.county, site.state].filter(Boolean).join(', ')}
              </p>
            )}
          </div>
          <button
            onClick={onClose}
            className="text-gray-500 hover:text-white text-lg px-2 shrink-0"
          >
            &#10005;
          </button>
        </div>
      </div>

      <div className="px-4 py-3 space-y-4">
        {/* Substation Info */}
        <Section title="Substation Details" icon="&#9889;">
          <InfoGrid>
            <InfoItem label="Voltage Class" value={site.volt_class || 'N/A'} />
            <InfoItem label="Max Voltage" value={site.max_volt ? `${site.max_volt} kV` : 'N/A'} />
            <InfoItem label="Connected Lines" value={String(site.connected_lines || site.hifld_lines || 'N/A')} />
            <InfoItem label="Owner" value={site.owner || 'Not available'} />
            <InfoItem label="Operator" value={site.operator || 'Not available'} />
            <InfoItem label="Status" value={site.sub_status || 'N/A'} />
            <InfoItem label="Type" value={site.sub_type || 'N/A'} />
            <InfoItem
              label="Coordinates"
              value={`${site.lat?.toFixed(5)}, ${site.lon?.toFixed(5)}`}
              mono
            />
          </InfoGrid>
        </Section>

        {/* Scoring Breakdown */}
        <Section title="Scoring Breakdown" icon="&#128202;">
          <div className="space-y-2.5">
            {SCORING_FACTORS.map((factor) => {
              const scoreKey = `sub_scores_${factor.key}_score`
              const weightKey = `sub_scores_${factor.key}_weight`
              const score = site[scoreKey] ?? null
              const weight = site[weightKey] ?? null

              if (score === null) return null

              const weighted = weight ? (score * weight).toFixed(1) : '—'

              return (
                <div key={factor.key} className="group">
                  <div className="flex items-center justify-between mb-1">
                    <div className="flex items-center gap-1.5">
                      <span className="text-xs">{factor.icon}</span>
                      <span className="text-xs text-gray-300 font-medium">{factor.label}</span>
                      {weight && (
                        <span className="text-[10px] text-gray-600">{(weight * 100).toFixed(0)}%</span>
                      )}
                    </div>
                    <div className="flex items-center gap-2">
                      <span className="text-xs font-mono text-gray-400">{score.toFixed(1)}</span>
                      <span className="text-[10px] text-gray-600">\u00D7{weight?.toFixed(2)}</span>
                      <span className="text-xs font-mono text-white font-bold">{weighted}</span>
                    </div>
                  </div>
                  <ScoreBar score={score} color={getScoreColor(score)} />
                  <p className="text-[10px] text-gray-600 mt-0.5 hidden group-hover:block">
                    {factor.description}
                  </p>
                </div>
              )
            })}
          </div>

          {/* Grading criteria */}
          <div className="mt-3 pt-3 border-t border-gray-800">
            <p className="text-[10px] text-gray-500 mb-1.5 uppercase tracking-wider font-semibold">Grading Scale</p>
            <div className="flex gap-2 text-[10px]">
              <span className="text-green-400">A: \u226580</span>
              <span className="text-lime-400">B: \u226565</span>
              <span className="text-amber-400">C: \u226550</span>
              <span className="text-red-400">D: \u226535</span>
              <span className="text-red-700">F: &lt;35</span>
            </div>
          </div>
        </Section>

        {/* Environmental Screening */}
        <Section title="Environmental Screening" icon="&#127807;">
          <div className="space-y-2">
            <div className="flex items-center justify-between">
              <span className="text-xs text-gray-400">Environmental Score</span>
              <span className={`text-sm font-bold ${
                (site.environmental_score ?? 100) >= 80 ? 'text-green-400' :
                (site.environmental_score ?? 100) >= 60 ? 'text-amber-400' : 'text-red-400'
              }`}>
                {site.environmental_score ?? 'N/A'} / 100
              </span>
            </div>
            {site.environmental_eliminate && (
              <div className="bg-red-900/30 border border-red-700/50 rounded px-2 py-1 text-xs text-red-400">
                &#9888;&#65039; Environmental elimination criteria triggered
              </div>
            )}
          </div>

          {/* FEMA Flood */}
          <ScreeningCard
            title="FEMA Flood Zones"
            status={site.flood_risk_level === 'low' ? 'pass' :
                    site.flood_risk_level === 'high' ? 'fail' :
                    site.flood_risk_level === 'moderate' ? 'warn' : 'unknown'}
          >
            <InfoGrid cols={2}>
              <InfoItem label="Flood Zone" value={site.flood_flood_zone || 'N/A'} />
              <InfoItem label="Risk Level" value={site.flood_risk_level || 'Unknown'} />
              <InfoItem label="In SFHA" value={site.flood_in_sfha ? 'Yes' : 'No'} />
              <InfoItem label="Floodplain %" value={site.flood_floodplain_pct ? `${site.flood_floodplain_pct}%` : '0%'} />
            </InfoGrid>
            {site.flood_details && (
              <p className="text-[10px] text-gray-500 mt-1">{site.flood_details}</p>
            )}
          </ScreeningCard>

          {/* EPA */}
          <ScreeningCard
            title="EPA Databases (Phase I ESA)"
            status={
              (site.epa_superfund_count > 0 && site.epa_superfund_nearest_distance_mi < 0.25) ? 'fail' :
              site.epa_superfund_count > 0 ? 'warn' :
              site.epa_echo_summary_significant_violations > 0 ? 'warn' : 'pass'
            }
          >
            <InfoGrid cols={2}>
              <InfoItem label="Superfund/NPL" value={`${site.epa_superfund_count || 0} sites`} />
              <InfoItem label="NPL Nearest" value={site.epa_superfund_nearest_distance_mi ? `${site.epa_superfund_nearest_distance_mi} mi` : 'None'} />
              <InfoItem label="Brownfields" value={`${site.epa_brownfields_count || 0} sites`} />
              <InfoItem label="TRI Facilities" value={`${site.epa_tri_count || 0}`} />
              <InfoItem label="ECHO Facilities" value={`${site.epa_echo_summary_total_facilities || 0}`} />
              <InfoItem label="Sig. Violations" value={`${site.epa_echo_summary_significant_violations || 0}`} />
            </InfoGrid>
          </ScreeningCard>

          {/* USFWS */}
          <ScreeningCard
            title="USFWS Wetlands & Habitat"
            status={
              site.usfws_critical_habitat_present ? 'fail' :
              (site.usfws_wetlands_count || 0) > 0 ? 'warn' : 'pass'
            }
          >
            <InfoGrid cols={2}>
              <InfoItem label="NWI Wetlands" value={`${site.usfws_wetlands_count || 0} features`} />
              <InfoItem label="Wetland Acres" value={site.usfws_wetlands_total_acres ? `${site.usfws_wetlands_total_acres} ac` : '0'} />
              <InfoItem label="Critical Habitat" value={site.usfws_critical_habitat_present ? 'YES' : 'No'} />
              <InfoItem label="Species" value={site.usfws_critical_habitat_species || 'None'} />
            </InfoGrid>
          </ScreeningCard>
        </Section>

        {/* Grid & Solar Assessment */}
        <Section title="Grid & Solar Assessment" icon="&#9728;&#65039;">
          <InfoGrid cols={2}>
            <InfoItem label="Nearby Plants" value={`${site.eia_nearby_plants || 0}`} />
            <InfoItem label="Nearby Capacity" value={site.eia_nearby_capacity_mw ? `${Math.round(site.eia_nearby_capacity_mw).toLocaleString()} MW` : 'N/A'} />
            <InfoItem label="Grid Density Score" value={`${site.eia_grid_density_score || 'N/A'} / 100`} />
            <InfoItem label="Solar GHI" value={site.ghi_annual ? `${site.ghi_annual} kWh/m\u00B2/d` : 'N/A'} />
            <InfoItem label="Solar DNI" value={site.nrel_dni_annual ? `${site.nrel_dni_annual} kWh/m\u00B2/d` : 'N/A'} />
            <InfoItem label="Co-location" value={site.solar_co_location || site.nrel_co_location_potential || 'N/A'} />
          </InfoGrid>
        </Section>

        {/* Risk Flags */}
        {site.risk_flags && site.risk_flags.length > 0 && (
          <Section title="Risk Flags" icon="&#9888;&#65039;">
            <div className="space-y-1">
              {site.risk_flags.split('; ').filter(Boolean).map((flag: string, i: number) => (
                <div
                  key={i}
                  className={`text-xs px-2 py-1 rounded ${
                    flag.startsWith('CRITICAL') || flag.startsWith('ELIMINATE')
                      ? 'bg-red-900/30 text-red-400 border border-red-800/40'
                      : flag.startsWith('WARNING')
                      ? 'bg-amber-900/30 text-amber-400 border border-amber-800/40'
                      : 'bg-gray-800 text-gray-400 border border-gray-700'
                  }`}
                >
                  {flag}
                </div>
              ))}
            </div>
          </Section>
        )}

        {/* Methodology */}
        <Section title="Methodology" icon="&#128218;">
          <div className="text-[10px] text-gray-500 space-y-1.5">
            <p>
              Sites are scored using an 8-factor weighted composite model aligned with
              BESS interconnection feasibility criteria.
            </p>
            <p>
              Environmental screening follows ASTM E1527-21 Phase I ESA standards,
              querying FEMA NFHL, EPA (Superfund NPL, Brownfields, TRI, ECHO),
              TCEQ (LPST, UST, IHW, MSW), and USFWS (NWI wetlands, critical habitat).
            </p>
            <p>
              Grid assessment uses EIA power plant data for nearby generation capacity
              and NREL Solar Resource API for co-location potential.
            </p>
            <p>
              Substation data is derived from HIFLD transmission line endpoints and
              enriched with the HIFLD Substations dataset (owner, operator, status).
            </p>
          </div>
        </Section>
      </div>
    </div>
  )
}

function Section({ title, icon, children }: { title: string; icon: string; children: React.ReactNode }) {
  return (
    <div>
      <h3 className="text-xs font-bold text-gray-300 uppercase tracking-wider mb-2 flex items-center gap-1.5">
        <span dangerouslySetInnerHTML={{ __html: icon }} />
        {title}
      </h3>
      {children}
    </div>
  )
}

function InfoGrid({ children, cols = 2 }: { children: React.ReactNode; cols?: number }) {
  return (
    <div className={`grid ${cols === 2 ? 'grid-cols-2' : 'grid-cols-2'} gap-x-4 gap-y-1`}>
      {children}
    </div>
  )
}

function InfoItem({ label, value, mono = false }: { label: string; value: string; mono?: boolean }) {
  return (
    <div className="flex items-baseline justify-between py-0.5">
      <span className="text-[10px] text-gray-500 shrink-0">{label}</span>
      <span className={`text-xs text-gray-200 text-right ${mono ? 'font-mono text-[10px]' : ''}`}>
        {value || '—'}
      </span>
    </div>
  )
}

function ScreeningCard({
  title,
  status,
  children,
}: {
  title: string
  status: 'pass' | 'warn' | 'fail' | 'unknown'
  children: React.ReactNode
}) {
  const statusColors = {
    pass: 'border-green-800/40 bg-green-900/10',
    warn: 'border-amber-800/40 bg-amber-900/10',
    fail: 'border-red-800/40 bg-red-900/10',
    unknown: 'border-gray-700 bg-gray-800/30',
  }
  const statusIcons = {
    pass: '\u2705',
    warn: '\u26A0\uFE0F',
    fail: '\u274C',
    unknown: '\u2753',
  }

  return (
    <div className={`mt-2 rounded-lg border p-2 ${statusColors[status]}`}>
      <div className="flex items-center gap-1.5 mb-1.5">
        <span className="text-xs">{statusIcons[status]}</span>
        <span className="text-[10px] font-semibold text-gray-300 uppercase tracking-wider">{title}</span>
      </div>
      {children}
    </div>
  )
}
