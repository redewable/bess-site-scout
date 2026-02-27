'use client'

import { useEffect, useState, useCallback, useMemo } from 'react'
import dynamic from 'next/dynamic'
import SiteTable from '@/components/SiteTable'
import SiteDetail from '@/components/SiteDetail'
import { Site, Meta, GenerationPlant, InterconnectionProject } from '@/types'

const SiteMap = dynamic(() => import('@/components/Map'), { ssr: false })

export default function Dashboard() {
  const [sites, setSites] = useState<Site[]>([])
  const [meta, setMeta] = useState<Meta | null>(null)
  const [generationPlants, setGenerationPlants] = useState<GenerationPlant[]>([])
  const [queueProjects, setQueueProjects] = useState<InterconnectionProject[]>([])
  const [selectedSite, setSelectedSite] = useState<Site | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [view, setView] = useState<'split' | 'map' | 'table'>('split')
  const [showDetail, setShowDetail] = useState(false)

  useEffect(() => {
    fetch('/api/sites')
      .then(res => res.json())
      .then(data => {
        if (data.error) { setError(data.error); return }
        const features = data.geojson?.features || []
        const parsed: Site[] = features.map((f: any) => ({
          ...f.properties,
          lat: f.geometry?.coordinates?.[1],
          lon: f.geometry?.coordinates?.[0],
        }))
        setSites(parsed)
        setMeta(data.meta)

        // Parse generation plant features
        if (data.generationPlants?.features) {
          const plants: GenerationPlant[] = data.generationPlants.features
            .filter((f: any) => f.geometry)
            .map((f: any) => ({
              ...f.properties,
              lat: f.geometry.coordinates[1],
              lon: f.geometry.coordinates[0],
            }))
          setGenerationPlants(plants)
        }

        // Parse interconnection queue features
        if (data.interconnectionQueue?.features) {
          const projects: InterconnectionProject[] = data.interconnectionQueue.features
            .map((f: any) => ({
              ...f.properties,
              lat: f.geometry?.coordinates?.[1] || null,
              lon: f.geometry?.coordinates?.[0] || null,
            }))
          setQueueProjects(projects)
        }
      })
      .catch(err => setError(err.message))
      .finally(() => setLoading(false))
  }, [])

  const handleSiteSelect = useCallback((site: Site) => {
    setSelectedSite(site)
    setShowDetail(true)
  }, [])

  const gradeStats = useMemo(() => {
    if (!meta?.grades) return []
    return ['A', 'B', 'C', 'D', 'F']
      .map(g => ({ grade: g, count: meta.grades[g] || 0 }))
      .filter(g => g.count > 0)
  }, [meta])

  if (loading) {
    return (
      <div className="flex items-center justify-center min-h-screen bg-gray-950">
        <div className="text-center">
          <div className="animate-pulse text-5xl mb-4">&#9889;</div>
          <p className="text-gray-400 text-lg">Loading BESS Site Scout...</p>
          <p className="text-gray-600 text-sm mt-2">Parsing pipeline output</p>
        </div>
      </div>
    )
  }

  if (error) {
    return (
      <div className="flex items-center justify-center min-h-screen bg-gray-950">
        <div className="text-center max-w-md bg-gray-900 rounded-2xl p-8 border border-gray-800">
          <div className="text-5xl mb-4">&#9888;&#65039;</div>
          <p className="text-red-400 text-xl font-bold mb-2">No Pipeline Data</p>
          <p className="text-gray-500 text-sm mb-6">{error}</p>
          <div className="bg-gray-800 rounded-lg p-4 text-left">
            <p className="text-gray-400 text-xs mb-2">Run the pipeline first:</p>
            <code className="text-green-400 text-sm">
              python3 -m src.main --test --verbose
            </code>
          </div>
        </div>
      </div>
    )
  }

  return (
    <div className="flex flex-col h-screen bg-gray-950">
      {/* Header */}
      <header className="border-b border-gray-800 bg-gray-900/80 backdrop-blur-sm px-4 py-2.5 flex items-center justify-between shrink-0">
        <div className="flex items-center gap-3">
          <span className="text-2xl">&#9889;</span>
          <div>
            <h1 className="text-base font-bold tracking-tight text-white">BESS Site Scout</h1>
            <p className="text-[10px] text-gray-500">ReDewable Energy</p>
          </div>
        </div>

        {/* Stats row */}
        <div className="flex items-center gap-5">
          <StatCard label="Sites" value={meta?.total_sites || 0} />
          <StatCard label="Grade A" value={meta?.grades?.A || 0} color="text-green-400" />
          <StatCard label="Grade B" value={meta?.grades?.B || 0} color="text-lime-400" />
          <StatCard label="Avg Score" value={meta?.avg_score || 0} color="text-blue-400" />
          <StatCard label="Avg GHI" value={meta?.avg_ghi || 0} color="text-amber-400" unit="kWh/m&#178;/d" />
          {generationPlants.length > 0 && (
            <StatCard label="Gen Plants" value={generationPlants.length} color="text-purple-400" />
          )}
          {queueProjects.length > 0 && (
            <StatCard label="Queue" value={queueProjects.length} color="text-cyan-400" />
          )}

          {/* Grade bar */}
          <div className="flex h-5 rounded-full overflow-hidden w-32 bg-gray-800">
            {gradeStats.map(({ grade, count }) => {
              const pct = meta ? (count / meta.total_sites) * 100 : 0
              const colors: Record<string, string> = {
                A: 'bg-green-500', B: 'bg-lime-500', C: 'bg-amber-500',
                D: 'bg-red-500', F: 'bg-red-900',
              }
              return (
                <div
                  key={grade}
                  className={`${colors[grade]} transition-all`}
                  style={{ width: `${pct}%` }}
                  title={`Grade ${grade}: ${count} (${pct.toFixed(0)}%)`}
                />
              )
            })}
          </div>

          {/* View toggle */}
          <div className="flex bg-gray-800 rounded-lg p-0.5 gap-0.5">
            {(['split', 'map', 'table'] as const).map(v => (
              <button
                key={v}
                onClick={() => setView(v)}
                className={`px-2.5 py-1 text-xs rounded-md transition-colors ${
                  view === v
                    ? 'bg-blue-600 text-white'
                    : 'text-gray-400 hover:text-white'
                }`}
              >
                {v === 'split' ? 'Split' : v === 'map' ? 'Map' : 'Table'}
              </button>
            ))}
          </div>
        </div>
      </header>

      {/* Main content */}
      <div className="flex-1 flex overflow-hidden">
        {/* Map */}
        {view !== 'table' && (
          <div className={`${
            view === 'map'
              ? (showDetail && selectedSite ? 'flex-1' : 'w-full')
              : (showDetail && selectedSite ? 'w-5/12' : 'w-1/2')
          } p-2`}>
            <div className="h-full rounded-lg overflow-hidden border border-gray-800">
              <SiteMap
                sites={sites}
                selectedSite={selectedSite}
                onSiteSelect={handleSiteSelect}
                generationPlants={generationPlants}
                queueProjects={queueProjects}
              />
            </div>
          </div>
        )}

        {/* Table */}
        {view !== 'map' && (
          <div className={`${
            view === 'table'
              ? (showDetail && selectedSite ? 'flex-1' : 'w-full')
              : (showDetail && selectedSite ? 'w-5/12' : 'w-1/2')
          } p-2 ${view === 'split' ? 'pl-0' : ''} flex flex-col`}>
            <SiteTable
              sites={sites}
              selectedSite={selectedSite}
              onSiteSelect={handleSiteSelect}
            />
          </div>
        )}

        {/* Detail Panel (slides in from right) */}
        {showDetail && selectedSite && (
          <div className="w-[380px] shrink-0 border-l border-gray-800">
            <SiteDetail
              site={selectedSite}
              onClose={() => { setShowDetail(false); setSelectedSite(null) }}
            />
          </div>
        )}
      </div>
    </div>
  )
}

function StatCard({ label, value, color = 'text-white', unit }: {
  label: string; value: number | string; color?: string; unit?: string
}) {
  return (
    <div className="text-center">
      <div className={`text-lg font-bold ${color} leading-tight`}>
        {typeof value === 'number' ? value.toLocaleString() : value}
        {unit && <span className="text-[9px] text-gray-500 ml-0.5" dangerouslySetInnerHTML={{ __html: unit }} />}
      </div>
      <div className="text-[9px] uppercase tracking-wider text-gray-500">{label}</div>
    </div>
  )
}
