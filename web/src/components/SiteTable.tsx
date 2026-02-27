'use client'

import { useState, useMemo } from 'react'
import { Site } from '@/types'

interface SiteTableProps {
  sites: Site[]
  selectedSite: Site | null
  onSiteSelect: (site: Site) => void
}

type SortKey = 'rank' | 'composite_score' | 'substation_name' | 'volt_class' | 'grade' | 'ghi_annual' | 'nearby_generation_mw' | 'connected_lines' | 'state' | 'owner' | 'environmental_score'
type SortDir = 'asc' | 'desc'

const GRADE_CLASSES: Record<string, string> = {
  A: 'text-green-400',
  B: 'text-lime-400',
  C: 'text-amber-400',
  D: 'text-red-400',
  F: 'text-red-700',
}

export default function SiteTable({ sites, selectedSite, onSiteSelect }: SiteTableProps) {
  const [sortKey, setSortKey] = useState<SortKey>('rank')
  const [sortDir, setSortDir] = useState<SortDir>('asc')
  const [filterGrade, setFilterGrade] = useState<string>('all')
  const [filterVoltage, setFilterVoltage] = useState<string>('all')
  const [filterState, setFilterState] = useState<string>('all')
  const [search, setSearch] = useState('')
  const [page, setPage] = useState(0)
  const PAGE_SIZE = 50

  const toggleSort = (key: SortKey) => {
    if (sortKey === key) {
      setSortDir(d => (d === 'asc' ? 'desc' : 'asc'))
    } else {
      setSortKey(key)
      setSortDir(key === 'rank' ? 'asc' : 'desc')
    }
  }

  const filtered = useMemo(() => {
    let result = [...sites]

    if (filterGrade !== 'all') result = result.filter(s => s.grade === filterGrade)
    if (filterVoltage !== 'all') result = result.filter(s => s.volt_class === filterVoltage)
    if (filterState !== 'all') result = result.filter(s => s.state === filterState)
    if (search.trim()) {
      const q = search.toLowerCase()
      result = result.filter(s =>
        s.substation_name?.toLowerCase().includes(q) ||
        s.owner?.toLowerCase().includes(q) ||
        s.city?.toLowerCase().includes(q) ||
        s.county?.toLowerCase().includes(q)
      )
    }

    result.sort((a, b) => {
      const aVal = a[sortKey] ?? 0
      const bVal = b[sortKey] ?? 0
      if (typeof aVal === 'string' && typeof bVal === 'string') {
        return sortDir === 'asc' ? aVal.localeCompare(bVal) : bVal.localeCompare(aVal)
      }
      const diff = (aVal as number) - (bVal as number)
      return sortDir === 'asc' ? diff : -diff
    })

    return result
  }, [sites, sortKey, sortDir, filterGrade, filterVoltage, filterState, search])

  const pageCount = Math.ceil(filtered.length / PAGE_SIZE)
  const paged = filtered.slice(page * PAGE_SIZE, (page + 1) * PAGE_SIZE)

  const voltageClasses = [...new Set(sites.map(s => s.volt_class))].sort()
  const states = [...new Set(sites.map(s => s.state).filter(Boolean))].sort()

  return (
    <div className="flex flex-col h-full">
      {/* Filters */}
      <div className="flex flex-wrap gap-2 mb-2 px-1">
        <input
          type="text"
          placeholder="Search substations, owners, cities..."
          value={search}
          onChange={(e) => { setSearch(e.target.value); setPage(0) }}
          className="bg-gray-800 border border-gray-700 rounded-lg px-3 py-1.5 text-xs
                     text-white placeholder-gray-500 focus:outline-none focus:border-blue-500 flex-1 min-w-[140px]"
        />
        <select
          value={filterGrade}
          onChange={(e) => { setFilterGrade(e.target.value); setPage(0) }}
          className="bg-gray-800 border border-gray-700 rounded-lg px-2 py-1.5 text-xs text-white"
        >
          <option value="all">All Grades</option>
          {['A','B','C','D','F'].map(g => <option key={g} value={g}>Grade {g}</option>)}
        </select>
        <select
          value={filterVoltage}
          onChange={(e) => { setFilterVoltage(e.target.value); setPage(0) }}
          className="bg-gray-800 border border-gray-700 rounded-lg px-2 py-1.5 text-xs text-white"
        >
          <option value="all">All Voltages</option>
          {voltageClasses.map(vc => <option key={vc} value={vc}>{vc} kV</option>)}
        </select>
        {states.length > 1 && (
          <select
            value={filterState}
            onChange={(e) => { setFilterState(e.target.value); setPage(0) }}
            className="bg-gray-800 border border-gray-700 rounded-lg px-2 py-1.5 text-xs text-white"
          >
            <option value="all">All States</option>
            {states.map(st => <option key={st} value={st}>{st}</option>)}
          </select>
        )}
      </div>

      {/* Table */}
      <div className="flex-1 overflow-auto rounded-lg border border-gray-800">
        <table className="w-full text-xs">
          <thead className="sticky top-0 bg-gray-900 z-10">
            <tr className="border-b border-gray-800">
              <Th col="rank" current={sortKey} dir={sortDir} onClick={toggleSort}>Rank</Th>
              <Th col="grade" current={sortKey} dir={sortDir} onClick={toggleSort}>Grade</Th>
              <Th col="composite_score" current={sortKey} dir={sortDir} onClick={toggleSort}>Score</Th>
              <Th col="substation_name" current={sortKey} dir={sortDir} onClick={toggleSort}>Substation</Th>
              <Th col="state" current={sortKey} dir={sortDir} onClick={toggleSort}>State</Th>
              <Th col="owner" current={sortKey} dir={sortDir} onClick={toggleSort}>Owner</Th>
              <Th col="volt_class" current={sortKey} dir={sortDir} onClick={toggleSort}>Voltage</Th>
              <Th col="connected_lines" current={sortKey} dir={sortDir} onClick={toggleSort}>Lines</Th>
              <Th col="environmental_score" current={sortKey} dir={sortDir} onClick={toggleSort}>Env</Th>
              <Th col="ghi_annual" current={sortKey} dir={sortDir} onClick={toggleSort}>GHI</Th>
              <Th col="nearby_generation_mw" current={sortKey} dir={sortDir} onClick={toggleSort}>Gen MW</Th>
            </tr>
          </thead>
          <tbody>
            {paged.map((site) => (
              <tr
                key={`${site.rank}-${site.substation_name}`}
                onClick={() => onSiteSelect(site)}
                className={`border-b border-gray-800/40 cursor-pointer transition-colors
                  ${selectedSite?.rank === site.rank
                    ? 'bg-blue-900/30 border-blue-700'
                    : 'hover:bg-gray-800/40'
                  }`}
              >
                <td className="px-2 py-1.5 text-gray-500 font-mono">#{site.rank}</td>
                <td className={`px-2 py-1.5 font-bold ${GRADE_CLASSES[site.grade] || 'text-gray-400'}`}>
                  {site.grade}
                </td>
                <td className="px-2 py-1.5 font-mono text-white">{site.composite_score}</td>
                <td className="px-2 py-1.5 text-white font-medium truncate max-w-[160px]">
                  {site.substation_name}
                </td>
                <td className="px-2 py-1.5 text-gray-400">{site.state || '-'}</td>
                <td className="px-2 py-1.5 text-gray-400 truncate max-w-[100px]">{site.owner || '-'}</td>
                <td className="px-2 py-1.5 text-gray-300">{site.volt_class}</td>
                <td className="px-2 py-1.5 text-gray-400 text-center">{site.connected_lines || '-'}</td>
                <td className={`px-2 py-1.5 font-mono ${
                  (site.environmental_score ?? 100) >= 80 ? 'text-green-400' :
                  (site.environmental_score ?? 100) >= 60 ? 'text-amber-400' : 'text-red-400'
                }`}>
                  {site.environmental_score ?? '-'}
                </td>
                <td className="px-2 py-1.5 text-amber-400 font-mono">
                  {site.ghi_annual ? Number(site.ghi_annual).toFixed(1) : '-'}
                </td>
                <td className="px-2 py-1.5 text-gray-400 font-mono">
                  {site.nearby_generation_mw ? Math.round(site.nearby_generation_mw).toLocaleString() : '-'}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {/* Pagination */}
      <div className="flex items-center justify-between mt-2 px-1 text-xs text-gray-400">
        <span>{filtered.length} sites</span>
        <div className="flex gap-1.5">
          <button
            onClick={() => setPage(p => Math.max(0, p - 1))}
            disabled={page === 0}
            className="px-2.5 py-1 rounded bg-gray-800 hover:bg-gray-700 disabled:opacity-30"
          >
            Prev
          </button>
          <span className="px-2 py-1">{page + 1} / {pageCount || 1}</span>
          <button
            onClick={() => setPage(p => Math.min(pageCount - 1, p + 1))}
            disabled={page >= pageCount - 1}
            className="px-2.5 py-1 rounded bg-gray-800 hover:bg-gray-700 disabled:opacity-30"
          >
            Next
          </button>
        </div>
      </div>
    </div>
  )
}

function Th({ col, current, dir, onClick, children }: {
  col: SortKey; current: SortKey; dir: SortDir;
  onClick: (k: SortKey) => void; children: React.ReactNode
}) {
  const active = current === col
  return (
    <th
      className="px-2 py-2 text-left cursor-pointer hover:text-blue-400 whitespace-nowrap select-none"
      onClick={() => onClick(col)}
    >
      {children}
      <span className={`ml-0.5 ${active ? 'text-blue-400' : 'text-gray-700'}`}>
        {active ? (dir === 'asc' ? '\u25B2' : '\u25BC') : '\u2195'}
      </span>
    </th>
  )
}
