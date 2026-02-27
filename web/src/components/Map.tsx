'use client'

import { useEffect, useRef, useCallback } from 'react'
import L from 'leaflet'
import { Site } from '@/types'

interface MapProps {
  sites: Site[]
  selectedSite: Site | null
  onSiteSelect: (site: Site) => void
}

const GRADE_COLORS: Record<string, string> = {
  A: '#22c55e',
  B: '#84cc16',
  C: '#f59e0b',
  D: '#ef4444',
  F: '#991b1b',
}

export default function SiteMap({ sites, selectedSite, onSiteSelect }: MapProps) {
  const mapRef = useRef<L.Map | null>(null)
  const markersRef = useRef<L.CircleMarker[]>([])
  const selectedMarkerRef = useRef<L.CircleMarker | null>(null)
  const containerRef = useRef<HTMLDivElement>(null)

  // Initialize map
  useEffect(() => {
    if (!containerRef.current || mapRef.current) return

    const map = L.map(containerRef.current, {
      center: [39.0, -98.5],  // Center of CONUS
      zoom: 5,
      zoomControl: false,
    })

    // Zoom control top-right
    L.control.zoom({ position: 'topright' }).addTo(map)

    // Base layers
    const dark = L.tileLayer(
      'https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png',
      { attribution: '&copy; CARTO', maxZoom: 19 }
    )
    const satellite = L.tileLayer(
      'https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}',
      { attribution: '&copy; Esri', maxZoom: 19 }
    )
    const topo = L.tileLayer(
      'https://{s}.tile.opentopomap.org/{z}/{x}/{y}.png',
      { attribution: '&copy; OpenTopoMap', maxZoom: 17 }
    )
    const light = L.tileLayer(
      'https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png',
      { attribution: '&copy; CARTO', maxZoom: 19 }
    )

    dark.addTo(map)

    // Layer control
    const baseLayers: Record<string, L.TileLayer> = {
      'Dark': dark,
      'Satellite': satellite,
      'Terrain': topo,
      'Light': light,
    }
    L.control.layers(baseLayers, {}, { position: 'topright', collapsed: true }).addTo(map)

    mapRef.current = map

    return () => {
      map.remove()
      mapRef.current = null
    }
  }, [])

  // Update markers when sites change
  useEffect(() => {
    const map = mapRef.current
    if (!map) return

    // Clear existing
    markersRef.current.forEach(m => m.remove())
    markersRef.current = []

    // Grade layers
    const gradeGroups: Record<string, L.LayerGroup> = {
      A: L.layerGroup(),
      B: L.layerGroup(),
      C: L.layerGroup(),
      D: L.layerGroup(),
      F: L.layerGroup(),
    }

    sites.forEach((site) => {
      if (!site.lat || !site.lon) return

      const color = GRADE_COLORS[site.grade] || '#666'
      const radius = site.grade === 'A' ? 7 : site.grade === 'B' ? 6 : 4

      const marker = L.circleMarker([site.lat, site.lon], {
        radius,
        fillColor: color,
        color: 'rgba(255,255,255,0.4)',
        weight: 1,
        opacity: 0.9,
        fillOpacity: 0.75,
      })

      const loc = [site.city, site.state].filter(Boolean).join(', ')
      marker.bindPopup(`
        <div style="font-family: -apple-system, sans-serif; min-width: 260px; color: #1a1a2e;">
          <div style="font-size: 15px; font-weight: 700; margin-bottom: 2px; border-bottom: 2px solid ${color}; padding-bottom: 4px;">
            #${site.rank} ${site.substation_name}
          </div>
          ${loc ? `<div style="font-size: 11px; color: #888; margin-bottom: 6px;">${loc}${site.owner ? ' &mdash; ' + site.owner : ''}</div>` : ''}
          <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 3px; font-size: 12px;">
            <span style="color: #666;">Grade</span>
            <span style="color: ${color}; font-weight: 700;">${site.grade} (${site.composite_score})</span>
            <span style="color: #666;">Voltage</span>
            <span>${site.volt_class}${site.max_volt ? ' (' + site.max_volt + ' kV)' : ''}</span>
            <span style="color: #666;">Connected Lines</span>
            <span>${site.connected_lines || site.hifld_lines || 'N/A'}</span>
            <span style="color: #666;">Env Score</span>
            <span>${site.environmental_score ?? 'N/A'} / 100</span>
            <span style="color: #666;">Flood Zone</span>
            <span>${site.flood_flood_zone || 'N/A'}</span>
            <span style="color: #666;">Solar GHI</span>
            <span>${site.ghi_annual ? Number(site.ghi_annual).toFixed(1) + ' kWh/m\u00B2/d' : 'N/A'}</span>
            <span style="color: #666;">Nearby Gen</span>
            <span>${site.nearby_generation_mw ? Math.round(site.nearby_generation_mw) + ' MW' : 'N/A'}</span>
            <span style="color: #666;">Coordinates</span>
            <span style="font-family: monospace; font-size: 10px;">${site.lat.toFixed(5)}, ${site.lon.toFixed(5)}</span>
          </div>
          <div style="margin-top: 6px; font-size: 10px; color: #999; text-align: center;">Click for full details \u2192</div>
        </div>
      `, { maxWidth: 320 })

      marker.on('click', () => onSiteSelect(site))
      marker.addTo(gradeGroups[site.grade] || gradeGroups.F)
      markersRef.current.push(marker)
    })

    // Add grade groups to map
    Object.entries(gradeGroups).forEach(([grade, group]) => {
      group.addTo(map)
    })

    // Fit bounds
    if (sites.length > 0) {
      const valid = sites.filter(s => s.lat && s.lon)
      if (valid.length > 0) {
        const bounds = L.latLngBounds(
          valid.map(s => [s.lat, s.lon] as [number, number])
        )
        map.fitBounds(bounds, { padding: [40, 40] })
      }
    }
  }, [sites, onSiteSelect])

  // Highlight selected
  useEffect(() => {
    const map = mapRef.current
    if (!map) return

    // Remove old highlight
    if (selectedMarkerRef.current) {
      selectedMarkerRef.current.remove()
      selectedMarkerRef.current = null
    }

    if (!selectedSite) return

    // Add pulsing highlight
    const highlight = L.circleMarker([selectedSite.lat, selectedSite.lon], {
      radius: 14,
      fillColor: 'transparent',
      color: '#3b82f6',
      weight: 3,
      opacity: 1,
      fillOpacity: 0,
      className: 'selected-pulse',
    })
    highlight.addTo(map)
    selectedMarkerRef.current = highlight

    map.setView([selectedSite.lat, selectedSite.lon], Math.max(map.getZoom(), 8), {
      animate: true,
    })
  }, [selectedSite])

  return (
    <div className="relative w-full h-full">
      <div ref={containerRef} className="w-full h-full rounded-lg" />

      {/* Legend */}
      <div className="absolute bottom-4 left-4 bg-gray-900/90 backdrop-blur-sm rounded-lg px-3 py-2 z-[1000] text-xs">
        <div className="text-gray-400 font-semibold mb-1.5 text-[10px] uppercase tracking-wider">
          Site Grade
        </div>
        {Object.entries(GRADE_COLORS).map(([grade, color]) => (
          <div key={grade} className="flex items-center gap-2 py-0.5">
            <div
              className="w-3 h-3 rounded-full"
              style={{ backgroundColor: color }}
            />
            <span className="text-gray-300">Grade {grade}</span>
          </div>
        ))}
      </div>
    </div>
  )
}
