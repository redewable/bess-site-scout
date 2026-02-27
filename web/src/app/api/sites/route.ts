import { NextResponse } from 'next/server'
import fs from 'fs'
import path from 'path'

/**
 * API Route: GET /api/sites
 *
 * Reads the latest GeoJSON output from the pipeline and returns it.
 * Now includes enriched data: owner, operator, state, scoring breakdown,
 * environmental screening results, flood zones, etc.
 */
export async function GET() {
  const outputDir = path.resolve(process.cwd(), '..', 'output')

  try {
    const files = fs.readdirSync(outputDir)
      .filter((f: string) => f.endsWith('.geojson'))
      .sort()
      .reverse()

    if (files.length === 0) {
      return NextResponse.json(
        { error: 'No pipeline output found. Run the pipeline first.' },
        { status: 404 }
      )
    }

    const latestFile = path.join(outputDir, files[0])
    const raw = fs.readFileSync(latestFile, 'utf-8')
    const geojson = JSON.parse(raw)

    const features = geojson.features || []
    const grades: Record<string, number> = { A: 0, B: 0, C: 0, D: 0, F: 0 }
    let totalScore = 0
    let totalGhi = 0
    let maxScore = 0

    const stateDistribution: Record<string, number> = {}
    const voltDistribution: Record<string, number> = {}

    for (const f of features) {
      const p = f.properties || {}
      const grade = p.grade || 'F'
      grades[grade] = (grades[grade] || 0) + 1
      totalScore += p.composite_score || 0
      totalGhi += p.ghi_annual || 0
      if (p.composite_score > maxScore) maxScore = p.composite_score

      // State distribution (from HIFLD enrichment)
      const st = p.state || 'Unknown'
      stateDistribution[st] = (stateDistribution[st] || 0) + 1

      // Voltage distribution
      const vc = p.volt_class || 'Unknown'
      voltDistribution[vc] = (voltDistribution[vc] || 0) + 1
    }

    return NextResponse.json({
      geojson,
      meta: {
        total_sites: features.length,
        grades,
        avg_score: features.length > 0
          ? Math.round((totalScore / features.length) * 10) / 10
          : 0,
        max_score: maxScore,
        avg_ghi: features.length > 0
          ? Math.round((totalGhi / features.length) * 100) / 100
          : 0,
        voltage_distribution: voltDistribution,
        state_distribution: stateDistribution,
        generated: files[0].match(/(\d{8}_\d{6})/)?.[1] || 'unknown',
        filename: files[0],
      },
    })
  } catch (err: any) {
    return NextResponse.json(
      { error: `Failed to load sites: ${err.message}` },
      { status: 500 }
    )
  }
}
