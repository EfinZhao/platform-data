import { useCallback, useEffect, useState } from 'react'

type SensorStatus = {
  name: string | null
  status: number | null
  frame_status: number | null
  phase_status: number | null
  ttc_status: number | null
  last_online: string | null
  last_checked: string | null
}

type SensorsResponse = Record<string, SensorStatus>

type SortKey = 'udid' | 'name' | 'status' | 'last_online' | 'last_checked'
type SortDir = 'asc' | 'desc'

const COLUMN_LABELS: Record<SortKey, string> = {
  udid: 'UDID',
  name: 'Intersection',
  status: 'Status',
  last_online: 'Last Online',
  last_checked: 'Last Checked',
}

const RETENTION_OPTIONS = [
  { label: '6 hours',  entries: 24  },
  { label: '24 hours', entries: 96  },
  { label: '2 days',   entries: 192 },
  { label: '5 days',   entries: 480 },
  { label: '1 week',   entries: 672 },
]

const EMPTY_SENSOR: SensorStatus = {
  name: null, status: null, frame_status: null,
  phase_status: null, ttc_status: null, last_online: null, last_checked: null,
}

const fmt = (iso: string | null): string => {
  if (!iso) return '—'
  return new Date(iso).toLocaleString()
}

const StatusBadge = ({ status }: { status: number | null }) => {
  if (status === 1)
    return (
      <span className="inline-flex items-center gap-1.5 rounded-full bg-green-100 px-2.5 py-0.5 text-xs font-medium text-green-800">
        <span className="h-1.5 w-1.5 rounded-full bg-green-500" />
        Online
      </span>
    )
  if (status === 0)
    return (
      <span className="inline-flex items-center gap-1.5 rounded-full bg-red-100 px-2.5 py-0.5 text-xs font-medium text-red-800">
        <span className="h-1.5 w-1.5 rounded-full bg-red-500" />
        Offline
      </span>
    )
  return (
    <span className="inline-flex items-center gap-1.5 rounded-full bg-gray-100 px-2.5 py-0.5 text-xs font-medium text-gray-600">
      <span className="h-1.5 w-1.5 rounded-full bg-gray-400" />
      Unknown
    </span>
  )
}

const SortIcon = ({ active, dir }: { active: boolean; dir: SortDir }) => (
  <span className={`ml-1 ${active ? 'text-blue-500' : 'text-gray-300'}`}>
    {!active || dir === 'asc' ? '↑' : '↓'}
  </span>
)

const ApiStatusRow = ({ label, available }: { label: string; available: number | null }) => (
  <div className="flex items-center justify-between border-b border-gray-100 py-3 last:border-0">
    <span className="text-sm text-gray-700">{label}</span>
    {available === 1 ? (
      <span className="inline-flex items-center gap-1 rounded-full bg-green-100 px-2.5 py-0.5 text-xs font-medium text-green-800">
        ✓ Available
      </span>
    ) : available === 0 ? (
      <span className="inline-flex items-center gap-1 rounded-full bg-red-100 px-2.5 py-0.5 text-xs font-medium text-red-800">
        ✗ Unavailable
      </span>
    ) : (
      <span className="text-xs text-gray-400">—</span>
    )}
  </div>
)

const sortEntries = (
  entries: [string, SensorStatus][],
  key: SortKey,
  dir: SortDir
): [string, SensorStatus][] => {
  const mul = dir === 'asc' ? 1 : -1

  return [...entries].sort(([aUdid, aS], [bUdid, bS]) => {
    if (key === 'udid') return mul * aUdid.localeCompare(bUdid)
    if (key === 'status') {
      const a = aS.status ?? -1
      const b = bS.status ?? -1
      return mul * (a - b)
    }
    const a = (aS[key] ?? '').toString()
    const b = (bS[key] ?? '').toString()
    return mul * a.localeCompare(b)
  })
}

const App = () => {
  const [sensors, setSensors] = useState<SensorsResponse>({})
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [fetchedAt, setFetchedAt] = useState<Date | null>(null)
  const [sortKey, setSortKey] = useState<SortKey>('udid')
  const [sortDir, setSortDir] = useState<SortDir>('asc')
  const [selectedUdid, setSelectedUdid] = useState<string | null>(null)
  const [retention, setRetention] = useState<number>(96)

  const handleSort = (key: SortKey) => {
    if (key === sortKey) {
      setSortDir(d => (d === 'asc' ? 'desc' : 'asc'))
    } else {
      setSortKey(key)
      setSortDir('asc')
    }
  }

  const loadSensors = useCallback(async () => {
    setError(null)
    try {
      const res = await fetch('/api/status/all')
      if (!res.ok) throw new Error(`Server returned ${res.status}`)
      const data: SensorsResponse = await res.json()
      setSensors(data)
      setFetchedAt(new Date())
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Unknown error')
    } finally {
      setLoading(false)
    }
  }, [])

  const refresh = useCallback(async () => {
    setError(null)
    setLoading(true)
    setSensors(prev =>
      Object.fromEntries(Object.keys(prev).map(udid => [udid, EMPTY_SENSOR]))
    )
    try {
      const res = await fetch('/api/update/all', { method: 'POST' })
      if (!res.ok) throw new Error(`Server returned ${res.status}`)
      const data: SensorsResponse = await res.json()
      setSensors(data)
      setFetchedAt(new Date())
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Unknown error')
    } finally {
      setLoading(false)
    }
  }, [])

  const changeRetention = useCallback(async (n: number) => {
    const prev = retention
    setRetention(n)
    try {
      const res = await fetch('/api/settings/retention', {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ entries: n }),
      })
      if (!res.ok) setRetention(prev)
    } catch {
      setRetention(prev)
    }
  }, [retention])

  useEffect(() => {
    loadSensors()
    fetch('/api/settings/retention')
      .then(r => r.ok ? r.json() : null)
      .then(data => { if (data?.entries) setRetention(data.entries) })
      .catch(() => {})
  }, [loadSensors])

  const entries = Object.entries(sensors)
  const sorted = sortEntries(entries, sortKey, sortDir)
  const online = entries.filter(([, s]) => s.status === 1).length
  const offline = entries.filter(([, s]) => s.status === 0).length
  const unknown = entries.filter(([, s]) => s.status === null).length

  const selectedSensor = selectedUdid ? (sensors[selectedUdid] ?? null) : null

  const thClass = 'px-4 py-3 cursor-pointer select-none hover:text-gray-700'

  return (
    <div className="min-h-screen bg-gray-50 p-6">
      <div className="mx-auto max-w-7xl">

        {/* Header */}
        <div className="mb-6 flex items-center justify-between">
          <div>
            <h1 className="text-2xl font-bold text-gray-900">BCT Sensor Dashboard</h1>
            {fetchedAt && (
              <p className="mt-0.5 text-sm text-gray-500">
                Fetched at {fetchedAt.toLocaleTimeString()}
              </p>
            )}
          </div>
          <div className="flex items-center gap-3">
            <div className="flex items-center gap-2">
              <label htmlFor="retention-select" className="text-sm text-gray-500 whitespace-nowrap">
                History window
              </label>
              <select
                id="retention-select"
                value={retention}
                onChange={e => changeRetention(Number(e.target.value))}
                className="rounded-md border border-gray-200 bg-white px-3 py-2 text-sm text-gray-700 focus:border-blue-500 focus:outline-none"
              >
                {RETENTION_OPTIONS.map(opt => (
                  <option key={opt.entries} value={opt.entries}>{opt.label}</option>
                ))}
              </select>
            </div>
            <button
              onClick={refresh}
              disabled={loading}
              className="rounded-md bg-blue-600 px-4 py-2 text-sm font-medium text-white hover:bg-blue-700 disabled:opacity-50"
            >
              {loading ? 'Refreshing...' : 'Refresh'}
            </button>
          </div>
        </div>

        {/* Summary */}
        <div className="mb-6 grid grid-cols-4 gap-4">
          {[
            { label: 'Total', value: entries.length, color: 'text-gray-900' },
            { label: 'Online', value: online, color: 'text-green-600' },
            { label: 'Offline', value: offline, color: 'text-red-600' },
            { label: 'Unknown', value: unknown, color: 'text-gray-400' },
          ].map(({ label, value, color }) => (
            <div key={label} className="rounded-lg bg-white p-4 shadow-sm ring-1 ring-gray-200">
              <p className="text-sm text-gray-500">{label}</p>
              <p className={`mt-1 text-3xl font-semibold ${color}`}>{value}</p>
            </div>
          ))}
        </div>

        {/* Error */}
        {error && (
          <div className="mb-6 rounded-md bg-red-50 p-4 text-sm text-red-700 ring-1 ring-red-200">
            Failed to load sensors: {error}
          </div>
        )}

        {/* Table + detail panel */}
        <div className="grid grid-cols-4 gap-4">

          {/* Table */}
          <div className="col-span-3 overflow-hidden rounded-lg bg-white shadow-sm ring-1 ring-gray-200">
            <table className="w-full text-sm">
              <thead className="bg-gray-50 text-left text-xs font-medium uppercase tracking-wide text-gray-500">
                <tr>
                  {(['udid', 'name', 'status', 'last_online', 'last_checked'] as SortKey[]).map(key => (
                    <th key={key} className={thClass} onClick={() => handleSort(key)}>
                      {COLUMN_LABELS[key]}
                      <SortIcon active={sortKey === key} dir={sortDir} />
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-100">
                {loading && entries.length === 0 ? (
                  <tr>
                    <td colSpan={5} className="px-4 py-8 text-center text-gray-400">
                      Loading...
                    </td>
                  </tr>
                ) : sorted.length === 0 ? (
                  <tr>
                    <td colSpan={5} className="px-4 py-8 text-center text-gray-400">
                      No sensors found.
                    </td>
                  </tr>
                ) : (
                  sorted.map(([udid, sensor]) => (
                    <tr
                      key={udid}
                      onClick={() => setSelectedUdid(prev => prev === udid ? null : udid)}
                      className={`cursor-pointer transition-colors hover:bg-blue-50 ${
                        selectedUdid === udid ? 'bg-blue-50' : ''
                      }`}
                    >
                      <td className="px-4 py-3 font-mono text-gray-800">{udid}</td>
                      <td className="px-4 py-3 text-gray-800">{sensor.name ?? '—'}</td>
                      <td className="px-4 py-3">
                        <StatusBadge status={sensor.status} />
                      </td>
                      <td className="px-4 py-3 text-gray-600">{fmt(sensor.last_online)}</td>
                      <td className="px-4 py-3 text-gray-600">{fmt(sensor.last_checked)}</td>
                    </tr>
                  ))
                )}
              </tbody>
            </table>
          </div>

          {/* Detail panel */}
          <div className="col-span-1 rounded-lg bg-white shadow-sm ring-1 ring-gray-200">
            {selectedUdid && selectedSensor ? (
              <>
                {/* Panel header */}
                <div className="flex items-start justify-between border-b border-gray-100 p-4">
                  <div className="min-w-0">
                    <p className="truncate font-semibold text-gray-900">
                      {selectedUdid ?? '—'}
                    </p>
                    <p className="mt-0.5 truncate font-mono text-xs text-gray-500">{selectedSensor.name}</p>
                  </div>
                  <button
                    onClick={() => setSelectedUdid(null)}
                    className="ml-2 shrink-0 rounded p-1 text-gray-400 hover:bg-gray-100 hover:text-gray-600"
                  >
                    ✕
                  </button>
                </div>

                {/* Connection status */}
                <div className="border-b border-gray-100 p-4">
                  <p className="mb-2 text-xs font-medium uppercase tracking-wide text-gray-400">
                    Connection
                  </p>
                  <StatusBadge status={selectedSensor.status} />
                  <div className="mt-3 space-y-1.5 text-xs text-gray-500">
                    <div className="flex justify-between gap-2">
                      <span className="shrink-0">Last online</span>
                      <span className="text-right text-gray-700">{fmt(selectedSensor.last_online)}</span>
                    </div>
                    <div className="flex justify-between gap-2">
                      <span className="shrink-0">Last checked</span>
                      <span className="text-right text-gray-700">{fmt(selectedSensor.last_checked)}</span>
                    </div>
                  </div>
                </div>

                {/* API data availability */}
                <div className="p-4">
                  <p className="mb-1 text-xs font-medium uppercase tracking-wide text-gray-400">
                    API Data
                  </p>
                  <ApiStatusRow label="Frame" available={selectedSensor.frame_status} />
                  <ApiStatusRow label="Phase Change" available={selectedSensor.phase_status} />
                  <ApiStatusRow label="Time to Change" available={selectedSensor.ttc_status} />
                </div>
              </>
            ) : (
              <div className="flex h-full items-center justify-center p-4">
                <p className="text-center text-xs font-medium uppercase tracking-wide text-gray-400">
                  Click a sensor<br />to see more
                </p>
              </div>
            )}
          </div>

        </div>
      </div>
    </div>
  )
}

export default App
