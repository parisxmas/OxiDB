import { useCallback, useEffect, useRef, useState } from 'react'
import { Link } from 'react-router-dom'
import { FileText, FolderOpen, Inbox } from 'lucide-react'
import { api } from '../api/client'
import type { DashboardData } from '../api/types'
import { Spinner } from '../components/ui/Spinner'

const POLL_INTERVAL = 30_000

export function DashboardPage() {
  const [data, setData] = useState<DashboardData | null>(null)
  const [loading, setLoading] = useState(true)
  const intervalRef = useRef<ReturnType<typeof setInterval> | null>(null)

  const fetchData = useCallback((silent = false) => {
    if (!silent) setLoading(true)
    api.get<DashboardData>('/dashboard').then(setData).finally(() => {
      if (!silent) setLoading(false)
    })
  }, [])

  useEffect(() => {
    fetchData()
    intervalRef.current = setInterval(() => fetchData(true), POLL_INTERVAL)
    return () => {
      if (intervalRef.current) clearInterval(intervalRef.current)
    }
  }, [fetchData])

  if (loading) return <div className="flex justify-center py-20"><Spinner className="h-8 w-8 text-blue-600" /></div>
  if (!data) return null

  return (
    <div>
      <div className="flex items-center justify-between">
        <h1 className="text-2xl font-bold text-gray-900">Dashboard</h1>
        <div className="flex items-center gap-2 text-xs text-gray-500">
          <span className="relative flex h-2 w-2">
            <span className="absolute inline-flex h-full w-full animate-ping rounded-full bg-green-400 opacity-75" />
            <span className="relative inline-flex h-2 w-2 rounded-full bg-green-500" />
          </span>
          Live
        </div>
      </div>
      <div className="mt-6 grid gap-6 sm:grid-cols-3">
        <StatCard icon={FileText} label="Forms" value={data.formCount} color="blue" />
        <StatCard icon={Inbox} label="Submissions" value={data.submissionCount} color="green" />
        <StatCard icon={FolderOpen} label="Documents" value={data.documentCount} color="purple" />
      </div>
      <div className="mt-8">
        <h2 className="text-lg font-semibold text-gray-900">Forms Overview</h2>
        {data.forms.length === 0 ? (
          <div className="mt-4 rounded-lg border-2 border-dashed border-gray-300 p-12 text-center">
            <FileText className="mx-auto h-12 w-12 text-gray-400" />
            <p className="mt-4 text-gray-500">No forms yet. Create your first form to get started.</p>
            <Link to="/forms" className="mt-4 inline-block text-sm font-medium text-blue-600 hover:text-blue-500">
              Create Form
            </Link>
          </div>
        ) : (
          <div className="mt-4 overflow-hidden rounded-lg border border-gray-200 bg-white shadow-sm">
            <table className="min-w-full divide-y divide-gray-200">
              <thead className="bg-gray-50">
                <tr>
                  <th className="px-6 py-3 text-left text-xs font-medium uppercase tracking-wider text-gray-500">Name</th>
                  <th className="px-6 py-3 text-left text-xs font-medium uppercase tracking-wider text-gray-500">Fields</th>
                  <th className="px-6 py-3 text-left text-xs font-medium uppercase tracking-wider text-gray-500">Submissions</th>
                  <th className="px-6 py-3 text-left text-xs font-medium uppercase tracking-wider text-gray-500">Created</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-200">
                {data.forms.map((f) => (
                  <tr key={f.id} className="hover:bg-gray-50">
                    <td className="px-6 py-4">
                      <Link to={`/forms/${f.id}`} className="font-medium text-blue-600 hover:text-blue-500">{f.name}</Link>
                    </td>
                    <td className="px-6 py-4 text-sm text-gray-500">{f.fieldCount}</td>
                    <td className="px-6 py-4 text-sm text-gray-500">{f.submissionCount}</td>
                    <td className="px-6 py-4 text-sm text-gray-500">{new Date(f.createdAt).toLocaleDateString()}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </div>
  )
}

const fmt = new Intl.NumberFormat()

const cardStyles: Record<string, { bar: string; iconBg: string; iconText: string }> = {
  blue:   { bar: 'bg-blue-500',   iconBg: 'bg-blue-50',   iconText: 'text-blue-300' },
  green:  { bar: 'bg-green-500',  iconBg: 'bg-green-50',  iconText: 'text-green-300' },
  purple: { bar: 'bg-purple-500', iconBg: 'bg-purple-50', iconText: 'text-purple-300' },
}

function StatCard({ icon: Icon, label, value, color }: { icon: any; label: string; value: number; color: string }) {
  const style = cardStyles[color] ?? cardStyles.blue
  return (
    <div className="relative overflow-hidden rounded-xl border border-gray-200 bg-white shadow-sm">
      <div className={`absolute inset-y-0 left-0 w-1 ${style.bar}`} />
      <div className="flex items-center justify-between p-6 pl-5">
        <div>
          <p className="text-sm font-medium text-gray-500">{label}</p>
          <p className="mt-1 text-3xl font-bold text-gray-900">{fmt.format(value)}</p>
        </div>
        <div className={`rounded-xl p-3 ${style.iconBg}`}>
          <Icon size={28} className={style.iconText} />
        </div>
      </div>
    </div>
  )
}
