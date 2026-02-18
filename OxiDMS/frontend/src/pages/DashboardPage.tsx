import { useEffect, useState } from 'react'
import { Link } from 'react-router-dom'
import { FileText, FolderOpen, Inbox } from 'lucide-react'
import { api } from '../api/client'
import type { DashboardData } from '../api/types'
import { Spinner } from '../components/ui/Spinner'

export function DashboardPage() {
  const [data, setData] = useState<DashboardData | null>(null)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    api.get<DashboardData>('/dashboard').then(setData).finally(() => setLoading(false))
  }, [])

  if (loading) return <div className="flex justify-center py-20"><Spinner className="h-8 w-8 text-blue-600" /></div>
  if (!data) return null

  return (
    <div>
      <h1 className="text-2xl font-bold text-gray-900">Dashboard</h1>
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

function StatCard({ icon: Icon, label, value, color }: { icon: any; label: string; value: number; color: string }) {
  const colors: Record<string, string> = {
    blue: 'bg-blue-50 text-blue-600',
    green: 'bg-green-50 text-green-600',
    purple: 'bg-purple-50 text-purple-600',
  }
  return (
    <div className="rounded-xl border border-gray-200 bg-white p-6 shadow-sm">
      <div className="flex items-center gap-4">
        <div className={`rounded-lg p-3 ${colors[color]}`}>
          <Icon size={24} />
        </div>
        <div>
          <p className="text-2xl font-bold text-gray-900">{value}</p>
          <p className="text-sm text-gray-500">{label}</p>
        </div>
      </div>
    </div>
  )
}
