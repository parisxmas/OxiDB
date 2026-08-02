import { useEffect, useState } from 'react'
import { Link, useNavigate } from 'react-router-dom'
import { Plus, FileText, Trash2, Eye } from 'lucide-react'
import toast from 'react-hot-toast'
import { api } from '../api/client'
import type { Form } from '../api/types'
import { Button } from '../components/ui/Button'
import { Spinner } from '../components/ui/Spinner'

export function FormsPage() {
  const navigate = useNavigate()
  const [forms, setForms] = useState<Form[]>([])
  const [loading, setLoading] = useState(true)

  const loadForms = () => {
    api.get<Form[]>('/forms').then(setForms).finally(() => setLoading(false))
  }

  useEffect(loadForms, [])

  if (loading) return <div className="flex justify-center py-20"><Spinner className="h-8 w-8 text-blue-600" /></div>

  return (
    <div>
      <div className="flex items-center justify-between">
        <h1 className="text-2xl font-bold text-gray-900">Forms</h1>
        <Button onClick={() => navigate('/forms/new')}>
          <Plus size={16} className="mr-2" /> New Form
        </Button>
      </div>

      {forms.length === 0 ? (
        <div className="mt-8 rounded-lg border-2 border-dashed border-gray-300 p-12 text-center">
          <FileText className="mx-auto h-12 w-12 text-gray-400" />
          <p className="mt-4 text-gray-500">No forms yet. Create your first form to get started.</p>
        </div>
      ) : (
        <div className="mt-6 grid gap-4 sm:grid-cols-2 lg:grid-cols-3">
          {forms.map((form) => (
            <div key={form._id} className="rounded-xl border border-gray-200 bg-white p-6 shadow-sm hover:shadow-md transition-shadow">
              <h3 className="font-semibold text-gray-900">{form.name}</h3>
              {form.description && <p className="mt-1 text-sm text-gray-500 line-clamp-2">{form.description}</p>}
              <p className="mt-3 text-xs text-gray-400">{form.fields.length} fields</p>
              <div className="mt-4 flex gap-2">
                <Link to={`/forms/${form._id}`}>
                  <Button variant="secondary" size="sm"><Eye size={14} className="mr-1" /> View</Button>
                </Link>
                <Button variant="ghost" size="sm" onClick={async () => {
                  try {
                    await api.delete(`/forms/${form._id}`)
                    setForms(forms.filter((f) => f._id !== form._id))
                    toast.success('Form deleted')
                  } catch (err: any) {
                    toast.error(err.message)
                  }
                }}>
                  <Trash2 size={14} className="text-red-500" />
                </Button>
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  )
}
