import { useEffect, useState } from 'react'
import { useParams, useNavigate } from 'react-router-dom'
import { ArrowLeft } from 'lucide-react'
import toast from 'react-hot-toast'
import { api } from '../api/client'
import type { Form, FieldDefinition } from '../api/types'
import { Spinner } from '../components/ui/Spinner'
import { SubmissionForm } from '../components/submissions/SubmissionForm'

export function NewSubmissionPage() {
  const { formId } = useParams<{ formId: string }>()
  const navigate = useNavigate()
  const [form, setForm] = useState<Form | null>(null)
  const [loading, setLoading] = useState(true)
  const [submitting, setSubmitting] = useState(false)

  useEffect(() => {
    api.get<Form>(`/forms/${formId}`).then(setForm)
      .catch(() => {
        toast.error('Form not found')
        navigate('/forms')
      })
      .finally(() => setLoading(false))
  }, [formId, navigate])

  if (loading) return <div className="flex justify-center py-20"><Spinner className="h-8 w-8 text-blue-600" /></div>
  if (!form) return null

  const handleSubmission = async (data: Record<string, unknown>, files: File[]) => {
    setSubmitting(true)
    try {
      if (files.length > 0) {
        const formData = new FormData()
        formData.append('data', JSON.stringify(data))
        files.forEach((f) => formData.append('files', f))
        await api.upload(`/forms/${form._id}/submissions`, formData)
      } else {
        await api.post(`/forms/${form._id}/submissions`, { data })
      }
      toast.success('Submission created')
      navigate(`/forms/${form._id}`)
    } catch (err: any) {
      toast.error(err.message)
    } finally {
      setSubmitting(false)
    }
  }

  return (
    <div className="space-y-4">
      <div className="flex items-center gap-4">
        <button
          onClick={() => navigate(`/forms/${form._id}`)}
          className="flex items-center gap-1 text-sm text-gray-500 hover:text-gray-700"
        >
          <ArrowLeft size={16} /> Back to {form.name}
        </button>
      </div>

      <h1 className="text-xl font-bold text-gray-900">New Submission — {form.name}</h1>
      {form.description && <p className="text-sm text-gray-500">{form.description}</p>}

      <SubmissionForm
        fields={form.fields as FieldDefinition[]}
        onSubmit={handleSubmission}
        loading={submitting}
      />
    </div>
  )
}
