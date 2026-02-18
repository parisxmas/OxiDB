import { useEffect, useState } from 'react'
import { useParams, useNavigate } from 'react-router-dom'
import { ArrowLeft, Save } from 'lucide-react'
import toast from 'react-hot-toast'
import { api } from '../api/client'
import type { Form, Submission, FieldDefinition, Document as DocType } from '../api/types'
import { Spinner } from '../components/ui/Spinner'
import { SubmissionForm } from '../components/submissions/SubmissionForm'

export function EditSubmissionPage() {
  const { formId, submissionId } = useParams<{ formId: string; submissionId: string }>()
  const navigate = useNavigate()
  const [form, setForm] = useState<Form | null>(null)
  const [submission, setSubmission] = useState<Submission | null>(null)
  const [loading, setLoading] = useState(true)
  const [saving, setSaving] = useState(false)

  useEffect(() => {
    Promise.all([
      api.get<Form>(`/forms/${formId}`),
      api.get<{ submission: Submission; documents: DocType[] }>(`/forms/${formId}/submissions/${submissionId}`),
    ])
      .then(([f, res]) => {
        setForm(f)
        setSubmission(res.submission)
      })
      .catch(() => {
        toast.error('Not found')
        navigate(`/forms/${formId}`)
      })
      .finally(() => setLoading(false))
  }, [formId, submissionId, navigate])

  if (loading) return <div className="flex justify-center py-20"><Spinner className="h-8 w-8 text-blue-600" /></div>
  if (!form || !submission) return null

  const handleUpdate = async (data: Record<string, unknown>, _files: File[]) => {
    setSaving(true)
    try {
      await api.put(`/forms/${formId}/submissions/${submissionId}`, { data })
      toast.success('Submission updated')
      navigate(`/forms/${formId}/submissions/${submissionId}`)
    } catch (err: any) {
      toast.error(err.message)
    } finally {
      setSaving(false)
    }
  }

  return (
    <div className="space-y-4">
      <div className="flex items-center gap-4">
        <button
          onClick={() => navigate(`/forms/${formId}/submissions/${submissionId}`)}
          className="flex items-center gap-1 text-sm text-gray-500 hover:text-gray-700"
        >
          <ArrowLeft size={16} /> Back to Submission
        </button>
      </div>

      <h1 className="text-xl font-bold text-gray-900">Edit Submission — {form.name}</h1>

      <SubmissionForm
        fields={form.fields as FieldDefinition[]}
        onSubmit={handleUpdate}
        loading={saving}
        defaultValues={submission.data}
      />
    </div>
  )
}
