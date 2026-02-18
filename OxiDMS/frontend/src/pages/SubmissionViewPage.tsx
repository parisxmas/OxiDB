import { useEffect, useState } from 'react'
import { useParams, useNavigate } from 'react-router-dom'
import { ArrowLeft, Pencil } from 'lucide-react'
import toast from 'react-hot-toast'
import { api, downloadFile } from '../api/client'
import type { Form, Submission, Document as DocType } from '../api/types'
import { Spinner } from '../components/ui/Spinner'
import { Button } from '../components/ui/Button'
import { Badge } from '../components/ui/Badge'
import { FileText, Download } from 'lucide-react'

export function SubmissionViewPage() {
  const { formId, submissionId } = useParams<{ formId: string; submissionId: string }>()
  const navigate = useNavigate()
  const [form, setForm] = useState<Form | null>(null)
  const [submission, setSubmission] = useState<Submission | null>(null)
  const [documents, setDocuments] = useState<DocType[]>([])
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    Promise.all([
      api.get<Form>(`/forms/${formId}`),
      api.get<{ submission: Submission; documents: DocType[] }>(`/forms/${formId}/submissions/${submissionId}`),
    ])
      .then(([f, res]) => {
        setForm(f)
        setSubmission(res.submission)
        setDocuments(res.documents || [])
      })
      .catch(() => {
        toast.error('Not found')
        navigate(`/forms/${formId}`)
      })
      .finally(() => setLoading(false))
  }, [formId, submissionId, navigate])

  if (loading) return <div className="flex justify-center py-20"><Spinner className="h-8 w-8 text-blue-600" /></div>
  if (!form || !submission) return null

  return (
    <div className="space-y-6">
      <div className="flex items-center gap-4">
        <button
          onClick={() => navigate(`/forms/${form._id}`)}
          className="flex items-center gap-1 text-sm text-gray-500 hover:text-gray-700"
        >
          <ArrowLeft size={16} /> Back to {form.name}
        </button>
      </div>

      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-xl font-bold text-gray-900">Submission Details</h1>
          <p className="text-sm text-gray-400 mt-1">
            Submitted {new Date(submission.createdAt).toLocaleString()}
          </p>
        </div>
        <Button onClick={() => navigate(`/forms/${form._id}/submissions/${submission._id}/edit`)}>
          <Pencil size={16} className="mr-1" /> Edit Submission
        </Button>
      </div>

      <div className="rounded-xl border border-gray-200 bg-white p-6">
        <div className="grid gap-4 sm:grid-cols-2">
          {form.fields.map((field: any) => {
            if (field.type === 'file') return null
            const value = submission.data[field.name]
            return (
              <div key={field.name}>
                <label className="block text-sm font-medium text-gray-500">{field.label}</label>
                <p className="mt-1 text-sm text-gray-900">
                  {value === true ? 'Yes' : value === false ? 'No' : value != null ? String(value) : '-'}
                </p>
              </div>
            )
          })}
        </div>
      </div>

      {documents.length > 0 && (
        <div className="rounded-xl border border-gray-200 bg-white p-6">
          <h3 className="text-sm font-semibold text-gray-500 uppercase tracking-wider mb-3">Attached Documents</h3>
          <div className="space-y-2">
            {documents.map((doc) => (
              <div key={doc._id} className="flex items-center justify-between rounded-lg border border-gray-200 px-4 py-2">
                <div className="flex items-center gap-2">
                  <FileText size={16} className="text-gray-400" />
                  <span className="text-sm text-gray-700">{doc.fileName}</span>
                  <Badge color="gray">{formatSize(doc.size)}</Badge>
                </div>
                <button
                  onClick={() => downloadFile(`/documents/${doc._id}/download`, doc.fileName)}
                  className="text-blue-600 hover:text-blue-500"
                >
                  <Download size={16} />
                </button>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  )
}

function formatSize(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`
}
