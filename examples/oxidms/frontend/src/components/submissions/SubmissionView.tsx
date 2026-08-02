import { useEffect, useState } from 'react'
import type { Submission, Form, Document as DocType } from '../../api/types'
import { api, downloadFile } from '../../api/client'
import { Badge } from '../ui/Badge'
import { FileText, Download } from 'lucide-react'

interface SubmissionViewProps {
  submission: Submission
  form: Form
}

export function SubmissionView({ submission, form }: SubmissionViewProps) {
  const [documents, setDocuments] = useState<DocType[]>([])

  useEffect(() => {
    api.get<{ submission: Submission; documents: DocType[] }>(`/forms/${form._id}/submissions/${submission._id}`)
      .then((res) => setDocuments(res.documents || []))
  }, [submission._id, form._id])

  return (
    <div className="space-y-6">
      <div className="grid gap-4">
        {form.fields.map((field) => {
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

      {documents.length > 0 && (
        <div>
          <h4 className="text-sm font-medium text-gray-500 mb-2">Attached Documents</h4>
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

      <div className="text-xs text-gray-400">
        Submitted {new Date(submission.createdAt).toLocaleString()}
      </div>
    </div>
  )
}

function formatSize(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`
}
