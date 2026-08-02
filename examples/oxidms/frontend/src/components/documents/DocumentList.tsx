import type { Document as DocType } from '../../api/types'
import { FileText, Download, Trash2, Image, File } from 'lucide-react'
import { api, downloadFile } from '../../api/client'
import { Button } from '../ui/Button'
import { Badge } from '../ui/Badge'
import toast from 'react-hot-toast'

interface DocumentListProps {
  documents: DocType[]
  onDelete: (id: string) => void
}

function getIcon(contentType: string) {
  if (contentType.startsWith('image/')) return Image
  if (contentType === 'application/pdf') return FileText
  return File
}

function formatSize(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`
}

export function DocumentList({ documents, onDelete }: DocumentListProps) {
  if (documents.length === 0) {
    return <p className="py-8 text-center text-gray-400">No documents uploaded yet</p>
  }

  return (
    <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-3">
      {documents.map((doc) => {
        const Icon = getIcon(doc.contentType)
        return (
          <div key={doc._id} className="rounded-lg border border-gray-200 bg-white p-4 shadow-sm">
            <div className="flex items-start justify-between">
              <div className="flex items-center gap-3">
                <div className="rounded-lg bg-gray-100 p-2">
                  <Icon size={20} className="text-gray-500" />
                </div>
                <div className="min-w-0">
                  <p className="truncate text-sm font-medium text-gray-900">{doc.fileName}</p>
                  <div className="flex gap-2 mt-1">
                    <Badge color="gray">{formatSize(doc.size)}</Badge>
                  </div>
                </div>
              </div>
            </div>
            <div className="mt-3 flex gap-2">
              <button
                onClick={() => downloadFile(`/documents/${doc._id}/download`, doc.fileName)}
                className="inline-flex items-center gap-1 text-xs font-medium text-blue-600 hover:text-blue-500"
              >
                <Download size={12} /> Download
              </button>
              <button
                onClick={async () => {
                  try {
                    await api.delete(`/documents/${doc._id}`)
                    onDelete(doc._id)
                    toast.success('Document deleted')
                  } catch (err: any) {
                    toast.error(err.message)
                  }
                }}
                className="inline-flex items-center gap-1 text-xs font-medium text-red-500 hover:text-red-600"
              >
                <Trash2 size={12} /> Delete
              </button>
            </div>
            <p className="mt-2 text-xs text-gray-400">{new Date(doc.createdAt).toLocaleDateString()}</p>
          </div>
        )
      })}
    </div>
  )
}
