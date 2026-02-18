import { useEffect, useState } from 'react'
import { api } from '../api/client'
import type { Document as DocType, PaginatedDocuments } from '../api/types'
import { Spinner } from '../components/ui/Spinner'
import { Button } from '../components/ui/Button'
import { Modal } from '../components/ui/Modal'
import { DocumentUpload } from '../components/documents/DocumentUpload'
import { DocumentList } from '../components/documents/DocumentList'
import { DocumentViewer } from '../components/documents/DocumentViewer'
import { ChevronLeft, ChevronRight } from 'lucide-react'

export function DocumentsPage() {
  const [docs, setDocs] = useState<DocType[]>([])
  const [total, setTotal] = useState(0)
  const [page, setPage] = useState(0)
  const [loading, setLoading] = useState(true)
  const [preview, setPreview] = useState<DocType | null>(null)
  const limit = 20

  const load = () => {
    api.get<PaginatedDocuments>(`/documents?skip=${page * limit}&limit=${limit}`)
      .then((res) => { setDocs(res.docs || []); setTotal(res.total) })
      .finally(() => setLoading(false))
  }

  useEffect(load, [page])

  if (loading) return <div className="flex justify-center py-20"><Spinner className="h-8 w-8 text-blue-600" /></div>

  const totalPages = Math.ceil(total / limit)

  return (
    <div>
      <h1 className="text-2xl font-bold text-gray-900">Documents</h1>

      <div className="mt-6">
        <DocumentUpload onUploaded={(doc) => { setDocs([doc, ...docs]); setTotal(total + 1) }} />
      </div>

      <div className="mt-8">
        <DocumentList
          documents={docs}
          onDelete={(id) => { setDocs(docs.filter((d) => d._id !== id)); setTotal(total - 1) }}
        />
      </div>

      {totalPages > 1 && (
        <div className="flex items-center justify-between pt-4">
          <span className="text-sm text-gray-500">{total} documents</span>
          <div className="flex gap-2">
            <Button variant="secondary" size="sm" disabled={page === 0} onClick={() => setPage(page - 1)}>
              <ChevronLeft size={14} />
            </Button>
            <span className="flex items-center text-sm text-gray-600">{page + 1} / {totalPages}</span>
            <Button variant="secondary" size="sm" disabled={page >= totalPages - 1} onClick={() => setPage(page + 1)}>
              <ChevronRight size={14} />
            </Button>
          </div>
        </div>
      )}

      <Modal open={!!preview} onClose={() => setPreview(null)} title={preview?.fileName || ''} wide>
        {preview && <DocumentViewer document={preview} />}
      </Modal>
    </div>
  )
}
