import { useEffect, useState } from 'react'
import type { Document as DocType } from '../../api/types'
import { downloadFile } from '../../api/client'
import { Spinner } from '../ui/Spinner'

interface DocumentViewerProps {
  document: DocType
}

export function DocumentViewer({ document }: DocumentViewerProps) {
  const [blobUrl, setBlobUrl] = useState<string | null>(null)

  useEffect(() => {
    const token = localStorage.getItem('token')
    fetch(`/api/v1/documents/${document._id}/download`, {
      headers: token ? { Authorization: `Bearer ${token}` } : {},
    })
      .then((res) => res.blob())
      .then((blob) => setBlobUrl(URL.createObjectURL(blob)))
    return () => { if (blobUrl) URL.revokeObjectURL(blobUrl) }
  }, [document._id])

  if (!blobUrl) {
    return <div className="flex justify-center py-12"><Spinner className="h-8 w-8 text-blue-600" /></div>
  }

  if (document.contentType.startsWith('image/')) {
    return <img src={blobUrl} alt={document.fileName} className="max-h-[70vh] rounded-lg object-contain" />
  }

  if (document.contentType === 'application/pdf') {
    return <iframe src={blobUrl} className="h-[70vh] w-full rounded-lg border" title={document.fileName} />
  }

  if (document.contentType.startsWith('text/')) {
    return <iframe src={blobUrl} className="h-[70vh] w-full rounded-lg border font-mono" title={document.fileName} />
  }

  return (
    <div className="flex flex-col items-center justify-center rounded-lg border border-gray-200 p-12">
      <p className="text-gray-500">Preview not available for this file type</p>
      <button
        onClick={() => downloadFile(`/documents/${document._id}/download`, document.fileName)}
        className="mt-4 text-sm font-medium text-blue-600 hover:text-blue-500"
      >
        Download file
      </button>
    </div>
  )
}
