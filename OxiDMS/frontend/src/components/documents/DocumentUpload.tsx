import { useState, useCallback } from 'react'
import { Upload } from 'lucide-react'
import { api } from '../../api/client'
import type { Document as DocType } from '../../api/types'
import toast from 'react-hot-toast'

interface DocumentUploadProps {
  onUploaded: (doc: DocType) => void
  formId?: string
  submissionId?: string
}

export function DocumentUpload({ onUploaded, formId, submissionId }: DocumentUploadProps) {
  const [dragging, setDragging] = useState(false)
  const [uploading, setUploading] = useState(false)

  const uploadFile = useCallback(async (file: File) => {
    setUploading(true)
    try {
      const formData = new FormData()
      formData.append('file', file)
      if (formId) formData.append('formId', formId)
      if (submissionId) formData.append('submissionId', submissionId)
      const doc = await api.upload<DocType>('/documents', formData)
      onUploaded(doc)
      toast.success(`Uploaded ${file.name}`)
    } catch (err: any) {
      toast.error(err.message)
    } finally {
      setUploading(false)
    }
  }, [formId, submissionId, onUploaded])

  const handleDrop = useCallback((e: React.DragEvent) => {
    e.preventDefault()
    setDragging(false)
    const files = e.dataTransfer.files
    if (files.length > 0) uploadFile(files[0])
  }, [uploadFile])

  return (
    <div
      onDragOver={(e) => { e.preventDefault(); setDragging(true) }}
      onDragLeave={() => setDragging(false)}
      onDrop={handleDrop}
      className={`flex flex-col items-center justify-center rounded-xl border-2 border-dashed p-8 transition-colors ${
        dragging ? 'border-blue-400 bg-blue-50' : 'border-gray-300 hover:border-gray-400'
      }`}
    >
      <Upload className={`mb-3 ${uploading ? 'animate-bounce text-blue-500' : 'text-gray-400'}`} size={32} />
      <p className="text-sm text-gray-600">
        {uploading ? 'Uploading...' : 'Drag & drop a file here, or'}
      </p>
      {!uploading && (
        <label className="mt-2 cursor-pointer text-sm font-medium text-blue-600 hover:text-blue-500">
          browse files
          <input type="file" className="hidden" onChange={(e) => {
            if (e.target.files?.[0]) uploadFile(e.target.files[0])
          }} />
        </label>
      )}
    </div>
  )
}
