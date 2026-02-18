import { useState, useEffect } from 'react'
import { useNavigate } from 'react-router-dom'
import { api } from '../../api/client'
import type { Form, PaginatedSubmissions, Submission, SearchResult } from '../../api/types'
import { Button } from '../ui/Button'
import { Eye, Pencil, Trash2, ChevronLeft, ChevronRight } from 'lucide-react'

interface SubmissionListProps {
  form: Form
  onView: (sub: Submission) => void
  refresh?: number
  searchResults?: SearchResult | null
}

export function SubmissionList({ form, onView, refresh, searchResults }: SubmissionListProps) {
  const navigate = useNavigate()
  const [data, setData] = useState<PaginatedSubmissions | null>(null)
  const [page, setPage] = useState(0)
  const limit = 20

  useEffect(() => {
    if (searchResults) return // skip fetch when showing search results
    api.get<PaginatedSubmissions>(`/forms/${form._id}/submissions?skip=${page * limit}&limit=${limit}`)
      .then(setData)
  }, [form._id, page, refresh, searchResults])

  // Convert search results to submission-like objects
  const searchSubs: Submission[] | null = searchResults
    ? searchResults.docs.map((doc) => ({
        _id: String(doc._id ?? ''),
        formId: String(doc.formId ?? ''),
        data: (doc.data as Record<string, unknown>) ?? {},
        files: (doc.files as string[]) ?? [],
        createdBy: String(doc.createdBy ?? ''),
        createdAt: String(doc.createdAt ?? ''),
        updatedAt: String(doc.updatedAt ?? ''),
      }))
    : null

  const submissions = searchSubs ?? data?.submissions ?? []
  const total = searchResults ? searchResults.total : (data?.total ?? 0)
  const totalPages = searchResults ? 1 : Math.ceil(total / limit)

  const fields = form.fields as any[]

  if (!searchResults && !data) return null

  return (
    <div>
      {submissions.length === 0 ? (
        <p className="py-8 text-center text-gray-400">
          {searchResults ? 'No matching submissions found' : 'No submissions yet'}
        </p>
      ) : (
        <>
          <div className="overflow-x-auto rounded-lg border border-gray-200">
            <table className="min-w-full divide-y divide-gray-200">
              <thead className="bg-gray-50">
                <tr>
                  <th className="px-4 py-3 text-left text-xs font-medium uppercase text-gray-500">#</th>
                  {fields.slice(0, 4).map((f: any) => (
                    <th key={f.name} className="px-4 py-3 text-left text-xs font-medium uppercase text-gray-500">{f.label}</th>
                  ))}
                  <th className="px-4 py-3 text-left text-xs font-medium uppercase text-gray-500">Date</th>
                  <th className="px-4 py-3" />
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-200 bg-white">
                {submissions.map((sub, i) => (
                  <tr key={sub._id} className="hover:bg-gray-50">
                    <td className="px-4 py-3 text-sm text-gray-500">{(searchResults ? 0 : page * limit) + i + 1}</td>
                    {fields.slice(0, 4).map((f: any) => (
                      <td key={f.name} className="px-4 py-3 text-sm text-gray-700 max-w-[200px] truncate">
                        {String(sub.data[f.name] ?? '-')}
                      </td>
                    ))}
                    <td className="px-4 py-3 text-sm text-gray-500">
                      {new Date(sub.createdAt).toLocaleDateString()}
                    </td>
                    <td className="px-4 py-3 text-right">
                      <div className="flex gap-1 justify-end">
                        <Button variant="ghost" size="sm" onClick={() => onView(sub)}>
                          <Eye size={14} />
                        </Button>
                        <Button variant="ghost" size="sm" onClick={() => navigate(`/forms/${form._id}/submissions/${sub._id}/edit`)}>
                          <Pencil size={14} />
                        </Button>
                        <Button variant="ghost" size="sm" onClick={async () => {
                          await api.delete(`/forms/${form._id}/submissions/${sub._id}`)
                          setData((d) => d ? { ...d, submissions: d.submissions.filter((s) => s._id !== sub._id), total: d.total - 1 } : d)
                        }}>
                          <Trash2 size={14} className="text-red-500" />
                        </Button>
                      </div>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          {!searchResults && totalPages > 1 && (
            <div className="flex items-center justify-between pt-4">
              <span className="text-sm text-gray-500">{total} submissions</span>
              <div className="flex gap-2">
                <Button variant="secondary" size="sm" disabled={page === 0} onClick={() => setPage(page - 1)}>
                  <ChevronLeft size={14} />
                </Button>
                <span className="flex items-center text-sm text-gray-600">
                  {page + 1} / {totalPages}
                </span>
                <Button variant="secondary" size="sm" disabled={page >= totalPages - 1} onClick={() => setPage(page + 1)}>
                  <ChevronRight size={14} />
                </Button>
              </div>
            </div>
          )}
          {searchResults && (
            <div className="pt-4">
              <span className="text-sm text-gray-500">{total} result{total !== 1 ? 's' : ''}</span>
            </div>
          )}
        </>
      )}
    </div>
  )
}
