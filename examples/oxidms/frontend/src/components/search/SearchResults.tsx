import { ChevronLeft, ChevronRight } from 'lucide-react'
import type { SearchResult } from '../../api/types'
import { Badge } from '../ui/Badge'
import { Button } from '../ui/Button'

interface SearchResultsProps {
  result: SearchResult
  fieldLabels: Record<string, string>
  page: number
  pageSize: number
  elapsedMs: number | null
  onPageChange: (page: number) => void
}

export function SearchResults({ result, fieldLabels, page, pageSize, elapsedMs, onPageChange }: SearchResultsProps) {
  const totalPages = Math.max(1, Math.ceil(result.total / pageSize))
  const from = page * pageSize + 1
  const to = Math.min((page + 1) * pageSize, result.total)

  if (result.total === 0) {
    return (
      <div className="space-y-3">
        <div className="flex items-center gap-3">
          <span className="text-sm text-gray-500">0 results</span>
          <Badge color="blue">{result.mode}</Badge>
          {elapsedMs !== null && (
            <span className="text-xs text-gray-400">{elapsedMs.toFixed(0)} ms</span>
          )}
        </div>
        <p className="py-8 text-center text-gray-400">No results found</p>
      </div>
    )
  }

  return (
    <div className="space-y-4">
      {/* Header: count, mode, timing */}
      <div className="flex items-center gap-3">
        <span className="text-sm text-gray-500">
          {result.total} result{result.total !== 1 ? 's' : ''}
        </span>
        <Badge color="blue">{result.mode}</Badge>
        {elapsedMs !== null && (
          <span className="text-xs text-gray-400">{elapsedMs.toFixed(0)} ms</span>
        )}
      </div>

      {/* Table */}
      <div className="overflow-x-auto rounded-lg border border-gray-200">
        <table className="min-w-full divide-y divide-gray-200">
          <thead className="bg-gray-50">
            <tr>
              {Object.keys(fieldLabels).map((key) => (
                <th key={key} className="px-4 py-3 text-left text-xs font-medium uppercase text-gray-500">
                  {fieldLabels[key]}
                </th>
              ))}
              {result.docs.some((d) => '_score' in d) && (
                <th className="px-4 py-3 text-left text-xs font-medium uppercase text-gray-500">Score</th>
              )}
            </tr>
          </thead>
          <tbody className="divide-y divide-gray-200 bg-white">
            {result.docs.map((doc, i) => {
              const data = (doc.data || doc) as Record<string, unknown>
              return (
                <tr key={i} className="hover:bg-gray-50">
                  {Object.keys(fieldLabels).map((key) => (
                    <td key={key} className="px-4 py-3 text-sm text-gray-700 max-w-[200px] truncate">
                      {String(data[key] ?? '-')}
                    </td>
                  ))}
                  {doc._score !== undefined && (
                    <td className="px-4 py-3 text-sm text-gray-500">
                      {Number(doc._score).toFixed(3)}
                    </td>
                  )}
                </tr>
              )
            })}
          </tbody>
        </table>
      </div>

      {/* Pagination */}
      {totalPages > 1 && (
        <div className="flex items-center justify-between border-t border-gray-200 pt-4">
          <span className="text-sm text-gray-500">
            Showing {from}–{to} of {result.total}
          </span>
          <div className="flex items-center gap-2">
            <Button
              variant="secondary"
              size="sm"
              onClick={() => onPageChange(page - 1)}
              disabled={page === 0}
            >
              <ChevronLeft size={16} />
              Previous
            </Button>
            <span className="text-sm text-gray-600 min-w-[80px] text-center">
              Page {page + 1} of {totalPages}
            </span>
            <Button
              variant="secondary"
              size="sm"
              onClick={() => onPageChange(page + 1)}
              disabled={page >= totalPages - 1}
            >
              Next
              <ChevronRight size={16} />
            </Button>
          </div>
        </div>
      )}
    </div>
  )
}
