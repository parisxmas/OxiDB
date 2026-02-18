import type { SearchResult } from '../../api/types'
import { Badge } from '../ui/Badge'

interface SearchResultsProps {
  result: SearchResult
  fieldLabels: Record<string, string>
}

export function SearchResults({ result, fieldLabels }: SearchResultsProps) {
  if (result.docs.length === 0) {
    return <p className="py-8 text-center text-gray-400">No results found</p>
  }

  return (
    <div className="space-y-4">
      <div className="flex items-center gap-3">
        <span className="text-sm text-gray-500">{result.total} result{result.total !== 1 ? 's' : ''}</span>
        <Badge color="blue">{result.mode}</Badge>
      </div>
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
    </div>
  )
}
