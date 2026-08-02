import { useEffect, useState, useCallback } from 'react'
import { useParams, useNavigate } from 'react-router-dom'
import { ArrowLeft, Plus, Settings, Search, X } from 'lucide-react'
import { api } from '../api/client'
import type { Form, FieldDefinition, SearchResult } from '../api/types'
import { Button } from '../components/ui/Button'
import { Spinner } from '../components/ui/Spinner'
import { Badge } from '../components/ui/Badge'
import { SubmissionList } from '../components/submissions/SubmissionList'

export function FormDetailPage() {
  const { formId } = useParams<{ formId: string }>()
  const navigate = useNavigate()
  const [form, setForm] = useState<Form | null>(null)
  const [loading, setLoading] = useState(true)

  // Search state
  const [filters, setFilters] = useState<Record<string, string>>({})
  const [searchResults, setSearchResults] = useState<SearchResult | null>(null)
  const [searching, setSearching] = useState(false)

  useEffect(() => {
    api.get<Form>(`/forms/${formId}`).then(setForm)
      .catch(() => navigate('/forms')).finally(() => setLoading(false))
  }, [formId, navigate])

  const indexedFields: FieldDefinition[] = form
    ? (form.fields as FieldDefinition[]).filter((f) => f.indexed)
    : []

  const hasActiveFilters = Object.values(filters).some((v) => v !== '')

  const handleSearch = useCallback(async () => {
    if (!form || !hasActiveFilters) return
    setSearching(true)
    try {
      const searchFilters: Record<string, any> = {}
      for (const [name, value] of Object.entries(filters)) {
        if (!value) continue
        const field = indexedFields.find((f) => f.name === name)
        if (!field) continue

        if (field.type === 'number') {
          searchFilters[name] = { value: Number(value) }
        } else {
          searchFilters[name] = { value }
        }
      }
      const result = await api.post<SearchResult>('/search', {
        formId: form._id,
        filters: searchFilters,
        limit: 100,
      })
      setSearchResults(result)
    } catch {
      setSearchResults(null)
    } finally {
      setSearching(false)
    }
  }, [form, filters, hasActiveFilters, indexedFields])

  const clearSearch = () => {
    setFilters({})
    setSearchResults(null)
  }

  if (loading) return <div className="flex justify-center py-20"><Spinner className="h-8 w-8 text-blue-600" /></div>
  if (!form) return null

  return (
    <div>
      <button onClick={() => navigate('/forms')} className="mb-4 flex items-center gap-1 text-sm text-gray-500 hover:text-gray-700">
        <ArrowLeft size={16} /> Back to Forms
      </button>

      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-gray-900">{form.name}</h1>
          {form.description && <p className="mt-1 text-sm text-gray-500">{form.description}</p>}
          <div className="mt-2 flex gap-2">
            <Badge color="blue">{form.fields.length} fields</Badge>
            <Badge color="gray">{form.slug}</Badge>
          </div>
        </div>
        <div className="flex gap-2">
          <Button variant="secondary" onClick={() => navigate(`/forms/${form._id}/edit`)}>
            <Settings size={16} className="mr-1" /> Edit Form
          </Button>
          <Button onClick={() => navigate(`/forms/${form._id}/submit`)}>
            <Plus size={16} className="mr-1" /> New Submission
          </Button>
        </div>
      </div>

      {/* Search by indexed fields */}
      {indexedFields.length > 0 && (
        <div className="mt-6 rounded-xl border border-gray-200 bg-white p-4">
          <div className="flex items-center gap-2 mb-3">
            <Search size={16} className="text-gray-400" />
            <h3 className="text-sm font-semibold text-gray-600">Search Submissions</h3>
            {searchResults && (
              <Badge color="blue">{searchResults.total} result{searchResults.total !== 1 ? 's' : ''}</Badge>
            )}
          </div>
          <div className="flex flex-wrap items-end gap-3">
            {indexedFields.map((field) => (
              <div key={field.name} className="flex-shrink-0">
                <label className="block text-xs font-medium text-gray-500 mb-1">{field.label}</label>
                {field.type === 'select' ? (
                  <select
                    className="rounded-lg border border-gray-300 px-3 py-1.5 text-sm focus:border-blue-500 focus:outline-none focus:ring-1 focus:ring-blue-500"
                    value={filters[field.name] || ''}
                    onChange={(e) => setFilters((prev) => ({ ...prev, [field.name]: e.target.value }))}
                  >
                    <option value="">All</option>
                    {field.options?.map((o) => <option key={o} value={o}>{o}</option>)}
                  </select>
                ) : field.type === 'checkbox' ? (
                  <select
                    className="rounded-lg border border-gray-300 px-3 py-1.5 text-sm focus:border-blue-500 focus:outline-none focus:ring-1 focus:ring-blue-500"
                    value={filters[field.name] || ''}
                    onChange={(e) => setFilters((prev) => ({ ...prev, [field.name]: e.target.value }))}
                  >
                    <option value="">All</option>
                    <option value="true">Yes</option>
                    <option value="false">No</option>
                  </select>
                ) : (
                  <input
                    type={field.type === 'number' ? 'number' : field.type === 'date' ? 'date' : 'text'}
                    className="rounded-lg border border-gray-300 px-3 py-1.5 text-sm focus:border-blue-500 focus:outline-none focus:ring-1 focus:ring-blue-500 w-40"
                    placeholder={`Search ${field.label.toLowerCase()}...`}
                    value={filters[field.name] || ''}
                    onChange={(e) => setFilters((prev) => ({ ...prev, [field.name]: e.target.value }))}
                    onKeyDown={(e) => e.key === 'Enter' && handleSearch()}
                  />
                )}
              </div>
            ))}
            <div className="flex gap-2">
              <Button size="sm" onClick={handleSearch} disabled={!hasActiveFilters || searching}>
                <Search size={14} className="mr-1" />
                {searching ? 'Searching...' : 'Search'}
              </Button>
              {searchResults && (
                <Button variant="ghost" size="sm" onClick={clearSearch}>
                  <X size={14} className="mr-1" /> Clear
                </Button>
              )}
            </div>
          </div>
        </div>
      )}

      <div className="mt-8">
        <h2 className="mb-4 text-lg font-semibold text-gray-900">
          {searchResults ? 'Search Results' : 'Submissions'}
        </h2>
        <SubmissionList
          form={form}
          onView={(sub) => navigate(`/forms/${form._id}/submissions/${sub._id}`)}
          searchResults={searchResults}
        />
      </div>
    </div>
  )
}
