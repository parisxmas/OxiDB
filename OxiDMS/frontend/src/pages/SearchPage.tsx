import { useEffect, useState } from 'react'
import { Search } from 'lucide-react'
import toast from 'react-hot-toast'
import { api } from '../api/client'
import type { Form, SearchResult as SearchResultType } from '../api/types'
import { Button } from '../components/ui/Button'
import { Input } from '../components/ui/Input'
import { Spinner } from '../components/ui/Spinner'
import { FilterBuilder, type FilterValues } from '../components/search/FilterBuilder'
import { SearchResults } from '../components/search/SearchResults'

const PAGE_SIZE = 20

export function SearchPage() {
  const [forms, setForms] = useState<Form[]>([])
  const [selectedFormId, setSelectedFormId] = useState('')
  const [textQuery, setTextQuery] = useState('')
  const [filters, setFilters] = useState<FilterValues>({})
  const [result, setResult] = useState<SearchResultType | null>(null)
  const [searching, setSearching] = useState(false)
  const [page, setPage] = useState(0)
  const [elapsedMs, setElapsedMs] = useState<number | null>(null)

  useEffect(() => {
    api.get<Form[]>('/forms').then(setForms)
  }, [])

  const selectedForm = forms.find((f) => f._id === selectedFormId)

  const runSearch = async (skipVal: number) => {
    setSearching(true)
    try {
      const apiFilters: Record<string, any> = {}
      for (const [key, val] of Object.entries(filters)) {
        if (val.min || val.max) {
          apiFilters[key] = { min: val.min || null, max: val.max || null }
        } else if (val.value) {
          apiFilters[key] = { value: val.value }
        }
      }

      const t0 = performance.now()
      const res = await api.post<SearchResultType>('/search', {
        formId: selectedFormId,
        filters: apiFilters,
        textQuery: textQuery.trim(),
        skip: skipVal,
        limit: PAGE_SIZE,
      })
      setElapsedMs(performance.now() - t0)
      setResult(res)
    } catch (err: any) {
      toast.error(err.message)
    } finally {
      setSearching(false)
    }
  }

  const handleSearch = () => {
    setPage(0)
    runSearch(0)
  }

  const handlePageChange = (newPage: number) => {
    setPage(newPage)
    runSearch(newPage * PAGE_SIZE)
  }

  const fieldLabels: Record<string, string> = {}
  if (selectedForm) {
    selectedForm.fields.forEach((f) => {
      if (f.type !== 'file') fieldLabels[f.name] = f.label
    })
  }

  return (
    <div>
      <h1 className="text-2xl font-bold text-gray-900">Search</h1>

      <div className="mt-6 space-y-6 rounded-xl border border-gray-200 bg-white p-6 shadow-sm">
        {/* Form selector */}
        <div className="grid gap-4 sm:grid-cols-2">
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">Form</label>
            <select
              className="w-full rounded-lg border border-gray-300 px-3 py-2 text-sm"
              value={selectedFormId}
              onChange={(e) => { setSelectedFormId(e.target.value); setFilters({}); setResult(null) }}
            >
              <option value="">All forms</option>
              {forms.map((f) => <option key={f._id} value={f._id}>{f.name}</option>)}
            </select>
          </div>
          <Input
            label="Full-text Search"
            value={textQuery}
            onChange={(e) => setTextQuery(e.target.value)}
            placeholder="Search in submission data..."
          />
        </div>

        {/* Field filters */}
        {selectedForm && (
          <div>
            <h3 className="mb-3 text-sm font-semibold text-gray-500 uppercase tracking-wider">Field Filters</h3>
            <FilterBuilder fields={selectedForm.fields} filters={filters} onChange={setFilters} />
          </div>
        )}

        <div className="flex justify-end">
          <Button onClick={handleSearch} disabled={searching}>
            {searching ? <Spinner className="mr-2 h-4 w-4" /> : <Search size={16} className="mr-2" />}
            Search
          </Button>
        </div>
      </div>

      {/* Results */}
      {result && (
        <div className="mt-6">
          <SearchResults
            result={result}
            fieldLabels={Object.keys(fieldLabels).length > 0 ? fieldLabels : { _id: 'ID' }}
            page={page}
            pageSize={PAGE_SIZE}
            elapsedMs={elapsedMs}
            onPageChange={handlePageChange}
          />
        </div>
      )}
    </div>
  )
}
