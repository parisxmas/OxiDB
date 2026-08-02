import type { FieldDefinition } from '../../api/types'
import { Input } from '../ui/Input'

export interface FilterValues {
  [field: string]: { value?: string; min?: string; max?: string }
}

interface FilterBuilderProps {
  fields: FieldDefinition[]
  filters: FilterValues
  onChange: (filters: FilterValues) => void
}

export function FilterBuilder({ fields, filters, onChange }: FilterBuilderProps) {
  const update = (name: string, patch: Partial<{ value: string; min: string; max: string }>) => {
    onChange({ ...filters, [name]: { ...filters[name], ...patch } })
  }

  return (
    <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-3">
      {fields.filter((f) => f.type !== 'file').map((field) => {
        if (field.type === 'number' || field.type === 'date') {
          return (
            <div key={field.name} className="space-y-1">
              <label className="block text-sm font-medium text-gray-700">{field.label}</label>
              <div className="grid grid-cols-2 gap-2">
                <Input
                  type={field.type}
                  placeholder="From"
                  value={filters[field.name]?.min || ''}
                  onChange={(e) => update(field.name, { min: e.target.value })}
                />
                <Input
                  type={field.type}
                  placeholder="To"
                  value={filters[field.name]?.max || ''}
                  onChange={(e) => update(field.name, { max: e.target.value })}
                />
              </div>
            </div>
          )
        }

        if (field.type === 'select' || field.type === 'radio') {
          return (
            <div key={field.name} className="space-y-1">
              <label className="block text-sm font-medium text-gray-700">{field.label}</label>
              <select
                className="w-full rounded-lg border border-gray-300 px-3 py-2 text-sm"
                value={filters[field.name]?.value || ''}
                onChange={(e) => update(field.name, { value: e.target.value })}
              >
                <option value="">All</option>
                {field.options?.map((o) => <option key={o} value={o}>{o}</option>)}
              </select>
            </div>
          )
        }

        return (
          <Input
            key={field.name}
            label={field.label}
            value={filters[field.name]?.value || ''}
            onChange={(e) => update(field.name, { value: e.target.value })}
            placeholder={`Filter by ${field.label.toLowerCase()}...`}
          />
        )
      })}
    </div>
  )
}
