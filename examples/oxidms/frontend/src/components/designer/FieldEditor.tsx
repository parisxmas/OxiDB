import type { FieldDefinition } from '../../api/types'
import { Input } from '../ui/Input'
import { Button } from '../ui/Button'
import { Trash2, Plus } from 'lucide-react'
import { snap } from './fieldTypes'

interface FieldEditorProps {
  field: FieldDefinition
  onChange: (updated: FieldDefinition) => void
  onRemove: () => void
}

export function FieldEditor({ field, onChange, onRemove }: FieldEditorProps) {
  const update = (patch: Partial<FieldDefinition>) => onChange({ ...field, ...patch })

  return (
    <div className="space-y-4">
      <h3 className="text-sm font-semibold text-gray-500 uppercase tracking-wider">Field Properties</h3>

      {/* Position & Size */}
      <div className="space-y-2 rounded-lg border border-gray-200 bg-gray-50 p-3">
        <label className="block text-xs font-medium text-gray-500 uppercase tracking-wider">Position & Size</label>
        <div className="grid grid-cols-3 gap-2">
          <div>
            <label className="block text-xs text-gray-500 mb-0.5">X</label>
            <input
              type="number"
              className="w-full rounded border border-gray-300 px-2 py-1 text-sm"
              value={field.x ?? 0}
              onChange={(e) => update({ x: snap(Number(e.target.value) || 0) })}
              step={20}
              min={0}
            />
          </div>
          <div>
            <label className="block text-xs text-gray-500 mb-0.5">Y</label>
            <input
              type="number"
              className="w-full rounded border border-gray-300 px-2 py-1 text-sm"
              value={field.y ?? 0}
              onChange={(e) => update({ y: snap(Number(e.target.value) || 0) })}
              step={20}
              min={0}
            />
          </div>
          <div>
            <label className="block text-xs text-gray-500 mb-0.5">W</label>
            <input
              type="number"
              className="w-full rounded border border-gray-300 px-2 py-1 text-sm"
              value={field.w ?? 300}
              onChange={(e) => update({ w: snap(Math.max(60, Number(e.target.value) || 60)) })}
              step={20}
              min={60}
            />
          </div>
        </div>
      </div>

      <Input label="Label" value={field.label} onChange={(e) => update({ label: e.target.value })} />

      <Input label="Field Name" value={field.name} onChange={(e) => update({ name: e.target.value.replace(/\s+/g, '_').toLowerCase() })} />

      {(field.type === 'text' || field.type === 'textarea' || field.type === 'email') && (
        <Input label="Placeholder" value={field.placeholder || ''} onChange={(e) => update({ placeholder: e.target.value })} />
      )}

      <div className="flex items-center gap-4">
        <label className="flex items-center gap-2 text-sm">
          <input type="checkbox" checked={field.required || false} onChange={(e) => update({ required: e.target.checked })} className="rounded border-gray-300 text-blue-600" />
          Required
        </label>
        <label className="flex items-center gap-2 text-sm">
          <input type="checkbox" checked={field.indexed || false} onChange={(e) => update({ indexed: e.target.checked })} className="rounded border-gray-300 text-blue-600" />
          Indexed
        </label>
      </div>

      {(field.type === 'select' || field.type === 'radio') && (
        <div className="space-y-2">
          <label className="block text-sm font-medium text-gray-700">Options</label>
          {(field.options || []).map((opt, i) => (
            <div key={i} className="flex gap-2">
              <input
                className="flex-1 rounded border border-gray-300 px-3 py-1.5 text-sm"
                value={opt}
                onChange={(e) => {
                  const newOpts = [...(field.options || [])]
                  newOpts[i] = e.target.value
                  update({ options: newOpts })
                }}
              />
              <button
                onClick={() => update({ options: field.options?.filter((_, j) => j !== i) })}
                className="text-red-400 hover:text-red-600"
              >
                <Trash2 size={16} />
              </button>
            </div>
          ))}
          <Button
            variant="ghost"
            size="sm"
            onClick={() => update({ options: [...(field.options || []), `Option ${(field.options?.length || 0) + 1}`] })}
          >
            <Plus size={14} className="mr-1" /> Add Option
          </Button>
        </div>
      )}

      {field.type === 'number' && (
        <div className="grid grid-cols-2 gap-3">
          <Input label="Min" type="number" value={field.min ?? ''} onChange={(e) => update({ min: e.target.value ? Number(e.target.value) : undefined })} />
          <Input label="Max" type="number" value={field.max ?? ''} onChange={(e) => update({ max: e.target.value ? Number(e.target.value) : undefined })} />
        </div>
      )}

      {(field.type === 'text' || field.type === 'textarea') && (
        <div className="grid grid-cols-2 gap-3">
          <Input label="Min Length" type="number" value={field.minLength ?? ''} onChange={(e) => update({ minLength: e.target.value ? Number(e.target.value) : undefined })} />
          <Input label="Max Length" type="number" value={field.maxLength ?? ''} onChange={(e) => update({ maxLength: e.target.value ? Number(e.target.value) : undefined })} />
        </div>
      )}

      <div className="pt-4 border-t">
        <Button variant="danger" size="sm" onClick={onRemove}>
          <Trash2 size={14} className="mr-1" /> Remove Field
        </Button>
      </div>
    </div>
  )
}
