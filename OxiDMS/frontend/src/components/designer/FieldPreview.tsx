import type { FieldDefinition } from '../../api/types'
import { Badge } from '../ui/Badge'

export function FieldPreview({ field }: { field: FieldDefinition }) {
  return (
    <div className="space-y-1.5">
      <div className="flex items-center gap-2">
        <span className="text-sm font-medium text-gray-700">{field.label}</span>
        {field.required && <Badge color="red">Required</Badge>}
        {field.indexed && <Badge color="blue">Indexed</Badge>}
        <Badge color="gray">{field.type}</Badge>
      </div>
      {renderPreview(field)}
    </div>
  )
}

function renderPreview(field: FieldDefinition) {
  const cls = "w-full rounded border border-gray-200 bg-gray-50 px-3 py-1.5 text-sm text-gray-400"
  switch (field.type) {
    case 'textarea':
      return <div className={`${cls} h-16`}>{field.placeholder || 'Text area...'}</div>
    case 'select':
      return (
        <select className={cls} disabled>
          <option>Select...</option>
          {field.options?.map((o) => <option key={o}>{o}</option>)}
        </select>
      )
    case 'checkbox':
      return (
        <div className="flex items-center gap-2">
          <input type="checkbox" disabled className="rounded border-gray-300" />
          <span className="text-sm text-gray-400">{field.label}</span>
        </div>
      )
    case 'radio':
      return (
        <div className="space-y-1">
          {(field.options || []).map((o) => (
            <div key={o} className="flex items-center gap-2">
              <input type="radio" disabled className="border-gray-300" />
              <span className="text-sm text-gray-400">{o}</span>
            </div>
          ))}
        </div>
      )
    case 'file':
      return <div className={`${cls} text-center`}>Drop file here or click to upload</div>
    default:
      return <div className={cls}>{field.placeholder || `Enter ${field.type}...`}</div>
  }
}
