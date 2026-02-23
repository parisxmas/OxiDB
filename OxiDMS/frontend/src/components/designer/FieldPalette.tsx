import { useDraggable } from '@dnd-kit/core'
import { Type, AlignLeft, Hash, Mail, Calendar, ChevronDown, CheckSquare, Circle, Upload, type LucideIcon } from 'lucide-react'
import { FIELD_TYPES, type FieldTypeInfo } from './fieldTypes'

const iconMap: Record<string, LucideIcon> = {
  Type, AlignLeft, Hash, Mail, Calendar, ChevronDown, CheckSquare, Circle, Upload,
}

function PaletteItem({ fieldType }: { fieldType: FieldTypeInfo }) {
  const { attributes, listeners, setNodeRef, isDragging } = useDraggable({
    id: `palette-${fieldType.type}`,
    data: { type: 'palette', fieldType },
  })

  const Icon = iconMap[fieldType.icon] || Type

  return (
    <div
      ref={setNodeRef}
      {...listeners}
      {...attributes}
      className={`flex cursor-grab items-center gap-2 rounded-lg border border-gray-200 bg-white px-3 py-2 text-sm font-medium text-gray-700 shadow-sm transition-all hover:border-blue-300 hover:shadow ${isDragging ? 'opacity-50' : ''}`}
    >
      <Icon size={16} />
      {fieldType.label}
    </div>
  )
}

export function FieldPalette() {
  return (
    <div className="space-y-2">
      <h3 className="text-sm font-semibold text-gray-500 uppercase tracking-wider">Field Types</h3>
      <div className="space-y-1.5">
        {FIELD_TYPES.map((ft) => (
          <PaletteItem key={ft.type} fieldType={ft} />
        ))}
      </div>
    </div>
  )
}
