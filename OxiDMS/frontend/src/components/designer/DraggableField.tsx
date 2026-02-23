import { useDraggable } from '@dnd-kit/core'
import { GripVertical } from 'lucide-react'
import type { FieldDefinition } from '../../api/types'
import { FieldPreview } from './FieldPreview'

interface DraggableFieldProps {
  field: FieldDefinition
  isSelected: boolean
  onClick: () => void
}

export function DraggableField({ field, isSelected, onClick }: DraggableFieldProps) {
  const { attributes, listeners, setNodeRef, transform, isDragging } = useDraggable({
    id: field.name,
    data: { type: 'canvas-field', field },
  })

  const style: React.CSSProperties = {
    position: 'absolute',
    left: field.x ?? 0,
    top: field.y ?? 0,
    width: field.w ?? 300,
    transform: transform ? `translate(${transform.x}px, ${transform.y}px)` : undefined,
    zIndex: isDragging ? 50 : isSelected ? 10 : 1,
    opacity: isDragging ? 0.7 : 1,
  }

  return (
    <div
      ref={setNodeRef}
      style={style}
      onClick={(e) => {
        e.stopPropagation()
        onClick()
      }}
      className={`group rounded-lg border-2 bg-white shadow-sm transition-shadow ${
        isSelected
          ? 'border-blue-500 shadow-blue-100 shadow-md'
          : 'border-gray-200 hover:border-gray-300 hover:shadow'
      } ${isDragging ? 'shadow-lg cursor-grabbing' : ''}`}
    >
      <div className="flex items-start gap-2 p-3">
        <button
          {...attributes}
          {...listeners}
          className="mt-0.5 cursor-grab text-gray-400 hover:text-gray-600 active:cursor-grabbing"
        >
          <GripVertical size={16} />
        </button>
        <div className="flex-1 min-w-0">
          <FieldPreview field={field} />
        </div>
      </div>
    </div>
  )
}
