import { useDroppable } from '@dnd-kit/core'
import type { FieldDefinition } from '../../api/types'
import { DraggableField } from './DraggableField'
import { GRID } from './fieldTypes'

export interface AlignmentGuide {
  type: 'vertical' | 'horizontal'
  position: number
}

interface DesignerCanvasProps {
  fields: FieldDefinition[]
  selectedIndex: number | null
  onSelect: (index: number) => void
  onDeselect: () => void
  guides: AlignmentGuide[]
}

export function DesignerCanvas({ fields, selectedIndex, onSelect, onDeselect, guides }: DesignerCanvasProps) {
  const { setNodeRef, isOver } = useDroppable({ id: 'canvas' })

  const maxBottom = fields.reduce((max, f) => {
    const bottom = (f.y ?? 0) + (f.h ?? 80) + 100
    return bottom > max ? bottom : max
  }, 600)

  return (
    <div
      ref={setNodeRef}
      onClick={onDeselect}
      className={`relative rounded-xl border-2 transition-colors overflow-auto ${
        isOver ? 'border-blue-400 bg-blue-50/20' : 'border-gray-200'
      }`}
      style={{
        minHeight: 'calc(100vh - 200px)',
        height: 'calc(100vh - 200px)',
        backgroundImage: `radial-gradient(circle, #d1d5db 1px, transparent 1px)`,
        backgroundSize: `${GRID}px ${GRID}px`,
      }}
    >
      <div className="relative" style={{ minHeight: Math.max(maxBottom, 600) }}>
        {fields.length === 0 && (
          <div className="absolute inset-0 flex items-center justify-center pointer-events-none">
            <div className="text-center text-gray-400">
              <p className="text-lg font-medium">Drag fields here</p>
              <p className="text-sm mt-1">Drop field types from the palette to build your form</p>
            </div>
          </div>
        )}

        {fields.map((field, i) => (
          <DraggableField
            key={field.name}
            field={field}
            isSelected={selectedIndex === i}
            onClick={() => onSelect(i)}
          />
        ))}

        {/* Alignment guides */}
        {guides.map((guide, i) =>
          guide.type === 'vertical' ? (
            <div
              key={`guide-${i}`}
              className="absolute top-0 bottom-0 pointer-events-none"
              style={{
                left: guide.position,
                width: 1,
                backgroundColor: 'rgba(59, 130, 246, 0.5)',
                zIndex: 100,
              }}
            />
          ) : (
            <div
              key={`guide-${i}`}
              className="absolute left-0 right-0 pointer-events-none"
              style={{
                top: guide.position,
                height: 1,
                backgroundColor: 'rgba(59, 130, 246, 0.5)',
                zIndex: 100,
              }}
            />
          )
        )}
      </div>
    </div>
  )
}
