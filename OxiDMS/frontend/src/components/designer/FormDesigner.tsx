import { useState, useCallback } from 'react'
import { DndContext, DragEndEvent, DragMoveEvent, DragOverlay, PointerSensor, useSensor, useSensors } from '@dnd-kit/core'
import type { FieldDefinition } from '../../api/types'
import { FieldPalette } from './FieldPalette'
import { DesignerCanvas, type AlignmentGuide } from './DesignerCanvas'
import { FieldEditor } from './FieldEditor'
import { createField, snap } from './fieldTypes'

const ALIGN_THRESHOLD = 8

interface FormDesignerProps {
  fields: FieldDefinition[]
  onChange: (fields: FieldDefinition[]) => void
}

export function FormDesigner({ fields, onChange }: FormDesignerProps) {
  const [selectedIndex, setSelectedIndex] = useState<number | null>(null)
  const [guides, setGuides] = useState<AlignmentGuide[]>([])

  const sensors = useSensors(
    useSensor(PointerSensor, { activationConstraint: { distance: 5 } })
  )

  const computeGuides = useCallback(
    (draggedName: string, x: number, y: number, w: number, h: number): AlignmentGuide[] => {
      const result: AlignmentGuide[] = []
      const dragRight = x + w
      const dragBottom = y + h

      for (const f of fields) {
        if (f.name === draggedName) continue
        const fx = f.x ?? 0
        const fy = f.y ?? 0
        const fw = f.w ?? 300
        const fh = f.h ?? 80
        const fRight = fx + fw
        const fBottom = fy + fh

        // Vertical guides (x-axis alignment)
        if (Math.abs(x - fx) < ALIGN_THRESHOLD) result.push({ type: 'vertical', position: fx })
        if (Math.abs(dragRight - fRight) < ALIGN_THRESHOLD) result.push({ type: 'vertical', position: fRight })
        if (Math.abs(x - fRight) < ALIGN_THRESHOLD) result.push({ type: 'vertical', position: fRight })
        if (Math.abs(dragRight - fx) < ALIGN_THRESHOLD) result.push({ type: 'vertical', position: fx })

        // Horizontal guides (y-axis alignment)
        if (Math.abs(y - fy) < ALIGN_THRESHOLD) result.push({ type: 'horizontal', position: fy })
        if (Math.abs(dragBottom - fBottom) < ALIGN_THRESHOLD) result.push({ type: 'horizontal', position: fBottom })
        if (Math.abs(y - fBottom) < ALIGN_THRESHOLD) result.push({ type: 'horizontal', position: fBottom })
        if (Math.abs(dragBottom - fy) < ALIGN_THRESHOLD) result.push({ type: 'horizontal', position: fy })
      }
      return result
    },
    [fields]
  )

  function handleDragMove(event: DragMoveEvent) {
    const { active, delta } = event
    const activeId = String(active.id)

    if (activeId.startsWith('palette-')) {
      setGuides([])
      return
    }

    const field = fields.find((f) => f.name === activeId)
    if (!field) return

    const newX = (field.x ?? 0) + delta.x
    const newY = (field.y ?? 0) + delta.y
    setGuides(computeGuides(activeId, newX, newY, field.w ?? 300, field.h ?? 80))
  }

  function handleDragEnd(event: DragEndEvent) {
    const { active, over, delta } = event
    setGuides([])

    // Dropping from palette to canvas
    if (String(active.id).startsWith('palette-')) {
      const data = active.data.current
      if (data?.fieldType && over?.id === 'canvas') {
        const newField = createField(data.fieldType, fields)

        // Try to place at drop position if we have reasonable coordinates
        const canvasEl = document.querySelector('[data-droppable-id="canvas"]') || document.getElementById('canvas')
        if (canvasEl) {
          const rect = canvasEl.getBoundingClientRect()
          const activatorEvent = event.activatorEvent as MouseEvent
          if (activatorEvent?.clientX) {
            const dropX = snap(Math.max(0, activatorEvent.clientX + delta.x - rect.left))
            const dropY = snap(Math.max(0, activatorEvent.clientY + delta.y - rect.top))
            newField.x = dropX
            newField.y = dropY
          }
        }

        const newFields = [...fields, newField]
        onChange(newFields)
        setSelectedIndex(newFields.length - 1)
      }
      return
    }

    // Moving a field within canvas
    const fieldIndex = fields.findIndex((f) => f.name === active.id)
    if (fieldIndex === -1) return

    const field = fields[fieldIndex]
    const newX = snap(Math.max(0, (field.x ?? 0) + delta.x))
    const newY = snap(Math.max(0, (field.y ?? 0) + delta.y))

    if (newX !== field.x || newY !== field.y) {
      const newFields = [...fields]
      newFields[fieldIndex] = { ...field, x: newX, y: newY }
      onChange(newFields)
      setSelectedIndex(fieldIndex)
    }
  }

  const selectedField = selectedIndex !== null ? fields[selectedIndex] : null

  return (
    <DndContext sensors={sensors} onDragMove={handleDragMove} onDragEnd={handleDragEnd}>
      <div className="flex gap-4" style={{ minHeight: 'calc(100vh - 200px)' }}>
        {/* Palette */}
        <div className="w-56 flex-shrink-0">
          <FieldPalette />
        </div>

        {/* Canvas */}
        <div className="flex-1 min-w-0">
          <DesignerCanvas
            fields={fields}
            selectedIndex={selectedIndex}
            onSelect={setSelectedIndex}
            onDeselect={() => setSelectedIndex(null)}
            guides={guides}
          />
        </div>

        {/* Properties */}
        <div className="w-72 flex-shrink-0">
          {selectedField ? (
            <FieldEditor
              field={selectedField}
              onChange={(updated) => {
                const newFields = [...fields]
                newFields[selectedIndex!] = updated
                onChange(newFields)
              }}
              onRemove={() => {
                onChange(fields.filter((_, i) => i !== selectedIndex))
                setSelectedIndex(null)
              }}
            />
          ) : (
            <div className="rounded-lg border border-gray-200 bg-gray-50 p-6 text-center text-sm text-gray-400">
              Select a field to edit its properties
            </div>
          )}
        </div>
      </div>
      <DragOverlay />
    </DndContext>
  )
}
