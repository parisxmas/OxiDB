import type { FieldDefinition } from '../../api/types'

export const GRID = 20

export interface FieldTypeInfo {
  type: FieldDefinition['type']
  label: string
  icon: string
  defaultField: Partial<FieldDefinition>
  defaultW: number
  defaultH: number
}

export const FIELD_TYPES: FieldTypeInfo[] = [
  { type: 'text', label: 'Text', icon: 'Type', defaultField: { placeholder: 'Enter text...' }, defaultW: 300, defaultH: 80 },
  { type: 'textarea', label: 'Text Area', icon: 'AlignLeft', defaultField: { placeholder: 'Enter long text...' }, defaultW: 300, defaultH: 120 },
  { type: 'number', label: 'Number', icon: 'Hash', defaultField: { placeholder: '0' }, defaultW: 300, defaultH: 80 },
  { type: 'email', label: 'Email', icon: 'Mail', defaultField: { placeholder: 'email@example.com' }, defaultW: 300, defaultH: 80 },
  { type: 'date', label: 'Date', icon: 'Calendar', defaultField: {}, defaultW: 300, defaultH: 80 },
  { type: 'select', label: 'Dropdown', icon: 'ChevronDown', defaultField: { options: ['Option 1', 'Option 2'] }, defaultW: 300, defaultH: 80 },
  { type: 'checkbox', label: 'Checkbox', icon: 'CheckSquare', defaultField: {}, defaultW: 200, defaultH: 60 },
  { type: 'radio', label: 'Radio', icon: 'Circle', defaultField: { options: ['Option 1', 'Option 2'] }, defaultW: 200, defaultH: 80 },
  { type: 'file', label: 'File Upload', icon: 'Upload', defaultField: {}, defaultW: 300, defaultH: 80 },
]

export function snap(val: number): number {
  return Math.round(val / GRID) * GRID
}

export function findNextAvailableY(fields: FieldDefinition[]): number {
  if (fields.length === 0) return GRID
  let maxBottom = 0
  for (const f of fields) {
    const bottom = (f.y ?? 0) + (f.h ?? 80)
    if (bottom > maxBottom) maxBottom = bottom
  }
  return snap(maxBottom + GRID)
}

const DEFAULT_W = 300
const DEFAULT_H = 80

/** Assign x/y/w/h to fields that are missing position data (legacy forms).
 *  Only touches fields that lack positions — preserves existing positions. */
export function normalizeFieldPositions(fields: FieldDefinition[]): FieldDefinition[] {
  // First pass: compute nextY from fields that already have positions
  let nextY = GRID
  for (const f of fields) {
    if (f.w && f.w > 0) {
      const bottom = (f.y ?? 0) + (f.h ?? DEFAULT_H) + GRID
      if (bottom > nextY) nextY = snap(bottom)
    }
  }

  // Second pass: only assign defaults to fields missing position data
  return fields.map((f) => {
    if (f.w && f.w > 0) return f // already has position — keep as-is

    const typeInfo = FIELD_TYPES.find((t) => t.type === f.type)
    const w = typeInfo?.defaultW || DEFAULT_W
    const h = typeInfo?.defaultH || DEFAULT_H
    const fixed: FieldDefinition = { ...f, x: GRID, y: nextY, w, h }
    nextY = snap(nextY + h + GRID)
    return fixed
  })
}

let counter = 0
export function createField(typeInfo: FieldTypeInfo, fields?: FieldDefinition[]): FieldDefinition {
  counter++
  const name = `${typeInfo.type}_${counter}`
  const y = fields ? findNextAvailableY(fields) : GRID
  return {
    name,
    label: `${typeInfo.label} Field`,
    type: typeInfo.type,
    required: false,
    x: GRID,
    y,
    w: typeInfo.defaultW,
    h: typeInfo.defaultH,
    ...typeInfo.defaultField,
  }
}
