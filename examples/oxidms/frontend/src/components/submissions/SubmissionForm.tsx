import { useForm } from 'react-hook-form'
import type { FieldDefinition } from '../../api/types'
import { DynamicField } from './DynamicField'
import { Button } from '../ui/Button'
import { GRID } from '../designer/fieldTypes'

interface SubmissionFormProps {
  fields: FieldDefinition[]
  onSubmit: (data: Record<string, unknown>, files: File[]) => Promise<void>
  loading?: boolean
  defaultValues?: Record<string, unknown>
}

export function SubmissionForm({ fields, onSubmit, loading, defaultValues }: SubmissionFormProps) {
  const { register, handleSubmit, formState: { errors } } = useForm({ defaultValues })

  const hasPositions = fields.some((f) => f.w && f.w > 0)

  const handleFormSubmit = async (data: Record<string, unknown>) => {
    const files: File[] = []
    const cleanData: Record<string, unknown> = {}

    for (const field of fields) {
      if (field.type === 'file') {
        const fileList = data[field.name] as FileList
        if (fileList?.length) {
          for (let i = 0; i < fileList.length; i++) {
            files.push(fileList[i])
          }
        }
        continue
      }
      if (field.type === 'number' && data[field.name] !== '') {
        cleanData[field.name] = Number(data[field.name])
      } else {
        cleanData[field.name] = data[field.name]
      }
    }

    await onSubmit(cleanData, files)
  }

  // Calculate canvas height from field positions
  const canvasHeight = hasPositions
    ? fields.reduce((max, f) => Math.max(max, (f.y ?? 0) + (f.h ?? 80)), 0) + GRID * 2
    : 0

  return (
    <form onSubmit={handleSubmit(handleFormSubmit)}>
      {hasPositions ? (
        <div className="relative" style={{ minHeight: canvasHeight }}>
          {fields.map((field) => (
            <div
              key={field.name}
              style={{
                position: 'absolute',
                left: field.x ?? 0,
                top: field.y ?? 0,
                width: field.w ?? 300,
              }}
            >
              <DynamicField
                field={field}
                register={register}
                error={errors[field.name]?.message as string}
              />
            </div>
          ))}
        </div>
      ) : (
        <div className="space-y-4">
          {fields.map((field) => (
            <DynamicField
              key={field.name}
              field={field}
              register={register}
              error={errors[field.name]?.message as string}
            />
          ))}
        </div>
      )}
      <div className="pt-4">
        <Button type="submit" disabled={loading}>
          {loading ? 'Submitting...' : 'Submit'}
        </Button>
      </div>
    </form>
  )
}
