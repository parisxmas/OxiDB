import type { FieldDefinition } from '../../api/types'
import { UseFormRegister } from 'react-hook-form'

interface DynamicFieldProps {
  field: FieldDefinition
  register: UseFormRegister<any>
  error?: string
}

export function DynamicField({ field, register, error }: DynamicFieldProps) {
  const cls = 'w-full rounded-lg border border-gray-300 px-3 py-2 text-sm focus:border-blue-500 focus:outline-none focus:ring-1 focus:ring-blue-500'
  const errorCls = error ? 'border-red-300' : ''

  return (
    <div className="space-y-1">
      <label className="block text-sm font-medium text-gray-700">
        {field.label}
        {field.required && <span className="text-red-500 ml-1">*</span>}
      </label>

      {field.type === 'textarea' ? (
        <textarea
          {...register(field.name, { required: field.required ? `${field.label} is required` : false })}
          placeholder={field.placeholder}
          rows={4}
          className={`${cls} ${errorCls}`}
        />
      ) : field.type === 'select' ? (
        <select
          {...register(field.name, { required: field.required ? `${field.label} is required` : false })}
          className={`${cls} ${errorCls}`}
        >
          <option value="">Select...</option>
          {field.options?.map((o) => <option key={o} value={o}>{o}</option>)}
        </select>
      ) : field.type === 'checkbox' ? (
        <div className="flex items-center gap-2">
          <input
            type="checkbox"
            {...register(field.name)}
            className="rounded border-gray-300 text-blue-600"
          />
          <span className="text-sm text-gray-600">{field.label}</span>
        </div>
      ) : field.type === 'radio' ? (
        <div className="space-y-2">
          {field.options?.map((o) => (
            <div key={o} className="flex items-center gap-2">
              <input
                type="radio"
                value={o}
                {...register(field.name, { required: field.required ? `${field.label} is required` : false })}
                className="border-gray-300 text-blue-600"
              />
              <span className="text-sm text-gray-600">{o}</span>
            </div>
          ))}
        </div>
      ) : field.type === 'file' ? (
        <input
          type="file"
          {...register(field.name)}
          className="block w-full text-sm text-gray-500 file:mr-4 file:rounded-lg file:border-0 file:bg-blue-50 file:px-4 file:py-2 file:text-sm file:font-medium file:text-blue-700 hover:file:bg-blue-100"
        />
      ) : (
        <input
          type={field.type === 'number' ? 'number' : field.type === 'date' ? 'date' : field.type === 'email' ? 'email' : 'text'}
          {...register(field.name, {
            required: field.required ? `${field.label} is required` : false,
            ...(field.type === 'number' && field.min !== undefined ? { min: { value: field.min, message: `Min value is ${field.min}` } } : {}),
            ...(field.type === 'number' && field.max !== undefined ? { max: { value: field.max, message: `Max value is ${field.max}` } } : {}),
          })}
          placeholder={field.placeholder}
          className={`${cls} ${errorCls}`}
        />
      )}

      {error && <p className="text-sm text-red-600">{error}</p>}
    </div>
  )
}
