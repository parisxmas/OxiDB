import { useEffect, useState } from 'react'
import { useParams, useNavigate } from 'react-router-dom'
import { ArrowLeft, Save } from 'lucide-react'
import toast from 'react-hot-toast'
import { api } from '../api/client'
import type { Form, FieldDefinition } from '../api/types'
import { Button } from '../components/ui/Button'
import { Input } from '../components/ui/Input'
import { Spinner } from '../components/ui/Spinner'
import { FormDesigner } from '../components/designer/FormDesigner'
import { normalizeFieldPositions } from '../components/designer/fieldTypes'

export function FormDesignerPage() {
  const { formId } = useParams<{ formId: string }>()
  const navigate = useNavigate()
  const isEdit = !!formId

  const [name, setName] = useState('')
  const [description, setDescription] = useState('')
  const [fields, setFields] = useState<FieldDefinition[]>([])
  const [saving, setSaving] = useState(false)
  const [loading, setLoading] = useState(isEdit)

  useEffect(() => {
    if (formId) {
      api.get<Form>(`/forms/${formId}`)
        .then((f) => {
          setName(f.name)
          setDescription(f.description || '')
          setFields(normalizeFieldPositions(f.fields))
        })
        .catch(() => {
          toast.error('Form not found')
          navigate('/forms')
        })
        .finally(() => setLoading(false))
    }
  }, [formId, navigate])

  const handleSave = async () => {
    if (!name.trim()) {
      toast.error('Form name is required')
      return
    }
    if (fields.length === 0) {
      toast.error('Add at least one field')
      return
    }
    setSaving(true)
    try {
      if (isEdit) {
        await api.put(`/forms/${formId}`, { name, description, fields })
        toast.success('Form updated')
        navigate(`/forms/${formId}`)
      } else {
        await api.post('/forms', { name, description, fields })
        toast.success('Form created')
        navigate('/forms')
      }
    } catch (err: any) {
      toast.error(err.message)
    } finally {
      setSaving(false)
    }
  }

  if (loading) {
    return (
      <div className="flex justify-center py-20">
        <Spinner className="h-8 w-8 text-blue-600" />
      </div>
    )
  }

  return (
    <div className="space-y-4">
      {/* Header bar */}
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-4">
          <button
            onClick={() => navigate(isEdit ? `/forms/${formId}` : '/forms')}
            className="flex items-center gap-1 text-sm text-gray-500 hover:text-gray-700"
          >
            <ArrowLeft size={16} /> Back
          </button>
          <h1 className="text-xl font-bold text-gray-900">
            {isEdit ? 'Edit Form' : 'Create Form'}
          </h1>
        </div>
        <Button onClick={handleSave} disabled={saving}>
          <Save size={16} className="mr-1" />
          {saving ? 'Saving...' : isEdit ? 'Save Changes' : 'Create Form'}
        </Button>
      </div>

      {/* Form name/description */}
      <div className="grid gap-4 sm:grid-cols-2">
        <Input
          label="Form Name"
          value={name}
          onChange={(e) => setName(e.target.value)}
          placeholder="e.g. Employee Onboarding"
        />
        <Input
          label="Description"
          value={description}
          onChange={(e) => setDescription(e.target.value)}
          placeholder="Optional description..."
        />
      </div>

      {/* Designer takes full remaining space */}
      <FormDesigner fields={fields} onChange={setFields} />
    </div>
  )
}
