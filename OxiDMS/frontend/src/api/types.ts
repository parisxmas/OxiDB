export interface User {
  id: string
  email: string
  name: string
  role: string
  createdAt: string
}

export interface AuthResult {
  token: string
  user: User
}

export interface FieldDefinition {
  name: string
  label: string
  type: 'text' | 'textarea' | 'number' | 'date' | 'select' | 'checkbox' | 'radio' | 'file' | 'email'
  required?: boolean
  placeholder?: string
  options?: string[]
  indexed?: boolean
  minLength?: number
  maxLength?: number
  min?: number
  max?: number
  x?: number
  y?: number
  w?: number
  h?: number
}

export interface Form {
  _id: string
  name: string
  slug: string
  description?: string
  fields: FieldDefinition[]
  createdBy: string
  createdAt: string
  updatedAt: string
}

export interface Submission {
  _id: string
  formId: string
  data: Record<string, unknown>
  files?: string[]
  createdBy: string
  createdAt: string
  updatedAt: string
}

export interface Document {
  _id: string
  fileName: string
  contentType: string
  size: number
  blobKey: string
  formId?: string
  submissionId?: string
  uploadedBy: string
  createdAt: string
}

export interface DashboardData {
  formCount: number
  submissionCount: number
  documentCount: number
  forms: {
    id: string
    name: string
    slug: string
    submissionCount: number
    fieldCount: number
    createdAt: string
  }[]
}

export interface PaginatedSubmissions {
  submissions: Submission[]
  total: number
  skip: number
  limit: number
}

export interface PaginatedDocuments {
  docs: Document[]
  total: number
  skip: number
  limit: number
}

export interface SearchResult {
  docs: Record<string, unknown>[]
  total: number
  mode: string
}
