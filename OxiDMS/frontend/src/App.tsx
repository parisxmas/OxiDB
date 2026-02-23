import { Routes, Route } from 'react-router-dom'
import { AppLayout } from './components/layout/AppLayout'
import { ProtectedRoute } from './components/auth/ProtectedRoute'
import { LoginPage } from './pages/LoginPage'
import { DashboardPage } from './pages/DashboardPage'
import { FormsPage } from './pages/FormsPage'
import { FormDetailPage } from './pages/FormDetailPage'
import { DocumentsPage } from './pages/DocumentsPage'
import { SearchPage } from './pages/SearchPage'
import { FormDesignerPage } from './pages/FormDesignerPage'
import { NewSubmissionPage } from './pages/NewSubmissionPage'
import { SubmissionViewPage } from './pages/SubmissionViewPage'
import { EditSubmissionPage } from './pages/EditSubmissionPage'

export default function App() {
  return (
    <Routes>
      <Route path="/login" element={<LoginPage />} />
      <Route
        element={
          <ProtectedRoute>
            <AppLayout />
          </ProtectedRoute>
        }
      >
        <Route path="/" element={<DashboardPage />} />
        <Route path="/forms" element={<FormsPage />} />
        <Route path="/forms/new" element={<FormDesignerPage />} />
        <Route path="/forms/:formId" element={<FormDetailPage />} />
        <Route path="/forms/:formId/edit" element={<FormDesignerPage />} />
        <Route path="/forms/:formId/submit" element={<NewSubmissionPage />} />
        <Route path="/forms/:formId/submissions/:submissionId" element={<SubmissionViewPage />} />
        <Route path="/forms/:formId/submissions/:submissionId/edit" element={<EditSubmissionPage />} />
        <Route path="/documents" element={<DocumentsPage />} />
        <Route path="/search" element={<SearchPage />} />
      </Route>
    </Routes>
  )
}
