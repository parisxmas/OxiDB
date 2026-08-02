import { useEffect } from 'react'
import { Navigate } from 'react-router-dom'
import { useAuthStore } from '../../store/authStore'
import { Spinner } from '../ui/Spinner'

export function ProtectedRoute({ children }: { children: React.ReactNode }) {
  const { token, user, loading, loadUser } = useAuthStore()

  useEffect(() => {
    if (token && !user) {
      loadUser()
    }
  }, [token, user, loadUser])

  if (!token) return <Navigate to="/login" replace />
  if (loading || !user) {
    return (
      <div className="flex h-screen items-center justify-center">
        <Spinner className="h-8 w-8 text-blue-600" />
      </div>
    )
  }
  return <>{children}</>
}
