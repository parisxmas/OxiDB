import { create } from 'zustand'
import { api } from '../api/client'
import type { User, AuthResult } from '../api/types'

interface AuthState {
  user: User | null
  token: string | null
  loading: boolean
  login: (email: string, password: string) => Promise<void>
  register: (email: string, password: string, name: string) => Promise<void>
  logout: () => void
  loadUser: () => Promise<void>
}

export const useAuthStore = create<AuthState>((set) => ({
  user: null,
  token: localStorage.getItem('token'),
  loading: false,

  login: async (email, password) => {
    const result = await api.post<AuthResult>('/auth/login', { email, password })
    localStorage.setItem('token', result.token)
    set({ token: result.token, user: result.user })
  },

  register: async (email, password, name) => {
    const result = await api.post<AuthResult>('/auth/register', { email, password, name })
    localStorage.setItem('token', result.token)
    set({ token: result.token, user: result.user })
  },

  logout: () => {
    localStorage.removeItem('token')
    set({ token: null, user: null })
  },

  loadUser: async () => {
    try {
      set({ loading: true })
      const user = await api.get<User>('/auth/me')
      set({ user, loading: false })
    } catch {
      localStorage.removeItem('token')
      set({ token: null, user: null, loading: false })
    }
  },
}))
