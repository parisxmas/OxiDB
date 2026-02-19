import { create } from 'zustand'

type Theme = 'light' | 'dark'

interface ThemeState {
  theme: Theme
  toggle: () => void
}

function applyTheme(theme: Theme) {
  if (theme === 'dark') {
    document.documentElement.classList.add('dark')
  } else {
    document.documentElement.classList.remove('dark')
  }
}

const stored = localStorage.getItem('theme') as Theme | null
const initial: Theme = stored === 'light' ? 'light' : 'dark'
applyTheme(initial)

export const useThemeStore = create<ThemeState>((set) => ({
  theme: initial,
  toggle: () => set((state) => {
    const next = state.theme === 'dark' ? 'light' : 'dark'
    localStorage.setItem('theme', next)
    applyTheme(next)
    return { theme: next }
  }),
}))
