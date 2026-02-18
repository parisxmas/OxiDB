import { ReactNode, useEffect } from 'react'
import { X } from 'lucide-react'

interface ModalProps {
  open: boolean
  onClose: () => void
  title: string
  children: ReactNode
  wide?: boolean
  fullscreen?: boolean
}

export function Modal({ open, onClose, title, children, wide, fullscreen }: ModalProps) {
  useEffect(() => {
    if (open) {
      document.body.style.overflow = 'hidden'
    } else {
      document.body.style.overflow = ''
    }
    return () => { document.body.style.overflow = '' }
  }, [open])

  if (!open) return null

  const sizeClass = fullscreen
    ? 'inset-3 w-auto max-w-none max-h-none'
    : wide
      ? 'w-full max-w-4xl max-h-[90vh] mx-4'
      : 'w-full max-w-lg max-h-[90vh] mx-4'

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center">
      <div className="absolute inset-0 bg-black/50" onClick={onClose} />
      <div className={`${fullscreen ? 'absolute' : 'relative'} bg-white rounded-xl shadow-xl overflow-hidden flex flex-col ${sizeClass}`}>
        <div className="flex items-center justify-between border-b px-6 py-4 flex-shrink-0">
          <h2 className="text-lg font-semibold text-gray-900">{title}</h2>
          <button onClick={onClose} className="text-gray-400 hover:text-gray-600">
            <X size={20} />
          </button>
        </div>
        <div className="px-6 py-4 overflow-y-auto flex-1">{children}</div>
      </div>
    </div>
  )
}
