import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useRef,
  useState,
  type ButtonHTMLAttributes,
  type ReactNode,
} from 'react'

type ConfirmOpts = {
  title: string
  body: string
  confirmLabel?: string
  cancelLabel?: string
  danger?: boolean
}

type DialogState = ConfirmOpts & { resolve: (ok: boolean) => void }

type ToastItem = { id: number; text: string }

type UiApi = {
  confirm: (opts: ConfirmOpts) => Promise<boolean>
  toast: (text: string) => void
}

const Ctx = createContext<UiApi | null>(null)

export function useUi(): UiApi {
  const v = useContext(Ctx)
  if (!v) throw new Error('useUi вне UiProvider')
  return v
}

/// POST/PATCH с тостом вместо alert. true — ответ 2xx.
export function useRun() {
  const { toast } = useUi()
  return useCallback(async (p: Promise<Response>, what: string) => {
    try {
      const r = await p
      if (!r.ok) {
        toast(`${what}: HTTP ${r.status}`)
        return false
      }
      return true
    } catch (e) {
      console.error(e)
      toast(`${what}: нет связи`)
      return false
    }
  }, [toast])
}

export function UiProvider({ children }: { children: ReactNode }) {
  const [dialog, setDialog] = useState<DialogState | null>(null)
  const [toasts, setToasts] = useState<ToastItem[]>([])
  const seq = useRef(0)
  const dialogRef = useRef(dialog)
  dialogRef.current = dialog

  const confirm = useCallback((opts: ConfirmOpts) => {
    return new Promise<boolean>((resolve) => {
      setDialog({ ...opts, resolve })
    })
  }, [])

  const toast = useCallback((text: string) => {
    const id = ++seq.current
    setToasts((list) => [...list, { id, text }])
    window.setTimeout(() => {
      setToasts((list) => list.filter((item) => item.id !== id))
    }, 4200)
  }, [])

  useEffect(() => {
    if (!dialog) return
    // Фаза захвата: Escape отменяет только подтверждение. Иначе всплывающий
    // слушатель Popup закроет настройки и сбросит несохранённые правки.
    const onKey = (e: KeyboardEvent) => {
      if (e.key !== 'Escape') return
      e.stopImmediatePropagation()
      dialogRef.current?.resolve(false)
      setDialog(null)
    }
    window.addEventListener('keydown', onKey, true)
    return () => window.removeEventListener('keydown', onKey, true)
  }, [dialog])

  const close = (ok: boolean) => {
    dialog?.resolve(ok)
    setDialog(null)
  }

  return (
    <Ctx.Provider value={{ confirm, toast }}>
      {children}
      {dialog && (
        <div className="backdrop" onMouseDown={() => close(false)}>
          <div
            className="dialog"
            role="dialog"
            aria-modal="true"
            aria-labelledby="dlg-title"
            onMouseDown={(e) => e.stopPropagation()}
          >
            <h2 id="dlg-title">{dialog.title}</h2>
            <p>{dialog.body}</p>
            <div className="row">
              <button type="button" className="btn btn-ghost" onClick={() => close(false)}>
                {dialog.cancelLabel ?? 'Отмена'}
              </button>
              <button
                type="button"
                className={`btn ${dialog.danger ? 'btn-danger' : 'btn-primary'}`}
                autoFocus
                onClick={() => close(true)}
              >
                {dialog.confirmLabel ?? 'Да'}
              </button>
            </div>
          </div>
        </div>
      )}
      <div className="toasts" aria-live="polite">
        {toasts.map((item) => (
          <div key={item.id} className="toast" role="status">{item.text}</div>
        ))}
      </div>
    </Ctx.Provider>
  )
}

export function Popup({
  title,
  onClose,
  wide,
  footer,
  children,
}: {
  title: string
  onClose: () => void
  wide?: boolean
  footer?: ReactNode
  children: ReactNode
}) {
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape') onClose()
    }
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
  }, [onClose])

  return (
    <div className="backdrop" onMouseDown={onClose}>
      <div
        className={`dialog${wide ? ' dialog-wide' : ''}`}
        role="dialog"
        aria-modal="true"
        aria-label={title}
        onMouseDown={(e) => e.stopPropagation()}
      >
        <h2>{title}</h2>
        <div className="dialog-body">{children}</div>
        {footer && <div className="dialog-foot">{footer}</div>}
      </div>
    </div>
  )
}

export function Button({
  variant = 'ghost',
  className,
  ...props
}: ButtonHTMLAttributes<HTMLButtonElement> & { variant?: 'primary' | 'ghost' | 'danger' }) {
  return <button {...props} className={`btn btn-${variant}${className ? ` ${className}` : ''}`} />
}

export function Badge({
  tone,
  children,
  title,
}: {
  tone?: 'master' | 'dup' | 'play' | 'steal' | 'quiet' | 'role'
  children: ReactNode
  title?: string
}) {
  return <span className={`badge${tone ? ` badge-${tone}` : ''}`} title={title}>{children}</span>
}
