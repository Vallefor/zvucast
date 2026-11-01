import {
  createContext,
  useCallback,
  useContext,
  useState,
  type ReactNode,
} from 'react'
import {
  readLang,
  translate,
  writeLang,
  type Lang,
  type MessageKey,
  type Translate,
  type Vars,
} from './i18n.ts'

type LocaleApi = {
  lang: Lang
  setLang: (lang: Lang) => void
  t: Translate
}

const Ctx = createContext<LocaleApi | null>(null)

export function useI18n(): LocaleApi {
  const value = useContext(Ctx)
  if (!value) throw new Error('useI18n outside I18nProvider')
  return value
}

export function I18nProvider({ children }: { children: ReactNode }) {
  const [lang, setLangState] = useState<Lang>(() => {
    const initial = readLang()
    document.documentElement.lang = initial
    return initial
  })

  const setLang = useCallback((next: Lang) => {
    writeLang(next)
    document.documentElement.lang = next
    setLangState(next)
  }, [])

  const t = useCallback((key: MessageKey, vars?: Vars) => translate(lang, key, vars), [lang])

  return <Ctx.Provider value={{ lang, setLang, t }}>{children}</Ctx.Provider>
}
