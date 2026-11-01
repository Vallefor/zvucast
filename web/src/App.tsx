import { NavLink, Navigate, Route, Routes } from 'react-router-dom'
import { I18nProvider, useI18n } from './locale'
import Nodes from './pages/Nodes'
import Zones from './pages/Zones'
import { UiProvider } from './ui'

const LINKS = [
  { to: '/nodes', key: 'nav.nodes' },
  { to: '/zones', key: 'nav.zones' },
] as const

function Tabs({ className }: { className?: string }) {
  const { t } = useI18n()
  return (
    <nav className={className}>
      {LINKS.map((link) => (
        <NavLink key={link.to} to={link.to} className="tab">
          {t(link.key)}
        </NavLink>
      ))}
    </nav>
  )
}

function LangSwitch() {
  const { lang, setLang, t } = useI18n()
  return (
    <div className="lang" role="group" aria-label={t('lang.group')}>
      <button type="button" lang="en" aria-pressed={lang === 'en'} onClick={() => setLang('en')}>EN</button>
      <button type="button" lang="ru" aria-pressed={lang === 'ru'} onClick={() => setLang('ru')}>RU</button>
    </div>
  )
}

export default function App() {
  return (
    <I18nProvider>
      <UiProvider>
        <div className="shell">
          <header className="topbar">
            <div className="brand">Zvu<span>cast</span></div>
            <Tabs className="tabs" />
            <LangSwitch />
          </header>
          <Routes>
            <Route path="/" element={<Navigate to="/nodes" replace />} />
            <Route path="/nodes" element={<Nodes />} />
            <Route path="/zones" element={<Zones />} />
            <Route path="*" element={<Navigate to="/nodes" replace />} />
          </Routes>
          <Tabs className="tabbar" />
        </div>
      </UiProvider>
    </I18nProvider>
  )
}
