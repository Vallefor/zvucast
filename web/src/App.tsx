import { NavLink, Navigate, Route, Routes } from 'react-router-dom'
import Nodes from './pages/Nodes'
import Zones from './pages/Zones'
import { UiProvider } from './ui'

const LINKS = [
  { to: '/nodes', label: 'Узлы' },
  { to: '/zones', label: 'Зоны' },
]

function Tabs({ className }: { className?: string }) {
  return (
    <nav className={className}>
      {LINKS.map((link) => (
        <NavLink key={link.to} to={link.to} className="tab">
          {link.label}
        </NavLink>
      ))}
    </nav>
  )
}

export default function App() {
  return (
    <UiProvider>
      <div className="shell">
        <header className="topbar">
          <div className="brand">Zvu<span>cast</span></div>
          <Tabs className="tabs" />
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
  )
}
