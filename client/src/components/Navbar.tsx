import { useEffect, useMemo, useRef, useState } from 'react'
import { Link, useLocation, useNavigate } from 'react-router-dom'
import { useDynamicContext } from '@dynamic-labs/sdk-react-core'

export default function Navbar() {
  const { primaryWallet, handleLogOut } = useDynamicContext()
  const location = useLocation()
  const navigate = useNavigate()
  const [menuOpen, setMenuOpen] = useState(false)
  const menuRef = useRef<HTMLDivElement | null>(null)

  useEffect(() => {
    function handleClickOutside(e: MouseEvent) {
      if (!menuOpen) return
      if (menuRef.current && !menuRef.current.contains(e.target as Node)) {
        setMenuOpen(false)
      }
    }

    function handleKeyDown(e: KeyboardEvent) {
      if (e.key === 'Escape') setMenuOpen(false)
    }

    document.addEventListener('mousedown', handleClickOutside)
    document.addEventListener('keydown', handleKeyDown)
    return () => {
      document.removeEventListener('mousedown', handleClickOutside)
      document.removeEventListener('keydown', handleKeyDown)
    }
  }, [menuOpen])

  const shortAddress = useMemo(() => {
    const addr = primaryWallet?.address
    if (!addr) return 'Not connected'
    if (addr.length <= 10) return addr
    return `${addr.slice(0, 6)}...${addr.slice(-4)}`
  }, [primaryWallet])

  const hasAddress = Boolean(primaryWallet?.address)

  const linkClass = (path: string) =>
    `px-3 py-2 rounded-md text-sm font-medium ${
      location.pathname === path ? 'text-[#32CD32]' : 'text-gray-600 hover:text-[#32CD32]'
    }`

  return (
    <div className="fixed inset-x-0 top-0 z-50 border-b bg-white/80 backdrop-blur supports-[backdrop-filter]:bg-white/60">
      <nav className="mx-auto flex h-14 w-full max-w-5xl items-center justify-between px-4">
        <div className="flex items-center gap-2 text-sm text-gray-800">
          {hasAddress && (
            <span className="relative inline-flex h-3.5 w-3.5 items-center justify-center">
              <span className="absolute inline-flex h-3.5 w-3.5 rounded-full bg-[#32CD32]/50 animate-ping" />
              <span className="relative inline-block h-2.5 w-2.5 rounded-full bg-[#32CD32]" />
            </span>
          )}
          <span>{shortAddress}</span>
        </div>
        <div className="flex items-center gap-1">
          <Link to="/home" className={linkClass('/home')}>Home</Link>
          <Link to="/about" className={linkClass('/about')}>About</Link>
          <Link to="/contact" className={linkClass('/contact')}>Contact</Link>
          <div className="relative ml-2" ref={menuRef}>
            <button
              onClick={() => setMenuOpen((v) => !v)}
              className="flex items-center gap-1 text-sm font-medium text-gray-700 hover:text-[#32CD32]"
              aria-haspopup="menu"
              aria-expanded={menuOpen}
            >
              <span>Account</span>
              <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 20 20" fill="currentColor" className="h-4 w-4">
                <path fillRule="evenodd" d="M5.23 7.21a.75.75 0 0 1 1.06.02L10 10.94l3.71-3.71a.75.75 0 1 1 1.06 1.06l-4.24 4.24a.75.75 0 0 1-1.06 0L5.25 8.29a.75.75 0 0 1-.02-1.08Z" clipRule="evenodd" />
              </svg>
            </button>
            {menuOpen && (
              <div className="absolute right-0 top-11 z-50 w-40 rounded-md border bg-white py-1 shadow-md">
                <button
                  className="block w-full px-3 py-2 text-left text-sm text-gray-700 hover:bg-gray-50 hover:text-[#32CD32]"
                  onClick={() => setMenuOpen(false)}
                >
                  User
                </button>
                <button
                  className="block w-full px-3 py-2 text-left text-sm text-gray-700 hover:bg-gray-50 hover:text-[#32CD32]"
                  onClick={async () => {
                    setMenuOpen(false)
                    try {
                      await handleLogOut?.()
                    } finally {
                      navigate('/', { replace: true })
                    }
                  }}
                >
                  Log out
                </button>
              </div>
            )}
          </div>
        </div>
      </nav>
    </div>
  )
}


