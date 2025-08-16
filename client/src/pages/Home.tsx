import { useEffect, useState } from 'react'
import { useDynamicContext } from '@dynamic-labs/sdk-react-core'
import { useNavigate } from 'react-router-dom'

const Home = () => {
	const [input, setInput] = useState<string>('')
  const { user } = useDynamicContext()
  const navigate = useNavigate()

  useEffect(() => {
    if (!user) {
      navigate('/', { replace: true })
    }
  }, [user, navigate])

	const handleSend = () => {
		const trimmed = input.trim()
		if (!trimmed) return
		// TODO: wire this to your backend or MCP call
		console.log('send:', trimmed)
		setInput('')
	}

	return (
		<>

		{/* Centered input bar */}
		<div className="flex min-h-screen items-center justify-center px-4 pt-14">
			<div className="w-full max-w-3xl">
				<div className="flex items-center gap-2 rounded-full border px-4 py-2 shadow-sm">
				<input
					type="text"
					value={input}
					onChange={(e) => setInput(e.target.value)}
					onKeyDown={(e) => {
						if (e.key === 'Enter') {
							e.preventDefault()
							handleSend()
						}
					}}
					placeholder="Type a message"
					className="flex-1 bg-transparent outline-none"
				/>
				<button
					aria-label="Send"
					onClick={handleSend}
					className="rounded-full p-2 text-gray-700 hover:bg-gray-100 active:opacity-80"
				>
					<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="currentColor" className="h-5 w-5">
						<path d="M2.01 21 23 12 2.01 3 2 10l15 2-15 2z" />
					</svg>
				</button>
				</div>
			</div>
		</div>
		
		</>
	)
}

export default Home