import { useEffect, useRef, useState } from 'react'
import { useDynamicContext } from '@dynamic-labs/sdk-react-core'
import { useNavigate } from 'react-router-dom'
const WEBHOOK_URL = import.meta.env.VITE_N8N_WORKFLOW_OPENSEA_MPC

const Home = () => {
	const [input, setInput] = useState<string>('')
	const [messages, setMessages] = useState<Array<{ role: 'user' | 'assistant'; content: string }>>([])
	const [isLoading, setIsLoading] = useState<boolean>(false)
  const { user, primaryWallet } = useDynamicContext()
  const navigate = useNavigate()
  const messagesEndRef = useRef<HTMLDivElement | null>(null)
  const sessionIdRef = useRef<string>('')
  const walletAddress = primaryWallet?.address;

  useEffect(() => {
    if (!user) {
      navigate('/', { replace: true })
    }
  }, [user, navigate])

  useEffect(() => {
    // Keep the latest message in view
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth', block: 'end' })
  }, [messages, isLoading])

  useEffect(() => {
    // Initialize a per-tab session id
    try {
      const existing = sessionStorage.getItem('chatSessionId')
      if (existing) {
        sessionIdRef.current = existing
      } else {
        const generated = (globalThis as any).crypto?.randomUUID?.() ?? Math.random().toString(36).slice(2)
        sessionStorage.setItem('chatSessionId', generated)
        sessionIdRef.current = generated
      }
    } catch {
      // Fallback if sessionStorage is unavailable
      sessionIdRef.current = Math.random().toString(36).slice(2)
    }
  }, [])

	const handleSend = async () => {
		const trimmed = input.trim()
		if (!trimmed) return
		// Add user's message and start loading
		setMessages((prev) => [...prev, { role: 'user', content: trimmed }])
		setIsLoading(true)
		setInput('')
		try {
			const response = await fetch(WEBHOOK_URL, {
				method: 'POST',
				headers: {
					'Content-Type': 'application/json',
					'X-Session-Id': sessionIdRef.current,
				},
				body: JSON.stringify({ message: trimmed, sessionId: sessionIdRef.current, walletAddress }),
			})
			if (!response.ok) {
				console.error('Webhook request failed:', response.status, response.statusText)
				setMessages((prev) => [...prev, { role: 'assistant', content: 'Sorry, something went wrong while fetching the data.' }])
				return
			}
			const contentType = response.headers.get('content-type') ?? ''
			const rawText = await response.text()
			let data: unknown = null
			if (rawText && rawText.trim() !== '') {
				if (contentType.includes('application/json')) {
					try {
						data = JSON.parse(rawText)
					} catch {
						data = rawText
					}
				} else {
					data = rawText
				}
			}
			console.log('webhook content-type:', contentType)
			let extracted = ''
			if (Array.isArray(data) && data.length > 0) {
				const first = (data as Array<unknown>)[0] as Record<string, unknown>
				if (first && typeof first === 'object' && 'output' in first) {
					extracted = String((first as any).output ?? '')
				}
			} else if (data && typeof data === 'object' && 'output' in (data as Record<string, unknown>)) {
				extracted = String((data as any).output ?? '')
			} else if (typeof data === 'string') {
				extracted = data
			}
			setMessages((prev) => [...prev, { role: 'assistant', content: extracted || 'No response received.' }])
		} catch (error) {
			console.error('Error sending to webhook:', error)
			setMessages((prev) => [...prev, { role: 'assistant', content: 'Sorry, there was an error contacting the server.' }])
		} finally {
			setIsLoading(false)
		}
	}

	return (
		<>
		<div className="flex min-h-screen flex-col pt-20">
			{/* Parent wrapper containing message area and input area */}
			<div className="mx-auto w-full max-w-3xl flex flex-col flex-1">
				{/* Message area */}
				<div className="flex-1 overflow-y-auto px-4 py-4">
					<div className="space-y-3">
						{messages.map((msg, idx) => (
							msg.role === 'user' ? (
								<div key={idx} className="flex justify-end">
									<div className="max-w-[80%] rounded-2xl bg-blue-600 px-4 py-2 text-sm text-white">
										{msg.content}
									</div>
								</div>
							) : (
								<div key={idx} className="flex justify-start">
									<div className="max-w-full px-4 py-2 text-sm space-y-2">
										{msg.content
											.replace(/\r/g, '')
											.split(/\n+/)
											.map((line) => line
												.replace(/^[*-]\s+/, '')
												.replace(/\*{1,2}/g, '')
												.trim()
											)
											.filter(Boolean)
											.map((para, pIdx) => (
												<p key={pIdx}>{para}</p>
											))}
									</div>
								</div>
							)
						))}
						{isLoading && (
							<div className="flex justify-start">
								<div className="max-w-[80%] rounded-2xl border px-4 py-2 text-sm text-gray-600">
									Loading...
								</div>
							</div>
						)}
						<div ref={messagesEndRef} />
					</div>
				</div>

				{/* Input area */}
				<div className="bg-white px-4 py-2 pb-5">
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
		</div>
		
		</>
	)
}

export default Home