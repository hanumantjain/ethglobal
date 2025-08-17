import { useEffect, useRef, useState } from 'react'
import { useDynamicContext } from '@dynamic-labs/sdk-react-core'
import { isEthereumWallet } from '@dynamic-labs/ethereum'
import { isAddress, parseEther } from 'viem'
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

	// Simple state machine for send/transfer flows
	const [awaitingField, setAwaitingField] = useState<'receiver' | 'amount' | null>(null)
	const [pendingReceiver, setPendingReceiver] = useState<string>('')

  const handleShowBalance = (text: string) => {
    setMessages((prev) => [...prev, { role: 'assistant', content: text }])
  }

	const stripCodeFences = (text: string): string => {
		const fenceMatch = text.match(/```[a-zA-Z]*\s*([\s\S]*?)\s*```/)
		if (fenceMatch && fenceMatch[1]) return fenceMatch[1].trim()
		return text.trim()
	}

	const extractJsonCandidate = (text: string): string | null => {
		const stripped = stripCodeFences(text)
		try {
			JSON.parse(stripped)
			return stripped
		} catch {}
		const start = stripped.indexOf('{')
		const end = stripped.lastIndexOf('}')
		if (start !== -1 && end !== -1 && end > start) {
			return stripped.slice(start, end + 1).trim()
		}
		return null
	}

	const tryParseJson = (value: unknown): any | null => {
		if (typeof value !== 'string') return null
		const candidate = extractJsonCandidate(value)
		if (!candidate) return null
		try {
			return JSON.parse(candidate)
		} catch {
			return null
		}
	}

	const performCheckBalance = async () => {
		try {
			if (!primaryWallet || !(primaryWallet as any).getBalance) {
				handleShowBalance('Balance unavailable')
				return
			}
			const balance: any = await (primaryWallet as any).getBalance()
			const symbol = typeof balance?.symbol === 'string' ? balance.symbol : 'ETH'
			const value =
				(typeof balance?.formatted === 'string' && balance.formatted) ||
				(typeof balance?.balance === 'string' && balance.balance) ||
				(typeof balance?.value === 'string' && balance.value) ||
				(String(balance))
			handleShowBalance(`${value} ${symbol}`)
		} catch (e) {
			console.error('Failed to fetch wallet balance via primaryWallet:', e)
			handleShowBalance('Balance unavailable')
		}
	}

	const performSendTransaction = async (toAddress: string, amountEth: string) => {
		try {
			if (!primaryWallet || !isEthereumWallet(primaryWallet)) {
				setMessages((prev) => [
					...prev,
					{ role: 'assistant', content: 'Wallet not ready for sending transactions.' },
				])
				return
			}
			const walletClient = await (primaryWallet as any).getWalletClient()
			const txHash = await walletClient.sendTransaction({
				to: toAddress as `0x${string}`,
				value: parseEther(String(amountEth)),
			})
			setMessages((prev) => [
				...prev,
				{ role: 'assistant', content: `Sent ${amountEth} ETH to ${toAddress}. Tx: ${txHash}` },
			])
		} catch (e: any) {
			console.error('Failed to send transaction:', e)
			setMessages((prev) => [
				...prev,
				{ role: 'assistant', content: 'Failed to send transaction. Please try again.' },
			])
		}
	}

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
		// Add user's message
		setMessages((prev) => [...prev, { role: 'user', content: trimmed }])
		setInput('')

		// If we are in the middle of a send/transfer flow, handle locally and short-circuit
		if (awaitingField) {
			if (awaitingField === 'receiver') {
				const candidate = trimmed
				if (!isAddress(candidate)) {
					setMessages((prev) => [
						...prev,
						{ role: 'assistant', content: 'That does not look like a valid EVM address. Please enter a valid 0x... address.' },
					])
					return
				}
				setPendingReceiver(candidate)
				setAwaitingField('amount')
				setMessages((prev) => [
					...prev,
					{ role: 'assistant', content: 'How much would you like to send? Enter the amount in ETH (e.g. 0.05).' },
				])
				return
			}
			if (awaitingField === 'amount') {
				const candidateAmount = trimmed
				const numeric = Number(candidateAmount)
				if (!isFinite(numeric) || numeric <= 0) {
					setMessages((prev) => [
						...prev,
						{ role: 'assistant', content: 'Please enter a valid positive number for the amount in ETH.' },
					])
					return
				}
				const toAddress = pendingReceiver
				const amountEth = candidateAmount
				// reset state before attempting send
				setAwaitingField(null)
				setPendingReceiver('')
				await performSendTransaction(toAddress, amountEth)
				return
			}
		}

		// Otherwise, proceed to call the webhook
		// If user typed only a valid wallet address (not in a flow), start amount prompt locally
		if (!awaitingField && isAddress(trimmed)) {
			setPendingReceiver(trimmed)
			setAwaitingField('amount')
			setMessages((prev) => [
				...prev,
				{ role: 'assistant', content: 'How much would you like to send? Enter the amount in ETH (e.g. 0.05).' },
			])
			return
		}

		setIsLoading(true)
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
			// Detect and execute wallet action if present (only check_balance for now)
			let actionPayload: any | null = null
			if (data && typeof data === 'object' && 'action' in (data as Record<string, unknown>)) {
				actionPayload = data
			} else {
				const parsed = tryParseJson(extracted)
				if (parsed && typeof parsed === 'object' && 'action' in parsed) {
					actionPayload = parsed
				}
			}
			if (actionPayload && typeof actionPayload === 'object') {
				const action = String((actionPayload as any).action || '')
				if (action === 'check_balance') {
					await performCheckBalance()
				} else if (/(send|transfer)/i.test(action)) {
					// Initiate send flow: ask for receiver then amount
					setAwaitingField('receiver')
					setPendingReceiver('')
					setMessages((prev) => [
						...prev,
						{ role: 'assistant', content: 'Please provide the recipient wallet address (0x...).' },
					])
				} else {
					setMessages((prev) => [...prev, { role: 'assistant', content: extracted || 'No response received.' }])
				}
			} else {
				setMessages((prev) => [...prev, { role: 'assistant', content: extracted || 'No response received.' }])
			}
		} catch (error) {
			console.error('Error sending to webhook:', error)
			setMessages((prev) => [...prev, { role: 'assistant', content: 'Sorry, there was an error contacting the server.' }])
		} finally {
			setIsLoading(false)
		}
	}

	return (
		<>
		<div className="flex h-screen flex-col pt-20 overflow-hidden">
			{/* Parent wrapper containing message area and input area */}
			<div className="mx-auto w-full max-w-3xl flex flex-col flex-1 overflow-hidden">
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