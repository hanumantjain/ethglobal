import React from 'react'

type BalanceButtonProps = {
  onClick: () => void
  className?: string
  label?: string
  disabled?: boolean
}

const BalanceButton: React.FC<BalanceButtonProps> = ({ onClick, className, label = 'Balance', disabled }) => {
  return (
    <button
      onClick={onClick}
      disabled={disabled}
      className={className ?? 'rounded-full px-3 py-1 text-sm text-gray-700 hover:bg-gray-100 active:opacity-80 border'}
    >
      {label}
    </button>
  )
}

export default BalanceButton


