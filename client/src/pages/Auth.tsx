import { DynamicWidget, useDynamicContext } from '@dynamic-labs/sdk-react-core'
import { useEffect } from 'react'
import { useNavigate } from 'react-router-dom'
  
const Auth = () => {
  const { user } = useDynamicContext()
  const navigate = useNavigate()

  useEffect(() => {
    if (user) {
      navigate('/home', { replace: true })
    }
  }, [user, navigate])

  return (
    <div className='flex justify-center items-center h-screen'>
          <DynamicWidget />
    </div>
  )
}

export default Auth