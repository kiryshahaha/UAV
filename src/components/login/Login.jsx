import React from 'react'
import styles from './Login.module.css'

const Login = ({ value, onChange, onKeyPress }) => {
  return (
    <div className={styles.container}>
        <input 
        type="text" 
        className={styles.input} 
        placeholder='Логин'
        onChange={onChange}
        onKeyPress={onKeyPress}
        />
    </div>
  )
}

export default Login