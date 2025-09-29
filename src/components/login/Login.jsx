import React from 'react'
import styles from './Login.module.css'

const Login = () => {
  return (
    <div className={styles.container}>
        <input type="text" className={styles.input} placeholder='Логин'/>
    </div>
  )
}

export default Login