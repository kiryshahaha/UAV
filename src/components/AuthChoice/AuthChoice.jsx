import React from 'react'
import styles from './AuthChoice.module.css'
import Login from '../login/Login'
import Password from '../password/Password'

const AuthChoice = () => {
  return (
    <div className={styles.container}>
        <div className={styles.mainText}>
            <span className={styles.text}>Вход</span>
        </div>
        <div className={styles.login}>
          <Login />
        </div>
        <div className={styles.password}>
          <Password />
        </div>
    </div>
  )
}

export default AuthChoice