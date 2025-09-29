import React from 'react'
import AuthChoice from '../AuthChoice/AuthChoice'
import AuthLogo from '../AuthLogo/AuthLogo'
import styles from "./AuthComponent.module.css"

const AuthComponent = () => {
  return (
    <div className={styles.container}>
      <div className={styles.logo}>
        <AuthLogo />
      </div>
      <div className={styles.logpas}>
        <AuthChoice />
      </div>
    </div>
  )
}

export default AuthComponent