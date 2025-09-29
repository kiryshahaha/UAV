import React from 'react'
import AuthChoice from '../AuthChoice/AuthChoice'
import AuthLogo from '../AuthLogo/AuthLogo'
import styles from "./AuthComponent.module.css"

const AuthComponent = () => {
  return (
    <div className={styles.container}>
        <AuthLogo />
    </div>
  )
}

export default AuthComponent