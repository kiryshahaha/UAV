import React from 'react'
import styles from './Auth.module.css'

import Background from '@/app/components/background/Background'
import AuthComponent from '@/app/components/authComponent/AuthComponent'

export default function auth() {
  return (
    <div className={styles.container}>
        <div className={styles.bg}>
            <Background />
        </div>
        <div className={styles.AuthComponent}>
          <AuthComponent />
        </div>
    </div>
  )
}
