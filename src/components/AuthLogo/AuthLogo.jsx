import React from 'react'
import Image from 'next/image'
import styles from './AuthLogo.module.css'

const AuthLogo = () => {
  return (
    <div className={styles.container}>
        <div className={styles.logo}>
            <Image src='/svg/LogoUAV.svg' height={120} width={112.5} alt='logo'/>
        </div>
        <div className={styles.textContainer}>
            <span className={styles.text}>UAVision</span>
        </div>
    </div>
  )
}

export default AuthLogo