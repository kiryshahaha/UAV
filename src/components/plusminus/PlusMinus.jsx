import React from 'react'
import Image from 'next/image'
import styles from './PlusMinus.module.css'

const PlusMinus = () => {
  return (
    <div className={styles.container}>
        <div className={styles.icon}>
            <div className={styles.imageWrapper}>
                <Image 
                    src='/svg/plus_icon.svg' 
                    fill
                    style={{objectFit: 'contain'}}
                    alt='plus-icon'
                />
            </div>
        </div>
        <div className={styles.icon}>
            <div className={styles.imageWrapper}>
                <Image 
                    src='/svg/minus_icon.svg' 
                    fill
                    style={{objectFit: 'contain'}}
                    alt='minus-icon'
                />
            </div>
        </div>
    </div>
  )
}

export default PlusMinus