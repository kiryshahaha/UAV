import React from 'react'
import Image from 'next/image'
import styles from './Icons.module.css'

const Icons = () => {
    return (
        <div className={styles.iconsContainer}>
            <div className={styles.icon}>
                <Image src='/svg/stat.svg' width={35} height={28} alt='stat-icon' />
            </div>
            <div className={styles.icon}>
                <Image src='/svg/brush.svg' width={22} height={22} alt='brush-icon' />
            </div>
        </div>
    )
}

export default Icons