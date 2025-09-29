import React from 'react'
import Image from 'next/image'
import styles from './Icons.module.css'

const Icons = () => {
    return (
        <div className={styles.iconsContainer}>
            <div className={styles.icon}>
                {/*иконка для загрузки файла*/}
                <div className={styles.imageWrapper}>
                    <Image 
                        src='/svg/Load.svg' 
                        fill
                        style={{objectFit: 'contain'}}
                        alt='download-icon' 
                    />
                </div>
            </div>
            <div className={styles.icon}>
                <div className={styles.imageWrapper}>
                    <Image 
                        src='/svg/stat.svg' 
                        fill
                        style={{objectFit: 'contain'}}
                        alt='stat-icon' 
                    />
                </div>
            </div>
            <div className={styles.icon}>
                <div className={styles.imageWrapper}>
                    <Image 
                        src='/svg/brush.svg' 
                        fill
                        style={{objectFit: 'contain'}}
                        alt='brush-icon' 
                    />
                </div>
            </div>
        </div>
    )
}

export default Icons;