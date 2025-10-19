import React from 'react'
import Image from 'next/image'
import styles from './Filter.module.css'

const Filter = () => {
  return (
    <div className={styles.filterContainer}>
        <Image src='/svg/Filter.svg' width={25} height={25} alt='filter-icon'/>
    </div>
  )
}

export default Filter