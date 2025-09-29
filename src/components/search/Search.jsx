import React from 'react'
import styles from './search.module.css'
import Image from 'next/image'

const Search = () => {
  return (
    <div className={styles.searchContainer}>
        <Image src='/svg/search.svg' width={25} height={25} alt='search-icon'/>
        <input type="text" className={styles.searchInput}/>
    </div>
  )
}

export default Search