import React from 'react'
import styles from './leftSearchBar.module.css'
import Search from '../search/Search'

const LeftSearchBar = () => {
  return (
    <div className={styles.leftSearchBarContainer}>
        <div>
            <Search />
        </div>
    </div>
  )
}

export default LeftSearchBar