import React from 'react'
import styles from './leftSearchBar.module.css'
import Search from '../search/Search'
import Filter from '../Filter/Filter'

const LeftSearchBar = () => {
  return (
    <div className={styles.leftSearchBarContainer}>
            <Search />
            <Filter />
    </div>
  )
}

export default LeftSearchBar