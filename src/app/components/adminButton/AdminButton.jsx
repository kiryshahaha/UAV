import React from 'react'
import styles from './AdminButton.module.css'

const AdminButton = () => {
  return (
    <div className={styles.container}>
        <span className={styles.text}>Администратор</span>
    </div>
  )
}

export default AdminButton