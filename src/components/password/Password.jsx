import React from 'react'
import styles from './Password.module.css'

const Password = () => {
  return (
    <div className={styles.container}>
        <input type="text" className={styles.input} placeholder='Пароль'/>
    </div>
  )
}

export default Password