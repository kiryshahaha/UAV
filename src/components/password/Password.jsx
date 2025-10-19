import React from 'react'
import styles from './Password.module.css'

const Password = ({ value, onChange, onKeyPress }) => {
  return (
    <div className={styles.container}>
        <input 
          type="password" 
          className={styles.input} 
          placeholder='Пароль'
          value={value}
          onChange={onChange}
          onKeyPress={onKeyPress}
        />
    </div>
  )
}

export default Password