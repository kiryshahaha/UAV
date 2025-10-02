import React from 'react';
import styles from './Loader.module.css';

const Loader = () => {
  return (
    <div className={styles.loaderContainer}>
      <div className={styles.spinner}></div>
      <div className={styles.loadingText}>Загрузка данных...</div>
    </div>
  );
};

export default Loader;