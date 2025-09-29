import React from 'react';
import Image from 'next/image';
import styles from './PlusMinus.module.css';

const PlusMinus = ({ mapRef }) => {
  const handleZoomIn = () => {
    if (mapRef.current) {
      mapRef.current.zoomIn();
    }
  };

  const handleZoomOut = () => {
    if (mapRef.current) {
      mapRef.current.zoomOut();
    }
  };

  return (
    <div className={styles.container}>
      <div className={styles.icon} onClick={handleZoomIn}>
        <div className={styles.imageWrapper}>
          <Image
            src="/svg/plus_icon.svg"
            fill
            style={{ objectFit: 'contain' }}
            alt="plus-icon"
          />
        </div>
      </div>
      <div className={styles.icon} onClick={handleZoomOut}>
        <div className={styles.imageWrapper}>
          <Image
            src="/svg/minus_icon.svg"
            fill
            style={{ objectFit: 'contain' }}
            alt="minus-icon"
          />
        </div>
      </div>
    </div>
  );
};

export default PlusMinus;