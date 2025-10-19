import React from "react";
import styles from "./ResetButton.module.css";
import Image from "next/image";

const ResetButton = ({ mapRef }) => {
  const handleReset = () => {
    if (mapRef.current) {
      mapRef.current.setView([55.7522, 37.6156], 5);
    }
  };

  return (
    <div className={styles.icon} onClick={handleReset}>
        <div className={styles.imageWrapper}>
          <Image
            src="/svg/carrier.svg"
            fill
            style={{ objectFit: 'contain' }}
            alt="plus-icon"
          />
        </div>
      </div>
  );
};

export default ResetButton;
