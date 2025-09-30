import React from "react";
import styles from "./ResetButton.module.css";

const ResetButton = ({ mapRef }) => {
  const handleReset = () => {
    if (mapRef.current) {
      mapRef.current.setView([55.7522, 37.6156], 5);
    }
  };
  return (
    <div className={styles.container} onClick={handleReset}>
      <span className={styles.text}>сброс</span>
    </div>
  );
};

export default ResetButton;
