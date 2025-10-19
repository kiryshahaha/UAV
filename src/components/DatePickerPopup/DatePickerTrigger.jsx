// components/DatePickerPopup/DatePickerTrigger.jsx
"use client";

import Image from "next/image";
import styles from "./DatePickerPopup.module.css";

export const DatePickerTrigger = ({ onOpen, selectedDate }) => {
  return (
    <button
      className={styles.triggerButton}
      onClick={onOpen}
      type="button"
    >
      <div className={styles.imageWrapper}>
        <Image
          src="/svg/calendar.svg"
          fill
          style={{ objectFit: "contain", cursor: "pointer" }}
          alt="calendar-icon"
        />
      </div>
    </button>
  );
};

export default DatePickerTrigger;