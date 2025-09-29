import React from "react";
import styles from "./leftSearchBar.module.css";
import Search from "../search/Search";
import Filter from "../Filter/Filter";
import ResetButton from "../ResetButton/ResetButton";

const LeftSearchBar = () => {
  return (
    <div className={styles.leftSearchBarContainer}>
      <div className={styles.leftSearchBar}>
        <div className={styles.searchContainer}>
          <Search />
        </div>
      </div>
    </div>
  );
};

export default LeftSearchBar;
