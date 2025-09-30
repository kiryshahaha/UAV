"use client";
import Image from "next/image";
import styles from "./page.module.css";
import Map from "@/components/map/Map";
import Icons from "@/components/IconsContainer/Icons";
import Search from "@/components/search/Search";
import PlusMinus from "@/components/plusminus/PlusMinus";
import { useRef } from "react";
import ResetButton from "@/components/resetButton/ResetButton";
import Filter from "@/components/Filter/Filter";

export default function Home() {
  const mapRef = useRef(null);

  return (
    <div className={styles.container}>
      {/* Первый уровень - карта */}
      <div className={styles.Map}>
        <Map ref={mapRef} />
      </div>

      {/* Второй уровень - поиск и иконки */}
      <div className={styles.overlayContent}>
        <div className={styles.LeftSearchBar}>
          <div className={styles.searchFilter}>
          <Search />
          <Filter />
          </div>
          <div>
          <ResetButton mapRef={mapRef}></ResetButton>
          </div>
        </div>
        <div className={styles.iconsContainer}>
          <div className={styles.icons}>
            <Icons />
          </div>
          <div className={styles.icons}>
            <PlusMinus mapRef={mapRef} />
          </div>
        </div>
      </div>
    </div>
  );
}
