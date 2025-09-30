"use client";
import Image from "next/image";
import styles from "./page.module.css";
import Map from "@/components/map/Map";
import Icons from "@/components/IconsContainer/Icons";
import Search from "@/components/search/Search";
import PlusMinus from "@/components/plusminus/PlusMinus";
import { useRef, useState } from "react";
import ResetButton from "@/components/resetButton/ResetButton";
import Filter from "@/components/Filter/Filter";

export default function Home() {
  const mapRef = useRef(null);
  const [selectedCity, setSelectedCity] = useState(null);

  const handleCitySelect = async (cityName) => {
    setSelectedCity(cityName);
    
    try {
      const response = await fetch(`http://localhost:8000/city/${encodeURIComponent(cityName)}`);
      if (response.ok) {
        const cityData = await response.json();
        console.log('Данные города:', cityData);
        
        // Здесь можно передать данные в карту или другие компоненты
        if (mapRef.current && mapRef.current.updateCityData) {
          mapRef.current.updateCityData(cityData);
        }
      }
    } catch (error) {
      console.error('Ошибка загрузки данных города:', error);
    }
  };

  return (
    <div className={styles.container}>
      {/* Первый уровень - карта */}
      <div className={styles.Map}>
        <Map ref={mapRef} selectedCity={selectedCity} />
      </div>

      {/* Второй уровень - поиск и иконки */}
      <div className={styles.overlayContent}>
        <div className={styles.LeftSearchBar}>
          <div className={styles.searchFilter}>
            <Search onCitySelect={handleCitySelect} />
            <Filter />
          </div>
          <div>
            <ResetButton mapRef={mapRef} />
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