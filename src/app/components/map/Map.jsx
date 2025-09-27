"use client";
import React, { useEffect, useRef } from "react";
import styles from "./map.module.css";
import regionData from "./../../../../public/geoData/gadm41_RUS_1.json";

export default function Map() {
  const mapRef = useRef(null);

  useEffect(() => {
    if (typeof window === "undefined" || mapRef.current) return;

    const L = require("leaflet");

    // Константы
    const CENTER = [55.7522, 37.6156]; // Москва
    const ZOOM = 5;
    const MIN_ZOOM = 2;
    const MAX_ZOOM = 15;

    const REGION_STYLE = {
      color: "#ffffff",
      fillColor: "#22222204",
      weight: 2,
      fillOpacity: 0.3,
    };

    const REGION_HOVER_STYLE = { fillColor: "yellow" };

    // Создаем карту
    const map = L.map("map", {
      center: CENTER,
      zoom: ZOOM,
      minZoom: MIN_ZOOM,
      maxZoom: MAX_ZOOM,
      attributionControl: false,
      maxBounds: L.latLngBounds(L.latLng(-90, -180), L.latLng(90, 180)),
      maxBoundsViscosity: 0.9,
      worldCopyJump: false,
    });

    mapRef.current = map;
    map.locate({ setView: true, maxZoom: 6 });

    // Слой OSM
    L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
      attribution: "&copy; OpenStreetMap contributors",
      keepBuffer: 10,
      reuseTiles: true,
      updateWhenIdle: false,
    }).addTo(map);

    // Функции для событий
    const onEachRegion = (feature, layer) => {
      if (feature.properties?.NAME_1) {
        layer.bindPopup(`Регион: ${feature.properties.NAME_1}`);
      }

      layer.on("mouseover", () => layer.setStyle(REGION_HOVER_STYLE));
      layer.on("mouseout", () => layer.setStyle(REGION_STYLE));
      layer.on("click", () =>
        map.fitBounds(layer.getBounds(), { padding: [50, 50], animate: true })
      );
    };

    // Слой регионов
    const geoLayer = L.geoJSON(regionData, {
      style: () => REGION_STYLE,
      onEachFeature: onEachRegion,
    }).addTo(map);

    // Кнопка сброса
    const resetControl = L.control({ position: "topright" });
    resetControl.onAdd = () => {
      const button = L.DomUtil.create("button", "reset-btn");
      button.innerHTML = "Сброс";
      Object.assign(button.style, {
        background: "#fff",
        padding: "5px 10px",
        cursor: "pointer",
      });
      button.onclick = () => map.setView(CENTER);
      return button;
    };

    resetControl.addTo(map);

    return () => {
      map.remove();
      mapRef.current = null;
    };
  }, []);

  return <div id="map" className={styles.mapContainer}></div>;
}
