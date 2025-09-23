"use client";
import React, { useEffect, useRef } from "react";
import styles from "./map.module.css";

export default function Map() {
  const mapRef = useRef(null);

  useEffect(() => {
    if (typeof window === "undefined" || mapRef.current) return;

    const L = require("leaflet");

    const southWest = L.latLng(-90, -160);
    const northEast = L.latLng(90, 360);
    const largeBounds = L.latLngBounds(southWest, northEast);

    const map = L.map("map", {
      center: [55.7522, 37.6156],
      zoom: 5,
      minZoom: 3,
      maxZoom: 15,
      attributionControl: false,
      preferCanvas: true,
      maxBounds: largeBounds,
      maxBoundsViscosity: 0.9,
    });

    mapRef.current = map;

    L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
      attribution: "&copy; OpenStreetMap contributors",
    }).addTo(map);

    return () => {
      map.remove();
      mapRef.current = null;
    };
  }, []);

  return <div id="map" className={styles.mapContainer}></div>;
}
