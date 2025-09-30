"use client";

import React, { useRef, useEffect, forwardRef } from "react";
import "leaflet/dist/leaflet.css";
import regionData from "public/geoData/RussiaWhole.json";
import styles from "./map.module.css";
import {
  MapContainer,
  TileLayer,
  GeoJSON,
} from "@/components/leaflet/leaFletNoSSR.js";

const Map = forwardRef((props, ref) => {
  const mapRef = useRef(null);

  const CONFIG = {
    center: [55.7522, 37.6156], // Москва
    zoom: 5,
    minZoom: 2,
    maxZoom: 16,
    ZoomControl: false,
    regionStyle: {
      color: "#424d5b3d",
      fillColor: "#22222204",
      weight: 2,
      fillOpacity: 0.3,
    },
    hoverStyle: { fillColor: "#aeff34ff" },
  };

  const onEachRegion = (feature, layer) => {
    if (feature.properties?.REGION_NAME) {
      layer.bindPopup(feature.properties.REGION_NAME);
    }
    layer.on({
      mouseover: () => layer.setStyle(CONFIG.hoverStyle),
      mouseout: () => layer.setStyle(CONFIG.regionStyle),
      click: () =>
        mapRef.current?.fitBounds(layer.getBounds(), {
          padding: [50, 50],
          animate: true,
        }),
    });
  };

  useEffect(() => {
    const map = mapRef.current;
    if (!map) return;
    const L = require("leaflet");

    L.Control.Reset = L.Control.extend({
      options: { position: "topright" },
      onAdd: () => {
        const button = L.DomUtil.create("button", "reset-btn");
        button.innerHTML = "Сброс";
        Object.assign(button.style, {
          background: "#fff",
          padding: "5px 10px",
          cursor: "pointer",
        });
        button.onclick = () => map.setView(CONFIG.center, CONFIG.zoom);
        return button;
      },
    });

    const resetControl = new L.Control.Reset();
    resetControl.addTo(map);

    return () => resetControl.remove();
  }, []);

  return (
    <MapContainer
      center={CONFIG.center}
      zoom={CONFIG.zoom}
      zoomControl={CONFIG.ZoomControl}
      minZoom={CONFIG.minZoom}
      maxZoom={CONFIG.maxZoom}
      className={styles.mapContainer}
      attributionControl={false}
      preferCanvas
      maxBounds={[
        [-90, -180],
        [90, 190],
      ]}
      maxBoundsViscosity={0.95}
      worldCopyJump={false}
      ref={(node) => {
        mapRef.current = node;
        if (ref) {
          ref.current = node;
        }
      }}
    >
      <TileLayer
        url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
        attribution="&copy; OpenStreetMap contributors"
        keepBuffer={10}
        reuseTiles
        updateWhenIdle={false}
      />
      <GeoJSON
        data={regionData}
        style={CONFIG.regionStyle}
        onEachFeature={onEachRegion}
      />
    </MapContainer>
  );
});

Map.displayName = "Map";

export default Map;