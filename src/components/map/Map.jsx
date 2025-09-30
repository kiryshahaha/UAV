"use client";

import React, { useRef, forwardRef, useState, useEffect, useMemo } from "react";
import "leaflet/dist/leaflet.css";
import "leaflet.markercluster/dist/MarkerCluster.css";
import "leaflet.markercluster/dist/MarkerCluster.Default.css";

import regionData from "public/geoData/RussiaWhole.json";
import styles from "./map.module.css";

import {
  Tooltip,
  MapContainer,
  TileLayer,
  GeoJSON,
  Marker,
  MarkerClusterGroup,
} from "@/components/leaflet/leaFletNoSSR.js";

const Map = forwardRef((props, ref) => {
  const mapRef = useRef(null);
  const markerClusterRef = useRef(null);

  const [droneIcon, setDroneIcon] = useState(null);
  const [rawDrones, setRawDrones] = useState([]);
  const [loading, setLoading] = useState(true);

  // Создаем иконку дрона
  useEffect(() => {
    import("leaflet").then((L) => {
      setDroneIcon(
        new L.Icon({
          iconUrl: "/svg/drone.svg",
          iconSize: [24, 24],
          iconAnchor: [12, 12],
          popupAnchor: [0, -12],
          className: "drone-icon",
        })
      );
    });
  }, []);

  // Загрузка дронов
  useEffect(() => {
    async function fetchDrones() {
      try {
        const res = await fetch("http://localhost:8000/flights/points");
        if (!res.ok) throw new Error(`Ошибка: ${res.status}`);
        const data = await res.json();
        setRawDrones(data);
      } catch (err) {
        console.error("❌ Ошибка загрузки дронов:", err);
      } finally {
        setLoading(false);
      }
    }
    fetchDrones();
  }, []);

  // Мемоизация обработанных данных, уникальных по ID
  const drones = useMemo(() => {
    const seen = new Set();
    return rawDrones
      .map((d) => {
        const coords =
          d.latitude && d.longitude
            ? { lat: d.latitude, lng: d.longitude }
            : null;
        if (!coords || seen.has(d.id)) return null;
        seen.add(d.id);
        return { id: d.id, ...coords };
      })
      .filter(Boolean);
  }, [rawDrones]);

  const CONFIG = {
    center: [55.7522, 37.6156],
    zoom: 6,
    minZoom: 2,
    maxZoom: 18,
    ZoomControl: false,
    regionStyle: {
      color: "#424d5b3d",
      fillColor: "#22222204",
      weight: 2,
      fillOpacity: 0.3,
    },
    hoverStyle: { fillColor: "#ff343444" },
  };

  const onEachRegion = (feature, layer) => {
    if (feature.properties?.REGION_NAME)
      layer.bindPopup(`Регион|область: ${feature.properties.REGION_NAME}`);
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

  return (
    <div
      className={styles.mapWrapper}
      style={{ position: "relative", width: "100%", height: "100%" }}
    >
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
          if (ref) ref.current = node;
        }}
      >
        <TileLayer
          url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
          attribution="&copy; OpenStreetMap contributors"
        />

        <GeoJSON
          data={regionData}
          style={CONFIG.regionStyle}
          onEachFeature={onEachRegion}
        />

        {droneIcon && drones.length > 0 && (
          <MarkerClusterGroup
            ref={markerClusterRef}
            chunkedLoading
            maxClusterRadius={100}
            spiderfyOnMaxZoom
            showCoverageOnHover
            zoomToBoundsOnClick
            disableClusteringAtZoom={16}
            spiderLegPolylineOptions={{
              weight: 1.5,
              color: "#222",
              opacity: 0.5,
            }}
          >
            {drones.map((d) => (
              <Marker key={d.id} position={[d.lat, d.lng]} icon={droneIcon}>
                <Tooltip
                  direction="top"
                  offset={[0, -10]}
                  opacity={1}
                  permanent={false}
                >
                  ID: {d.id}
                </Tooltip>
              </Marker>
            ))}
          </MarkerClusterGroup>
        )}
      </MapContainer>
    </div>
  );
});

Map.displayName = "Map";
export default Map;
