"use client";

import React, { useRef, forwardRef, useState, useEffect, useMemo, useCallback } from "react";
import "leaflet/dist/leaflet.css";
import "leaflet.markercluster/dist/MarkerCluster.css";
import "leaflet.markercluster/dist/MarkerCluster.Default.css";

import regionData from "public/geoData/RussiaWhole.json";
import styles from "./map.module.css";

import {
  MapContainer,
  TileLayer,
  GeoJSON,
  MarkerClusterGroup,
} from "@/components/leaflet/leaFletNoSSR.js";

// Импортируем вынесенные компоненты
import DronePopup from "./DronePopup";
import DroneMarker from "./DroneMarker";

const Map = forwardRef((props, ref) => {
  const { tileUrl, onTileUrlChange } = props;

  const mapRef = useRef(null);
  const markerClusterRef = useRef(null);

  const [droneIcon, setDroneIcon] = useState(null);
  const [rawDrones, setRawDrones] = useState([]);
  const [loading, setLoading] = useState(true);
  
  // Состояние для кастомного попапа
  const [selectedDrone, setSelectedDrone] = useState(null);
  const [popupPosition, setPopupPosition] = useState({ x: 0, y: 0 });

  // Мемоизируем конфиг
  const CONFIG = useMemo(() => ({
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
  }), []);

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

  // Мемоизируем обработчики
  const onEachRegion = useCallback((feature, layer) => {
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
  }, [CONFIG.hoverStyle, CONFIG.regionStyle]);

  const handleDroneClick = useCallback((drone, event) => {
    const mapContainer = event.target._map.getContainer();
    const rect = mapContainer.getBoundingClientRect();
    
    const clickX = event.originalEvent.clientX - rect.left;
    const clickY = event.originalEvent.clientY - rect.top;
    
    setPopupPosition({ x: clickX, y: clickY });
    setSelectedDrone(drone);
  }, []);

  const handleClosePopup = useCallback(() => {
    setSelectedDrone(null);
  }, []);

  // Мемоизируем кластерную группу дронов
  const droneClusterGroup = useMemo(() => {
    if (!droneIcon || drones.length === 0) return null;

    return (
      <MarkerClusterGroup
        key="drone-cluster"
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
        {drones.map((drone) => (
          <DroneMarker
            key={drone.id}
            drone={drone}
            droneIcon={droneIcon}
            onDroneClick={handleDroneClick}
          />
        ))}
      </MarkerClusterGroup>
    );
  }, [droneIcon, drones, handleDroneClick]);

  // Закрытие попапа при клике на карту
  useEffect(() => {
    if (!mapRef.current) return;

    const handleMapClick = (e) => {
      if (!e.originalEvent?.target?.closest?.('.leaflet-marker-icon')) {
        setSelectedDrone(null);
      }
    };

    mapRef.current.on('click', handleMapClick);

    return () => {
      mapRef.current?.off('click', handleMapClick);
    };
  }, []);

  React.useImperativeHandle(ref, () => ({
    changeTileLayer: (newUrl) => {
      if (onTileUrlChange) {
        onTileUrlChange(newUrl);
      }
    },
    resetMap: () => {
      if (mapRef.current) {
        mapRef.current.setView(CONFIG.center, CONFIG.zoom);
      }
    },
    updateCityData: (cityData) => {
      if (mapRef.current && cityData.lat && cityData.lon) {
        mapRef.current.setView([cityData.lat, cityData.lon], 12);
      }
    },
    zoomIn: () => {
      if (mapRef.current) {
        mapRef.current.zoomIn();
      }
    },
    zoomOut: () => {
      if (mapRef.current) {
        mapRef.current.zoomOut();
      }
    },
    getMap: () => mapRef.current,
    flyTo: (center, zoom) => {
      if (mapRef.current) {
        mapRef.current.flyTo(center, zoom);
      }
    },
    setView: (center, zoom) => {
      if (mapRef.current) {
        mapRef.current.setView(center, zoom);
      }
    },
  }), [CONFIG.center, CONFIG.zoom, onTileUrlChange]);

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
          url={tileUrl}
          attribution="&copy; OpenStreetMap contributors"
        />

        <GeoJSON
          data={regionData}
          style={CONFIG.regionStyle}
          onEachFeature={onEachRegion}
        />

        {droneClusterGroup}

      </MapContainer>

      {/* Кастомный попап */}
      <DronePopup 
        drone={selectedDrone}
        isVisible={!!selectedDrone}
        onClose={handleClosePopup}
        position={popupPosition}
      />
    </div>
  );
});

Map.displayName = "Map";
export default Map;