"use client";

import React, { useRef, useMemo, forwardRef } from "react";
import "leaflet/dist/leaflet.css";
import "leaflet.markercluster/dist/MarkerCluster.css";
import "leaflet.markercluster/dist/MarkerCluster.Default.css";
import L from "leaflet";

import regionData from "public/geoData/RussiaWhole.json";
import droneData from "public/data/processed_data.json";
import styles from "./map.module.css";

import {
  MapContainer,
  TileLayer,
  GeoJSON,
  Marker,
  Popup,
  MarkerClusterGroup,
} from "@/components/leaflet/leaFletNoSSR.js";

// Иконка дрона через твой SVG
const droneIcon = new L.Icon({
  iconUrl: "/svg/drone.svg",
  iconSize: [24, 24],
  iconAnchor: [12, 12],
  popupAnchor: [0, -12],
  className: "drone-icon",
});

const Map = forwardRef((props, ref) => {
  const mapRef = useRef(null);
  const markerClusterRef = useRef(null);

  const CONFIG = {
    center: [55.7522, 37.6156],
    zoom: 5,
    minZoom: 2,
    maxZoom: 18,
    ZoomControl: false,
    regionStyle: {
      color: "#424d5b3d",
      fillColor: "#22222204",
      weight: 2,
      fillOpacity: 0.3,
    },
    hoverStyle: { fillColor: "#aeff3444" },
  };

  const processedDrones = useMemo(
    () =>
      droneData
        .map((d) => {
          const coords = d.dep_coord ||
            d.coordinates?.[0] || { latitude: 0, longitude: 0 };
          if (!coords || coords.latitude === 0 || coords.longitude === 0)
            return null;
          return { id: d.id, lat: coords.latitude, lng: coords.longitude };
        })
        .filter(Boolean),
    []
  );

  const onEachRegion = (feature, layer) => {
    if (feature.properties?.REGION_NAME)
      layer.bindPopup(feature.properties.REGION_NAME);
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

      <MarkerClusterGroup
        ref={markerClusterRef}
        chunkedLoading
        maxClusterRadius={50}
        spiderfyOnMaxZoom
        showCoverageOnHover
        zoomToBoundsOnClick
        disableClusteringAtZoom={16}
        spiderLegPolylineOptions={{ weight: 1.5, color: "#222", opacity: 0.5 }}
      >
        {processedDrones.map((d) => (
          <Marker key={d.id} position={[d.lat, d.lng]} icon={droneIcon} />
        ))}
      </MarkerClusterGroup>
    </MapContainer>
  );
});

Map.displayName = "Map";
export default Map;
