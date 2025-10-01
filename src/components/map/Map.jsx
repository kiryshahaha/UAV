"use client";

import React, {
  useRef,
  forwardRef,
  useState,
  useEffect,
  useMemo,
  useCallback,
} from "react";
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
import DronePopup from "./DronePopup";
import DroneMarker from "./DroneMarker";
import RegionPopup from "./RegionPopup";

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
  maxBounds: [
    [-90, -180],
    [90, 190],
  ],
  clusterOptions: {
    chunkedLoading: true,
    maxClusterRadius: 100,
    spiderfyOnMaxZoom: true,
    showCoverageOnHover: true,
    zoomToBoundsOnClick: true,
    disableClusteringAtZoom: 16,
    spiderLegPolylineOptions: { weight: 1.5, color: "#222", opacity: 0.5 },
  },
};

function useDrones() {
  const [rawDrones, setRawDrones] = useState([]);
  const [loading, setLoading] = useState(true);
  const [droneIcon, setDroneIcon] = useState(null);

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
        setRawDrones(await res.json());
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

  return { drones, droneIcon, loading };
}

function useRegions({ foundRegions, mapRef, setSelectedRegion }) {
  useEffect(() => {
    if (!foundRegions?.length || !mapRef.current) return;

    const normalize = (str) => str?.toLowerCase().trim();
    const matchedFeatures = regionData.features.filter((feature) =>
      foundRegions.some((searchTerm) =>
        normalize(feature.properties?.REGION_NAME).includes(
          normalize(searchTerm)
        )
      )
    );

    if (matchedFeatures.length > 0) {
      const L = require("leaflet");
      const geoJsonHighlight = {
        type: "FeatureCollection",
        features: matchedFeatures,
      };
      const layer = new L.GeoJSON(geoJsonHighlight);
      mapRef.current.fitBounds(layer.getBounds(), { padding: [50, 50] });

      const center = layer.getBounds().getCenter();
      const regionName = matchedFeatures[0].properties.REGION_NAME;

      // Устанавливаем позицию попапа по центру карты
      const container = mapRef.current.getContainer();
      setSelectedRegion({
        name: regionName,
        position: {
          x: container.clientWidth / 2,
          y: container.clientHeight / 2
        }
      });
    }
  }, [foundRegions, mapRef, setSelectedRegion]);
}

const Map = forwardRef((props, ref) => {
  const { tileUrl, onTileUrlChange, foundRegions = [], onShowRegionStatistics = [] } = props;
  const mapRef = useRef(null);
  const markerClusterRef = useRef(null);
  const { drones, droneIcon, loading } = useDrones();

  const [selectedDrone, setSelectedDrone] = useState(null);
  const [popupPosition, setPopupPosition] = useState({ x: 0, y: 0 });
  const [selectedRegion, setSelectedRegion] = useState(null);
  const [selectedRegionForDrawer, setSelectedRegionForDrawer] = useState(null);

  useRegions({ foundRegions, mapRef, setSelectedRegion });

  const onEachRegion = useCallback((feature, layer) => {
    // Убираем полностью стандартные попапы - не вызываем layer.bindPopup()

    layer.on({
      mouseover: () => layer.setStyle(CONFIG.hoverStyle),
      mouseout: () => layer.setStyle(CONFIG.regionStyle),
      click: (e) => {
        // Устанавливаем позицию попапа по клику
        const rect = e.target._map.getContainer().getBoundingClientRect();
        setSelectedRegion({
          name: feature.properties.REGION_NAME,
          position: {
            x: e.originalEvent.clientX - rect.left,
            y: e.originalEvent.clientY - rect.top,
          }
        });
        mapRef.current?.fitBounds(layer.getBounds(), {
          padding: [50, 50],
          animate: true,
        });
      },
    });
  }, []);

  const handleDroneClick = useCallback((drone, event) => {
    const rect = event.target._map.getContainer().getBoundingClientRect();
    setPopupPosition({
      x: event.originalEvent.clientX - rect.left,
      y: event.originalEvent.clientY - rect.top,
    });
    setSelectedDrone(drone);
  }, []);

  const handleClosePopup = useCallback(() => setSelectedDrone(null), []);
  const handleCloseRegionPopup = useCallback(() => setSelectedRegion(null), []);

  useEffect(() => {
    if (!mapRef.current) return;
    const handleMapClick = (e) => {
      // Закрываем попап дрона если кликнули не по маркеру
      if (!e.originalEvent?.target?.closest?.(".leaflet-marker-icon")) {
        setSelectedDrone(null);
      }
      // Закрываем попап региона если кликнули не по региону
      if (!e.originalEvent?.target?.closest?.(".leaflet-interactive")) {
        setSelectedRegion(null);
      }
    };
    mapRef.current.on("click", handleMapClick);
    return () => mapRef.current?.off("click", handleMapClick);
  }, []);

  const handleShowStatistics = useCallback((regionName) => {
    console.log("Opening drawer for region:", regionName);
    // Вместо локального состояния вызываем функцию из пропсов
    onShowRegionStatistics?.(regionName);
    // Закрываем попап региона
    setSelectedRegion(null);
  }, [onShowRegionStatistics]);
  
  const droneClusterGroup = useMemo(() => {
    if (!droneIcon || !drones.length || loading) return null;
    return (
      <MarkerClusterGroup ref={markerClusterRef} {...CONFIG.clusterOptions}>
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
  }, [droneIcon, drones, handleDroneClick, loading]);

  React.useImperativeHandle(
    ref,
    () => ({
      changeTileLayer: (newUrl) => onTileUrlChange?.(newUrl),
      resetMap: () => mapRef.current?.setView(CONFIG.center, CONFIG.zoom),
      updateCityData: (cityData) =>
        cityData.lat &&
        cityData.lon &&
        mapRef.current?.setView([cityData.lat, cityData.lon], 12),
      zoomIn: () => mapRef.current?.zoomIn(),
      zoomOut: () => mapRef.current?.zoomOut(),
      getMap: () => mapRef.current,
      flyTo: (center, zoom) => mapRef.current?.flyTo(center, zoom),
      setView: (center, zoom) => mapRef.current?.setView(center, zoom),
    }),
    [onTileUrlChange]
  );

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
        maxBounds={CONFIG.maxBounds}
        maxBoundsViscosity={0.95}
        worldCopyJump={false}
        ref={mapRef}
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
      <DronePopup
        drone={selectedDrone}
        isVisible={!!selectedDrone}
        onClose={handleClosePopup}
        position={popupPosition}
      />
      <RegionPopup
        regionName={selectedRegion?.name}
        isVisible={!!selectedRegion}
        onClose={handleCloseRegionPopup}
        position={selectedRegion?.position}
        onShowStatistics={handleShowStatistics}
      />
    </div>
  );
});

Map.displayName = "Map";
export default Map;