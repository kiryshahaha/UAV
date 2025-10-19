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
  Circle,
} from "@/components/leaflet/leaFletNoSSR.js";
import DronePopup from "./DronePopup";
import DroneMarker from "./DroneMarker";
import RegionPopup from "./RegionPopup";
import {
  useDrones,
  MAX_RADIUS_KM,
} from "@/app/api/fetchDronesUseEffect/useDrones";

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
  zoneStyle: {
    color: "#3388ff",
    fillColor: "#3388ff",
    fillOpacity: 0.2,
    weight: 2,
  },
};

// Хук useRegions должен принимать setSelectedMapRegion как параметр
function useRegions({ foundRegions, mapRef, setSelectedMapRegion }) {
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

      const regionName = matchedFeatures[0].properties.REGION_NAME;
      const container = mapRef.current.getContainer();
      setSelectedMapRegion({
        name: regionName,
        position: {
          x: container.clientWidth / 2,
          y: container.clientHeight / 2,
        },
      });
    }
  }, [foundRegions, mapRef, setSelectedMapRegion]);
}

const Map = forwardRef((props, ref) => {
  const {
    tileUrl,
    onTileUrlChange,
    foundRegions = [],
    onShowRegionStatistics,
    onShowRegionDashboard,
    onShowRegionOperators,
    // Добавляем пропсы для фильтрации
    dateRange = null,
    selectedRegion = null,
    tableVersion = 0,
  } = props;

  const API_URL = process.env.NEXT_PUBLIC_API_URL;

  const mapRef = useRef(null);
  const markerClusterRef = useRef(null);

  const handleShowDashboard = useCallback(
    (regionName) => {
      onShowRegionDashboard?.(regionName);
      setSelectedMapRegion(null);
    },
    [onShowRegionDashboard]
  );

  // Добавляем обработчик для операторов региона
  const handleShowOperators = useCallback(
    (regionName) => {
      onShowRegionOperators?.(regionName);
      setSelectedMapRegion(null);
    },
    [onShowRegionOperators]
  );

  // Используем обновленный хук useDrones с параметрами фильтрации
  const { drones, droneIcon, loading } = useDrones({
    dateRange,
    region: selectedRegion,
    tableVersion, // Передаем версию таблицы
  });

  const [selectedDrone, setSelectedDrone] = useState(null);
  const [selectedFlight, setSelectedFlight] = useState(null);
  const [popupPosition, setPopupPosition] = useState({ x: 0, y: 0 });
  const [selectedMapRegion, setSelectedMapRegion] = useState(null);

  // Исправленный вызов хука - передаем setSelectedMapRegion
  useRegions({ 
    foundRegions, 
    mapRef, 
    setSelectedMapRegion 
  });

  const onEachRegion = useCallback((feature, layer) => {
    layer.on({
      mouseover: () => layer.setStyle(CONFIG.hoverStyle),
      mouseout: () => layer.setStyle(CONFIG.regionStyle),
      click: (e) => {
        const rect = e.target._map.getContainer().getBoundingClientRect();
        setSelectedMapRegion({
          name: feature.properties.REGION_NAME,
          position: {
            x: e.originalEvent.clientX - rect.left,
            y: e.originalEvent.clientY - rect.top,
          },
        });
        mapRef.current?.fitBounds(layer.getBounds(), {
          padding: [50, 50],
          animate: true,
        });
      },
    });
  }, []);

  const handleDroneClick = useCallback(
    async (drone, event) => {
      const rect = event.target._map.getContainer().getBoundingClientRect();
      setPopupPosition({
        x: event.originalEvent.clientX - rect.left,
        y: event.originalEvent.clientY - rect.top,
      });
      setSelectedDrone(drone);
      setSelectedFlight(null);

      try {
        const res = await fetch(`${API_URL}/flights/${drone.id}`);
        if (res.ok) {
          const data = await res.json();
          setSelectedFlight(data);
        }
      } catch (err) {
        console.error("Ошибка загрузки зоны полета:", err);
      }
    },
    [API_URL]
  );

  const handleClosePopup = useCallback(() => {
    setSelectedDrone(null);
    setSelectedFlight(null);
  }, []);

  const handleCloseRegionPopup = useCallback(() => setSelectedMapRegion(null), []);

  const circleData = useMemo(() => {
    if (!selectedFlight) return null;
    const rawRadiusStr = selectedFlight?.flight_zone_radius;
    const takeoffPoint = selectedFlight?.takeoff_point;
    if (!takeoffPoint || !takeoffPoint.latitude || !takeoffPoint.longitude) {
      return null;
    }

    const lat = Number(takeoffPoint.latitude);
    const lng = Number(takeoffPoint.longitude);
    if (isNaN(lat) || isNaN(lng)) return null;

    let radiusKm = NaN;
    if (typeof rawRadiusStr === "string") {
      if (rawRadiusStr.startsWith("R")) {
        radiusKm = parseInt(rawRadiusStr.substring(1), 10);
      } else {
        const match = rawRadiusStr.match(/\d+/);
        radiusKm = match ? parseInt(match[0], 10) : NaN;
      }
    } else if (typeof rawRadiusStr === "number") {
      radiusKm = rawRadiusStr;
    }

    if (isNaN(radiusKm) || radiusKm <= 0 || radiusKm > MAX_RADIUS_KM)
      return null;

    return {
      center: [lat, lng],
      radius: radiusKm * 1000,
      style: CONFIG.zoneStyle,
    };
  }, [selectedFlight]);

  useEffect(() => {
    if (!mapRef.current || !circleData) return;
    const L = require("leaflet");
    const tempCircle = L.circle(circleData.center, {
      radius: circleData.radius,
    }).addTo(mapRef.current);
    const bounds = tempCircle.getBounds();
    mapRef.current.removeLayer(tempCircle);
    if (bounds.isValid()) {
      mapRef.current.fitBounds(bounds, {
        padding: [50, 50],
        animate: true,
        duration: 1,
      });
    }
  }, [circleData]);

  useEffect(() => {
    if (!mapRef.current) return;
    const handleMapClick = (e) => {
      if (!e.originalEvent?.target?.closest?.(".leaflet-marker-icon")) {
        setSelectedDrone(null);
        setSelectedFlight(null);
      }
      if (!e.originalEvent?.target?.closest?.(".leaflet-interactive")) {
        setSelectedMapRegion(null);
      }
    };
    mapRef.current.on("click", handleMapClick);
    return () => mapRef.current?.off("click", handleMapClick);
  }, []);

  const handleShowStatistics = useCallback(
    (regionName) => {
      onShowRegionStatistics?.(regionName);
      setSelectedMapRegion(null);
    },
    [onShowRegionStatistics]
  );

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
        {circleData && (
          <Circle
            center={circleData.center}
            radius={circleData.radius}
            pathOptions={circleData.style}
          />
        )}
        {droneClusterGroup}
      </MapContainer>
      <DronePopup
        drone={selectedDrone}
        flight={selectedFlight}
        isVisible={!!selectedDrone}
        onClose={handleClosePopup}
        position={popupPosition}
      />
      <RegionPopup
        regionName={selectedMapRegion?.name}
        isVisible={!!selectedMapRegion}
        onClose={handleCloseRegionPopup}
        position={selectedMapRegion?.position}
        onShowStatistics={handleShowStatistics}
        onShowDashboard={handleShowDashboard}
        onShowOperators={handleShowOperators}
      />

      
    </div>
  );
});

Map.displayName = "Map";
export default Map;