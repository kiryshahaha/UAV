"use client";

import React, { useCallback } from "react";
import { Marker, Tooltip } from "@/components/leaflet/leaFletNoSSR.js";

const DroneMarker = ({ drone, droneIcon, onDroneClick }) => {
  const handleClick = useCallback((e) => {
    onDroneClick(drone, e);
  }, [drone, onDroneClick]);

  return (
    <Marker 
      position={[drone.lat, drone.lng]} 
      icon={droneIcon}
      eventHandlers={{
        click: handleClick
      }}
    >
      <Tooltip
        direction="top"
        offset={[0, -10]}
        opacity={1}
        permanent={false}
      >
        ID: {drone.id} (кликните для деталей)
      </Tooltip>
    </Marker>
  );
};

export default React.memo(DroneMarker);