"use client";

import React from "react";

const DronePopup = ({ drone, isVisible, onClose, position }) => {
  if (!isVisible || !drone) return null;

  return (
    <div style={{
      position: "absolute",
      top: position?.y || "50%",
      left: position?.x || "50%",
      transform: "translate(-50%, -50%)",
      backgroundColor: "white",
      borderRadius: "8px",
      boxShadow: "0 4px 20px rgba(0,0,0,0.15)",
      padding: "16px",
      minWidth: "250px",
      zIndex: 1000,
      border: "1px solid #e0e0e0"
    }}>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: "12px" }}>
        <h3 style={{ margin: 0, fontSize: "16px", color: "#333" }}>Информация о дроне</h3>
        <button 
          onClick={onClose}
          style={{
            background: "none",
            border: "none",
            fontSize: "18px",
            cursor: "pointer",
            color: "#666"
          }}
        >
          ×
        </button>
      </div>
      
      <div style={{ marginBottom: "16px" }}>
        <p style={{ margin: "4px 0", fontSize: "14px" }}><strong>ID:</strong> {drone.id}</p>
        <p style={{ margin: "4px 0", fontSize: "14px" }}><strong>Координаты:</strong></p>
        <p style={{ margin: "4px 0", fontSize: "12px", color: "#666" }}>
          Широта: {drone.lat.toFixed(6)}
        </p>
        <p style={{ margin: "4px 0", fontSize: "12px", color: "#666" }}>
          Долгота: {drone.lng.toFixed(6)}
        </p>
      </div>

      <div style={{ display: "flex", gap: "8px" }}>
        <button 
          onClick={() => {
            console.log("Подробности дрона:", drone.id);
          }}
          style={{
            padding: "8px 16px",
            backgroundColor: "#007bff",
            color: "white",
            border: "none",
            borderRadius: "4px",
            cursor: "pointer",
            fontSize: "14px",
            flex: 1
          }}
        >
          Подробности
        </button>
        <button 
          onClick={() => {
            console.log("Управление дрона:", drone.id);
          }}
          style={{
            padding: "8px 16px",
            backgroundColor: "#28a745",
            color: "white",
            border: "none",
            borderRadius: "4px",
            cursor: "pointer",
            fontSize: "14px",
            flex: 1
          }}
        >
          Управление
        </button>
      </div>
    </div>
  );
};

export default React.memo(DronePopup);