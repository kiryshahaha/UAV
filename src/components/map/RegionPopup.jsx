"use client";

import React from "react";
import RegionPopupContent from "./RegionPopupContent";

const RegionPopup = ({ regionName, isVisible, onClose, position }) => {
  if (!isVisible || !regionName) return null;

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
        <h3 style={{ margin: 0, fontSize: "16px", color: "#333" }}>Информация о регионе</h3>
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
      <RegionPopupContent regionName={regionName} />
    </div>
  );
};

export default React.memo(RegionPopup);