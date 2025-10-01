// RegionPopupContent.jsx
"use client";

import React from "react";

const RegionPopupContent = ({ regionName, onShowStatistics }) => {
  const handleShowStatistics = () => {
    console.log(`Показать статистику для региона: ${regionName}`);
    onShowStatistics?.(regionName);
  };

  return (
    <div style={{ minWidth: "200px", display: 'flex', flexDirection:'column', justifyContent: 'center', textAlign: 'center' }}>
      <span style={{ margin: "0 0 10px 0", color: "#333" }}>Регион: {regionName}</span>
      <button 
        onClick={handleShowStatistics}
        style={{
          background: "#007bff",
          color: "white",
          border: "none",
          padding: "8px 16px",
          borderRadius: "4px",
          cursor: "pointer",
          fontSize: "14px"
        }}
      >
        Показать статистику
      </button>
    </div>
  );
};

export default React.memo(RegionPopupContent);