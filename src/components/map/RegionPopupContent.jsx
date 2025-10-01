// RegionPopupContent.jsx
"use client";

import React from "react";

const RegionPopupContent = ({ regionName }) => {
  const handleShowStatistics = () => {
    console.log(`Показать статистику для региона: ${regionName}`);
    // Здесь будет логика для показа статистики
  };

  return (
    <div style={{ minWidth: "200px", fontFamily: "Arial, sans-serif" }}>
      <h4 style={{ margin: "0 0 10px 0", color: "#333" }}>Регион: {regionName}</h4>
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