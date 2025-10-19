// RegionPopupContent.jsx
"use client";

import React from "react";

const RegionPopupContent = ({ regionName, onShowStatistics, onShowDashboard, onShowOperators }) => {
  const handleShowStatistics = () => {
    console.log(`Показать статистику для региона: ${regionName}`);
    onShowStatistics?.(regionName);
  };

  const handleShowDashboard = () => {
    console.log(`Открыть дашборд для региона: ${regionName}`);
    onShowDashboard?.(regionName);
  };

  const handleShowOperators = () => {
    console.log(`Открыть операторов для региона: ${regionName}`);
    onShowOperators?.(regionName);
  };

  return (
    <div style={{ minWidth: "200px", display: 'flex', flexDirection:'column', justifyContent: 'center', textAlign: 'center', gap: '10px' }}>
      <span style={{ margin: "0 0 10px 0", color: "#333", fontSize: "14px", fontWeight: "500" }}>
        Регион: <strong>{regionName}</strong>
      </span>
      
      <div style={{ display: 'flex', flexDirection: 'column', gap: '8px' }}>
        <button 
          onClick={handleShowStatistics}
          style={{
            background: "#007bff",
            color: "white",
            border: "none",
            padding: "10px 16px",
            borderRadius: "6px",
            cursor: "pointer",
            fontSize: "14px",
            fontWeight: "500",
            transition: "background-color 0.2s"
          }}
          onMouseOver={(e) => e.target.style.backgroundColor = "#0056b3"}
          onMouseOut={(e) => e.target.style.backgroundColor = "#007bff"}
        >
          Быстрая статистика
        </button>

        <button 
          onClick={handleShowDashboard}
          style={{
            background: "#28a745",
            color: "white",
            border: "none",
            padding: "10px 16px",
            borderRadius: "6px",
            cursor: "pointer",
            fontSize: "14px",
            fontWeight: "500",
            transition: "background-color 0.2s"
          }}
          onMouseOver={(e) => e.target.style.backgroundColor = "#218838"}
          onMouseOut={(e) => e.target.style.backgroundColor = "#28a745"}
        >
          Полный дашборд
        </button>

        <button 
          onClick={handleShowOperators}
          style={{
            background: "#ffc107",
            color: "white",
            border: "none",
            padding: "10px 16px",
            borderRadius: "6px",
            cursor: "pointer",
            fontSize: "14px",
            fontWeight: "500",
            transition: "background-color 0.2s"
          }}
          onMouseOver={(e) => e.target.style.backgroundColor = "#e0a800"}
          onMouseOut={(e) => e.target.style.backgroundColor = "#ffc107"}
        >
          Операторы региона
        </button>
      </div>
      
      <div style={{ fontSize: "12px", color: "#666", marginTop: "8px" }}>
        Подробная аналитика по региону
      </div>
    </div>
  );
};

export default React.memo(RegionPopupContent);