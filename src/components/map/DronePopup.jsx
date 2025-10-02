"use client";

import React, { useState, useRef, useEffect } from "react";

const DronePopup = ({ drone, flight, isVisible, onClose, position }) => {
  const [pos, setPos] = useState({ x: 0, y: 0 });
  const [offset, setOffset] = useState({ x: 0, y: 0 });
  const [dragging, setDragging] = useState(false);
  const popupRef = useRef(null);

  // при первом открытии — позиция берется из props
  useEffect(() => {
    if (isVisible && position) {
      setPos({
        x: position.x || window.innerWidth / 2,
        y: position.y || window.innerHeight / 2,
      });
    }
  }, [isVisible, position]);

  if (!isVisible || !drone) return null;

  const startDrag = (e) => {
    setDragging(true);
    const rect = popupRef.current.getBoundingClientRect();
    setOffset({
      x: e.clientX - rect.left,
      y: e.clientY - rect.top,
    });
  };

  const onDrag = (e) => {
    if (!dragging) return;
    setPos({
      x: e.clientX - offset.x,
      y: e.clientY - offset.y,
    });
  };

  const stopDrag = () => setDragging(false);

  return (
    <div
      ref={popupRef}
      style={{
        position: "absolute",
        top: pos.y,
        left: pos.x,
        backgroundColor: "white",
        borderRadius: "8px",
        boxShadow: "0 4px 20px rgba(0,0,0,0.15)",
        padding: "16px",
        minWidth: "280px",
        maxWidth: "400px",
        zIndex: 1000,
        border: "1px solid #e0e0e0",
        maxHeight: "450px",
        overflowY: "auto",
        cursor: dragging ? "grabbing" : "default",
      }}
      onMouseMove={onDrag}
      onMouseUp={stopDrag}
      onMouseLeave={stopDrag}
    >
      {/* Заголовок с кнопкой закрытия */}
      <div
        style={{
          display: "flex",
          justifyContent: "space-between",
          alignItems: "center",
          marginBottom: "12px",
          cursor: "grab",
          userSelect: "none",
        }}
        onMouseDown={startDrag}
      >
        <h3 style={{ margin: 0, fontSize: "16px", color: "#333" }}>
          Информация о дроне
        </h3>
        <button
          onClick={onClose}
          style={{
            background: "none",
            border: "none",
            fontSize: "18px",
            cursor: "pointer",
            color: "#666",
          }}
        >
          ×
        </button>
      </div>

      {/* Базовые данные */}
      <div style={{ marginBottom: "16px" }}>
        <p style={{ margin: "4px 0", fontSize: "14px" }}>
          <strong>ID:</strong> {drone.id}
        </p>
        <p style={{ margin: "4px 0", fontSize: "14px" }}>
          <strong>Координаты:</strong> {drone.lat.toFixed(6)}, {drone.lng.toFixed(6)}
        </p>
      </div>

      {/* Детали полета */}
      {flight && (
        <div>
          <h4 style={{ margin: "6px 0", fontSize: "14px", color: "#444" }}>
            Детали полёта
          </h4>
          <p style={{ margin: "2px 0", fontSize: "13px" }}>
            <strong>Рег. номер:</strong>{" "}
            {flight.registration_number || "не указан"}
          </p>
          <p style={{ margin: "2px 0", fontSize: "13px" }}>
            <strong>Дата:</strong> {flight.date_of_flight}
          </p>
          <p style={{ margin: "2px 0", fontSize: "13px" }}>
            <strong>Оператор:</strong> {flight.operator}
          </p>

          <h4 style={{ margin: "6px 0", fontSize: "14px", color: "#444" }}>
            Время полёта
          </h4>
          <p style={{ margin: "2px 0", fontSize: "13px" }}>
            <strong>Вылет:</strong> {flight.flight_time?.departure_time}
          </p>
          <p style={{ margin: "2px 0", fontSize: "13px" }}>
            <strong>Прилёт:</strong> {flight.flight_time?.arrival_time}
          </p>
          <p style={{ margin: "2px 0", fontSize: "13px" }}>
            <strong>Длительность:</strong> {flight.flight_time?.duration_minutes} мин
          </p>

          <h4 style={{ margin: "6px 0", fontSize: "14px", color: "#444" }}>
            Зона
          </h4>
          <p style={{ margin: "2px 0", fontSize: "13px" }}>
            <strong>Код зоны:</strong> {flight.flight_zone}
          </p>
          <p style={{ margin: "2px 0", fontSize: "13px" }}>
            <strong>Радиус:</strong> {flight.flight_zone_radius}
          </p>

          <h4 style={{ margin: "6px 0", fontSize: "14px", color: "#444" }}>
            Дополнительно
          </h4>
          <p style={{ margin: "2px 0", fontSize: "13px" }}>
            <strong>Тип ЛА:</strong> {flight.additional_info?.aircraft_type}
          </p>
          <p style={{ margin: "2px 0", fontSize: "13px" }}>
            <strong>Уровень:</strong> {flight.additional_info?.flight_level}
          </p>
          <p
            style={{ margin: "2px 0", fontSize: "13px", whiteSpace: "pre-wrap" }}
          >
            <strong>Примечания:</strong> {flight.additional_info?.remarks}
          </p>
        </div>
      )}
    </div>
  );
};

export default React.memo(DronePopup);
