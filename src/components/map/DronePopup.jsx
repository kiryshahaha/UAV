"use client";

import React, { useState, useRef, useEffect } from "react";

// Функция для расшифровки высоты полета (возвращает второе значение если есть разделитель /)
const decodeFlightLevel = (level) => {
  if (!level || typeof level !== 'string') return null;
  
  // Обработка формата M0025/M0050 (берем второе значение)
  if (level.includes('/')) {
    const levels = level.split('/');
    if (levels.length >= 2) {
      const secondLevel = levels[1].trim();
      return decodeSingleFlightLevel(secondLevel);
    }
  }
  
  // Обработка одиночного значения
  return decodeSingleFlightLevel(level);
};

// Функция для расшифровки одиночного уровня высоты
const decodeSingleFlightLevel = (level) => {
  if (!level || typeof level !== 'string') return null;
  
  level = level.trim();
  
  // Обработка формата M0040 (400 метров)
  const mFormatMatch = level.match(/^M(\d+)$/i);
  if (mFormatMatch) {
    const heightValue = parseInt(mFormatMatch[1]);
    if (!isNaN(heightValue)) {
      return `${heightValue * 10} м`;
    }
  }
  
  // Обработка других форматов высоты
  const standardMatch = level.match(/^FL?(\d+)$/i);
  if (standardMatch) {
    const heightValue = parseInt(standardMatch[1]);
    if (!isNaN(heightValue)) {
      return `${heightValue * 100} футов`;
    }
  }
  
  return level;
};

// Функция для расшифровки типа БПЛА
const getAircraftTypeName = (typeCode) => {
  if (!typeCode) return null;
  
  const typeMappings = {
    'BLA': 'Беспилотный летательный аппарат',
    'AER': 'Аэростатический',
    '3BLA': 'Беспилотный (3 осевой)',
    '2BLA': 'Беспилотный (2 осевой)',
    '3BLA\\nRMK': 'Беспилотный (3 осевой) с примечанием',
    'BLA\\nRMK': 'Беспилотный с примечанием',
    'MULTI': 'Мультироторный',
    'FIXED': 'Самолетного типа',
    'VTOL': 'Вертолетного типа',
    'HYBRID': 'Гибридный',
    'QUAD': 'Квадрокоптер',
    'HEX': 'Гексакоптер',
    'OCTO': 'Октокоптер'
  };

  const cleanType = typeCode.replace(/\\n/g, ' ');
  return typeMappings[cleanType] || typeCode;
};

// Функция для расшифровки радиуса полета
const decodeFlightRadius = (radius) => {
  if (!radius) return null;
  
  if (typeof radius === 'string') {
    const match = radius.match(/(\d+)/);
    if (match) {
      const radiusValue = parseInt(match[1]);
      if (!isNaN(radiusValue)) {
        return `${radiusValue} км`;
      }
    }
    return radius;
  } else if (typeof radius === 'number') {
    return `${radius} км`;
  }
  
  return radius;
};

// Функция для расшифровки зоны полета по табелю
const decodeFlightZone = (zoneCode) => {
  if (!zoneCode || typeof zoneCode !== 'string') return zoneCode;
  
  const zoneMapping = {
    'Z1': 'Городская зона',
    'Z2': 'Пригородная зона', 
    'Z3': 'Сельская зона',
    'Z4': 'Промышленная зона',
    'Z5': 'Особая зона',
    'URB': 'Городская территория',
    'SUB': 'Пригородная территория',
    'RUR': 'Сельская территория',
    'IND': 'Промышленная территория',
    'RES': 'Жилая зона',
    'CTR': 'Контролируемая зона',
    'DNG': 'Опасная зона',
  };
  
  return zoneMapping[zoneCode.toUpperCase()] || zoneCode;
};

// Функции валидации и очистки данных (улучшенные)
const cleanOperatorName = (name) => {
  if (!name || typeof name !== 'string') return '';
  
  let cleaned = name.trim();
  
  // Удаляем телефоны (последовательности цифр длиной 7+)
  cleaned = cleaned.replace(/\b\d{7,}\b/g, '');
  
  // Заменяем цифру 4 на "Ч" если рядом есть 4 буквы
  cleaned = cleaned.replace(/([а-яА-Яa-zA-Z]{4})4/g, '$1Ч');
  cleaned = cleaned.replace(/4([а-яА-Яa-zA-Z]{4})/g, 'Ч$1');
  
  // Удаляем ZZZZZ
  cleaned = cleaned.replace(/ZZZZZ/g, '');
  cleaned = cleaned.replace(/\s+ZZZZZ/g, '');
  cleaned = cleaned.replace(/ZZZZZ\s+/g, '');
  
  // Удаляем все после RMK (включая RMK) - разные варианты
  cleaned = cleaned.replace(/RMK\/.*$/i, '');
  cleaned = cleaned.replace(/RMK\s.*$/i, '');
  cleaned = cleaned.replace(/RMK.*$/i, '');
  
  // Удаляем SID и все что после него
  cleaned = cleaned.replace(/SID\/.*$/i, '');
  cleaned = cleaned.replace(/SID\s.*$/i, '');
  cleaned = cleaned.replace(/SID.*$/i, '');
  
  // Удаляем другие служебные коды
  cleaned = cleaned.replace(/STAR\/.*$/i, '');
  cleaned = cleaned.replace(/VIA\/.*$/i, '');
  
  // Удаляем лишние пробелы и специальные символы
  cleaned = cleaned.replace(/\s+/g, ' ').trim();
  cleaned = cleaned.replace(/[^\wа-яА-Я\s\-\.]/g, '');
  
  // Удаляем одиночные буквы и цифры
  cleaned = cleaned.replace(/\b[а-яА-Яa-zA-Z0-9]\b/g, '');
  
  // Удаляем лишние пробелы снова
  cleaned = cleaned.replace(/\s+/g, ' ').trim();
  
  return cleaned || 'Неизвестный оператор';
};

// Улучшенная функция для извлечения телефонов
const extractPhones = (text) => {
  if (!text || typeof text !== 'string') return [];
  
  // Ищем последовательности цифр длиной от 7 до 15 символов
  const phoneRegex = /\b\d{7,15}\b/g;
  const potentialPhones = text.match(phoneRegex) || [];
  
  // Фильтруем телефонные номера
  const validPhones = potentialPhones.filter(phone => {
    const phoneNum = parseInt(phone);
    
    // Фильтруем слишком большие числа
    if (phoneNum > 9999999999) return false;
    
    // Фильтруем номера, которые выглядят как коды/идентификаторы
    if (phone.length >= 10 && phone.startsWith('0')) return false;
    
    // Проверяем на последовательности одинаковых цифр
    const allSame = phone.split('').every(char => char === phone[0]);
    if (allSame) return false;
    
    // Проверяем на последовательные цифры
    const isSequential = phone.split('').every((char, index, arr) => 
      index === 0 || parseInt(char) === parseInt(arr[index - 1]) + 1
    );
    if (isSequential) return false;
    
    // Проверяем на убывающие последовательности
    const isDescending = phone.split('').every((char, index, arr) => 
      index === 0 || parseInt(char) === parseInt(arr[index - 1]) - 1
    );
    if (isDescending) return false;
    
    return true;
  });
  
  return [...new Set(validPhones)];
};

// Улучшенная функция для предварительной очистки текста
const preCleanText = (text) => {
  if (!text || typeof text !== 'string') return text;
  
  let cleaned = text;
  
  // Удаляем блоки с SID (включая все после него)
  cleaned = cleaned.replace(/SID\/.*$/gi, '');
  cleaned = cleaned.replace(/SID\s+.*$/gi, '');
  cleaned = cleaned.replace(/SID.*$/gi, '');
  
  // Удаляем блоки с RMK (включая все после него)
  cleaned = cleaned.replace(/RMK\/.*$/gi, '');
  cleaned = cleaned.replace(/RMK\s+.*$/gi, '');
  cleaned = cleaned.replace(/RMK.*$/gi, '');
  
  // Удаляем другие служебные коды
  cleaned = cleaned.replace(/STAR\/.*$/gi, '');
  cleaned = cleaned.replace(/VIA\/.*$/gi, '');
  cleaned = cleaned.replace(/[A-Z]{2,3}\d{6,}/g, '');
  
  return cleaned;
};

// Функция для извлечения телефонов из всех полей объекта flight
const extractAllPhonesFromFlight = (flight) => {
  if (!flight) return [];
  
  const allPhones = [];
  
  const checkFieldForPhones = (value, fieldPath = '') => {
    if (typeof value === 'string') {
      const skipFields = ['flight_zone', 'registration_number', 'date_of_flight', 'flight_level'];
      const currentField = fieldPath.split('.').pop();
      
      if (!skipFields.includes(currentField)) {
        const preCleanedText = preCleanText(value);
        const phonesInField = extractPhones(preCleanedText);
        if (phonesInField.length > 0) {
          console.log(`Найдены телефоны в поле ${fieldPath}:`, phonesInField, `(оригинальный текст: ${value.substring(0, 50)}...)`);
          allPhones.push(...phonesInField);
        }
      }
    } else if (typeof value === 'object' && value !== null) {
      Object.entries(value).forEach(([key, nestedValue]) => {
        checkFieldForPhones(nestedValue, fieldPath ? `${fieldPath}.${key}` : key);
      });
    }
  };
  
  Object.entries(flight).forEach(([key, value]) => {
    checkFieldForPhones(value, key);
  });
  
  const uniquePhones = [...new Set(allPhones)];
  console.log('Все найденные уникальные телефоны:', uniquePhones);
  return uniquePhones;
};

// Основная функция валидации и очистки данных полета
const validateAndCleanFlightData = (flight) => {
  if (!flight) return null;
  
  const cleanedFlight = { ...flight };
  
  // Очищаем поле оператора
  if (cleanedFlight.operator) {
    cleanedFlight.operator = cleanOperatorName(cleanedFlight.operator);
  }
  
  // Очищаем другие текстовые поля
  const textFields = ['registration_number', 'flight_zone', 'flight_zone_radius'];
  textFields.forEach(field => {
    if (cleanedFlight[field] && typeof cleanedFlight[field] === 'string') {
      cleanedFlight[field] = cleanedFlight[field].trim();
    }
  });
  
  // Расшифровываем зону полета
  if (cleanedFlight.flight_zone) {
    cleanedFlight.flight_zone_decoded = decodeFlightZone(cleanedFlight.flight_zone);
  }
  
  // Расшифровываем радиус полета
  if (cleanedFlight.flight_zone_radius) {
    cleanedFlight.flight_zone_radius_decoded = decodeFlightRadius(cleanedFlight.flight_zone_radius);
  }
  
  // Очищаем поля во вложенных объектах
  if (cleanedFlight.flight_time) {
    const cleanedFlightTime = { ...cleanedFlight.flight_time };
    
    const timeFields = ['departure_time', 'arrival_time'];
    timeFields.forEach(field => {
      if (cleanedFlightTime[field] && typeof cleanedFlightTime[field] === 'string') {
        cleanedFlightTime[field] = cleanedFlightTime[field].trim();
      }
    });
    
    cleanedFlight.flight_time = cleanedFlightTime;
  }
  
  if (cleanedFlight.additional_info) {
    const cleanedAdditionalInfo = { ...cleanedFlight.additional_info };
    
    const additionalFields = ['aircraft_type', 'flight_level', 'remarks'];
    additionalFields.forEach(field => {
      if (cleanedAdditionalInfo[field] && typeof cleanedAdditionalInfo[field] === 'string') {
        if (field === 'remarks') {
          let cleanedRemarks = cleanedAdditionalInfo[field];
          cleanedRemarks = preCleanText(cleanedRemarks);
          cleanedRemarks = cleanedRemarks.replace(/\s+/g, ' ').trim();
          cleanedAdditionalInfo[field] = cleanedRemarks;
        } else {
          cleanedAdditionalInfo[field] = cleanedAdditionalInfo[field].trim();
        }
      }
    });
    
    // Расшифровываем высоту полета (берем второе значение если есть /)
    if (cleanedAdditionalInfo.flight_level) {
      cleanedAdditionalInfo.flight_level_decoded = decodeFlightLevel(cleanedAdditionalInfo.flight_level);
    }
    
    // Расшифровываем тип БПЛА и заменяем исходное значение
    if (cleanedAdditionalInfo.aircraft_type) {
      const decodedType = getAircraftTypeName(cleanedAdditionalInfo.aircraft_type);
      cleanedAdditionalInfo.aircraft_type_original = cleanedAdditionalInfo.aircraft_type; // сохраняем оригинал
      cleanedAdditionalInfo.aircraft_type = decodedType; // заменяем на расшифрованное
    }
    
    cleanedFlight.additional_info = cleanedAdditionalInfo;
  }
  
  return cleanedFlight;
};

// Остальная часть компонента DronePopup остается без изменений...
const DronePopup = ({ drone, flight, isVisible, onClose, position }) => {
  const [pos, setPos] = useState({ x: 0, y: 0 });
  const [offset, setOffset] = useState({ x: 0, y: 0 });
  const [dragging, setDragging] = useState(false);
  const [isMobile, setIsMobile] = useState(false);
  const popupRef = useRef(null);

  // Обрабатываем данные полета
  const processedFlight = flight ? validateAndCleanFlightData(flight) : null;
  const extractedPhones = flight ? extractAllPhonesFromFlight(flight) : [];

  // Определяем мобильное устройство
  useEffect(() => {
    const checkMobile = () => {
      setIsMobile(window.innerWidth <= 768);
    };
    
    checkMobile();
    window.addEventListener('resize', checkMobile);
    
    return () => {
      window.removeEventListener('resize', checkMobile);
    };
  }, []);

  // Позиционирование попапа
  useEffect(() => {
    if (isVisible && drone) {
      if (isMobile) {
        setPos({
          x: 10,
          y: window.innerHeight * 0.1,
        });
      } else {
        setPos({
          x: position?.x || window.innerWidth / 2 - 200,
          y: position?.y || window.innerHeight / 2 - 150,
        });
      }
    }
  }, [isVisible, position, isMobile, drone]);

  if (!isVisible || !drone) return null;

  const startDrag = (e) => {
    if (isMobile) return;
    
    setDragging(true);
    const rect = popupRef.current.getBoundingClientRect();
    setOffset({
      x: e.clientX - rect.left,
      y: e.clientY - rect.top,
    });
  };

  const onDrag = (e) => {
    if (!dragging || isMobile) return;
    
    let x = e.clientX - offset.x;
    let y = e.clientY - offset.y;

    const popupWidth = popupRef.current.offsetWidth;
    const popupHeight = popupRef.current.offsetHeight;
    
    x = Math.max(0, Math.min(x, window.innerWidth - popupWidth));
    y = Math.max(0, Math.min(y, window.innerHeight - popupHeight));

    setPos({ x, y });
  };

  const stopDrag = () => {
    if (!isMobile) {
      setDragging(false);
    }
  };

  const formatPhoneNumber = (phone) => {
    if (phone.length === 11 && phone.startsWith('7')) {
      return `+7 (${phone.substring(1, 4)}) ${phone.substring(4, 7)}-${phone.substring(7, 9)}-${phone.substring(9)}`;
    } else if (phone.length === 10 && phone.startsWith('9')) {
      return `+7 (${phone.substring(0, 3)}) ${phone.substring(3, 6)}-${phone.substring(6, 8)}-${phone.substring(8)}`;
    }
    return phone;
  };

  return (
    <div
      ref={popupRef}
      style={{
        position: isMobile ? "fixed" : "absolute",
        top: isMobile ? "auto" : pos.y,
        bottom: isMobile ? "40%" : "auto",
        left: isMobile ? "10px" : pos.x,
        right: isMobile ? "10px" : "auto",
        backgroundColor: "rgba(255, 255, 255, 0.95)",
        backdropFilter: "blur(20px)",
        borderRadius: "16px",
        boxShadow: `
          0 8px 32px rgba(0, 0, 0, 0.15),
          0 2px 8px rgba(0, 0, 0, 0.1),
          inset 0 1px 0 rgba(255, 255, 255, 0.6)
        `,
        padding: "0",
        width: isMobile ? "calc(100vw - 20px)" : "450px",
        maxWidth: "420px",
        zIndex: 9999,
        border: "1px solid rgba(255, 255, 255, 0.4)",
        maxHeight: isMobile ? "35vh" : "500px",
        overflow: "hidden",
        cursor: dragging ? "grabbing" : "default",
        fontFamily: "'Inter', -apple-system, BlinkMacSystemFont, sans-serif",
        isolation: "isolate",
        transform: "translateZ(0)",
      }}
      onMouseMove={onDrag}
      onMouseUp={stopDrag}
      onMouseLeave={stopDrag}
    >
      {/* Заголовок с стеклянным эффектом */}
      <div
        style={{
          background: "rgba(255, 255, 255, 0.4)",
          borderBottom: "1px solid rgba(0, 0, 0, 0.08)",
          padding: isMobile ? "16px 16px 12px" : "18px 20px 14px",
          cursor: isMobile ? "default" : "grab",
          userSelect: "none",
          backdropFilter: "blur(10px)",
          zIndex: 10000,
          position: "relative",
        }}
        onMouseDown={startDrag}
      >
        <div
          style={{
            display: "flex",
            justifyContent: "space-between",
            alignItems: "flex-start",
          }}
        >
          <div style={{ flex: 1, marginRight: "10px" }}>
            <h3 style={{ 
              margin: "0 0 6px 0", 
              fontSize: isMobile ? "16px" : "17px", 
              fontWeight: "600",
              color: "#1a1a1a",
              letterSpacing: "-0.01em"
            }}>
              Дрон #{drone.id}
            </h3>
            <div style={{ 
              display: "flex", 
              alignItems: "center",
              fontSize: isMobile ? "11px" : "12px",
              color: "#666"
            }}>
              <div style={{
                width: "6px",
                height: "6px",
                borderRadius: "50%",
                backgroundColor: "#10b981",
                marginRight: "6px"
              }}/>
              Активен • {drone.lat.toFixed(4)}, {drone.lng.toFixed(4)}
            </div>
          </div>
          <button
            onClick={onClose}
            style={{
              background: "rgba(0, 0, 0, 0.06)",
              border: "none",
              borderRadius: "8px",
              width: isMobile ? "36px" : "32px",
              height: isMobile ? "36px" : "32px",
              fontSize: isMobile ? "20px" : "18px",
              cursor: "pointer",
              color: "#666",
              display: "flex",
              alignItems: "center",
              justifyContent: "center",
              transition: "all 0.15s ease",
              fontWeight: "300",
              flexShrink: 0,
              zIndex: 10001,
              position: "relative",
            }}
            onMouseOver={(e) => {
              if (!isMobile) {
                e.target.style.background = "rgba(0, 0, 0, 0.1)";
                e.target.style.color = "#333";
              }
            }}
            onMouseOut={(e) => {
              if (!isMobile) {
                e.target.style.background = "rgba(0, 0, 0, 0.06)";
                e.target.style.color = "#666";
              }
            }}
          >
            ×
          </button>
        </div>
      </div>

      {/* Основное содержимое */}
      <div style={{ 
        padding: isMobile ? "16px" : "20px", 
        maxHeight: isMobile ? "calc(70vh - 80px)" : "400px", 
        overflowY: "auto",
        background: "rgba(255, 255, 255, 0.3)",
        position: "relative",
        zIndex: 9998,
      }}>
        {/* Детали полета */}
        {processedFlight && (
          <div>
            {/* Основная информация */}
            <div style={{ marginBottom: isMobile ? "16px" : "24px" }}>
              <div style={{ 
                display: "flex", 
                alignItems: "center", 
                marginBottom: "10px",
                padding: "10px 14px",
                background: "rgba(255, 255, 255, 0.6)",
                borderRadius: "10px",
                border: "1px solid rgba(0, 0, 0, 0.05)"
              }}>
                <div style={{ 
                  width: "4px", 
                  height: "4px", 
                  borderRadius: "50%", 
                  backgroundColor: "#3b82f6", 
                  marginRight: "8px" 
                }}/>
                <span style={{ 
                  fontSize: isMobile ? "13px" : "14px", 
                  fontWeight: "600", 
                  color: "#1a1a1a" 
                }}>
                  Основная информация
                </span>
              </div>
              
              <div style={{ 
                background: "rgba(255, 255, 255, 0.5)",
                borderRadius: "10px",
                padding: isMobile ? "12px" : "16px",
                border: "1px solid rgba(0, 0, 0, 0.04)"
              }}>
                <InfoRow label="Рег. номер" value={processedFlight.registration_number} isMobile={isMobile} />
                <InfoRow label="Дата полёта" value={processedFlight.date_of_flight} isMobile={isMobile} />
                <InfoRow label="Оператор" value={processedFlight.operator} isMobile={isMobile} />
              </div>
            </div>

            {/* Найденные телефоны */}
            {extractedPhones.length > 0 && (
              <div style={{ marginBottom: isMobile ? "16px" : "24px" }}>
                <div style={{ 
                  display: "flex", 
                  alignItems: "center", 
                  marginBottom: "10px",
                  padding: "10px 14px",
                  background: "rgba(255, 255, 255, 0.6)",
                  borderRadius: "10px",
                  border: "1px solid rgba(0, 0, 0, 0.05)"
                }}>
                  <div style={{ 
                    width: "4px", 
                    height: "4px", 
                    borderRadius: "50%", 
                    backgroundColor: "#ef4444", 
                    marginRight: "8px" 
                  }}/>
                  <span style={{ 
                    fontSize: isMobile ? "13px" : "14px", 
                    fontWeight: "600", 
                    color: "#1a1a1a" 
                  }}>
                    Найденные телефоны ({extractedPhones.length})
                  </span>
                </div>
                
                <div style={{ 
                  background: "rgba(255, 255, 255, 0.5)",
                  borderRadius: "10px",
                  padding: isMobile ? "12px" : "16px",
                  border: "1px solid rgba(0, 0, 0, 0.04)"
                }}>
                  {extractedPhones.map((phone, index) => (
                    <div key={index} style={{ 
                      display: "flex",
                      justifyContent: "space-between",
                      alignItems: "center",
                      marginBottom: isMobile ? "8px" : "10px",
                      fontSize: isMobile ? "13px" : "14px"
                    }}>
                      <span style={{ 
                        color: "#555", 
                        fontWeight: "500",
                        minWidth: isMobile ? "90px" : "100px"
                      }}>
                        Телефон {index + 1}:
                      </span>
                      <span style={{ 
                        color: "#1a1a1a", 
                        fontWeight: "500",
                        fontFamily: "'Monaco', 'Consolas', monospace",
                        textAlign: "right",
                        flex: 1
                      }}>
                        {formatPhoneNumber(phone)}
                      </span>
                    </div>
                  ))}
                </div>
              </div>
            )}

            {/* Время полета */}
            <div style={{ marginBottom: isMobile ? "16px" : "24px" }}>
              <div style={{ 
                display: "flex", 
                alignItems: "center", 
                marginBottom: "10px",
                padding: "10px 14px",
                background: "rgba(255, 255, 255, 0.6)",
                borderRadius: "10px",
                border: "1px solid rgba(0, 0, 0, 0.05)"
              }}>
                <div style={{ 
                  width: "4px", 
                  height: "4px", 
                  borderRadius: "50%", 
                  backgroundColor: "#f59e0b", 
                  marginRight: "8px" 
                }}/>
                <span style={{ 
                  fontSize: isMobile ? "13px" : "14px", 
                  fontWeight: "600", 
                  color: "#1a1a1a" 
                }}>
                  Время полёта
                </span>
              </div>
              
              <div style={{ 
                background: "rgba(255, 255, 255, 0.5)",
                borderRadius: "10px",
                padding: isMobile ? "12px" : "16px",
                border: "1px solid rgba(0, 0, 0, 0.04)"
              }}>
                <InfoRow label="Вылет" value={processedFlight.flight_time?.departure_time} isMobile={isMobile} />
                <InfoRow label="Прилёт" value={processedFlight.flight_time?.arrival_time} isMobile={isMobile} />
                <InfoRow 
                  label="Длительность" 
                  value={processedFlight.flight_time?.duration_minutes ? 
                    `${processedFlight.flight_time.duration_minutes} мин` : null
                  } 
                  isMobile={isMobile}
                />
              </div>
            </div>

            {/* Зона полета */}
            <div style={{ marginBottom: isMobile ? "16px" : "24px" }}>
              <div style={{ 
                display: "flex", 
                alignItems: "center", 
                marginBottom: "10px",
                padding: "10px 14px",
                background: "rgba(255, 255, 255, 0.6)",
                borderRadius: "10px",
                border: "1px solid rgba(0, 0, 0, 0.05)"
              }}>
                <div style={{ 
                  width: "4px", 
                  height: "4px", 
                  borderRadius: "50%", 
                  backgroundColor: "#10b981", 
                  marginRight: "8px" 
                }}/>
                <span style={{ 
                  fontSize: isMobile ? "13px" : "14px", 
                  fontWeight: "600", 
                  color: "#1a1a1a" 
                }}>
                  Зона полёта
                </span>
              </div>
              
              <div style={{ 
                background: "rgba(255, 255, 255, 0.5)",
                borderRadius: "10px",
                padding: isMobile ? "12px" : "16px",
                border: "1px solid rgba(0, 0, 0, 0.04)"
              }}>
                <InfoRow label="Код зоны" value={processedFlight.flight_zone} isMobile={isMobile} />
                {/* <InfoRow label="Расшифровка" value={processedFlight.flight_zone_decoded} isMobile={isMobile} /> */}
                <InfoRow label="Радиус" value={processedFlight.flight_zone_radius_decoded} isMobile={isMobile} />
              </div>
            </div>

            {/* Дополнительная информация */}
            <div style={{ marginBottom: isMobile ? "16px" : "24px" }}>
              <div style={{ 
                display: "flex", 
                alignItems: "center", 
                marginBottom: "10px",
                padding: "10px 14px",
                background: "rgba(255, 255, 255, 0.6)",
                borderRadius: "10px",
                border: "1px solid rgba(0, 0, 0, 0.05)"
              }}>
                <div style={{ 
                  width: "4px", 
                  height: "4px", 
                  borderRadius: "50%", 
                  backgroundColor: "#8b5cf6", 
                  marginRight: "8px" 
                }}/>
                <span style={{ 
                  fontSize: isMobile ? "13px" : "14px", 
                  fontWeight: "600", 
                  color: "#1a1a1a" 
                }}>
                  Дополнительно
                </span>
              </div>
              
              <div style={{ 
                background: "rgba(255, 255, 255, 0.5)",
                borderRadius: "10px",
                padding: isMobile ? "12px" : "16px",
                border: "1px solid rgba(0, 0, 0, 0.04)"
              }}>
                <InfoRow label="Тип ЛА" value={processedFlight.additional_info?.aircraft_type} isMobile={isMobile} />
                {/* <InfoRow label="Уровень полёта" value={processedFlight.additional_info?.flight_level} isMobile={isMobile} /> */}
                <InfoRow label="Высота" value={processedFlight.additional_info?.flight_level_decoded} isMobile={isMobile} />
                
                <div style={{ 
                  marginTop: "10px", 
                  paddingTop: "10px", 
                  borderTop: "1px solid rgba(0, 0, 0, 0.06)" 
                }}>
                  <div style={{ 
                    fontSize: isMobile ? "12px" : "13px", 
                    fontWeight: "600", 
                    color: "#1a1a1a",
                    marginBottom: "6px"
                  }}>
                    Примечания:
                  </div>
                  <div style={{ 
                    fontSize: isMobile ? "12px" : "13px", 
                    color: "#555", 
                    lineHeight: "1.5",
                    fontStyle: processedFlight.additional_info?.remarks ? "normal" : "italic"
                  }}>
                    {processedFlight.additional_info?.remarks || "Отсутствуют"}
                  </div>
                </div>
              </div>
            </div>
          </div>
        )}

        {!processedFlight && (
          <div
            style={{
              textAlign: "center",
              padding: isMobile ? "30px 16px" : "40px 20px",
              color: "#888",
              background: "rgba(255, 255, 255, 0.4)",
              borderRadius: "12px",
              border: "1px solid rgba(0, 0, 0, 0.05)"
            }}
          >
            <div style={{ fontSize: isMobile ? "28px" : "32px", marginBottom: "12px", opacity: 0.5 }}>📡</div>
            <p style={{ margin: 0, fontSize: isMobile ? "13px" : "14px", fontWeight: "500" }}>
              Нет данных о полёте
            </p>
          </div>
        )}
      </div>
    </div>
  );
};

// Вспомогательный компонент для строк информации
const InfoRow = ({ label, value, isMobile }) => (
  <div style={{ 
    display: "flex", 
    justifyContent: "space-between",
    alignItems: "flex-start",
    marginBottom: isMobile ? "8px" : "10px",
    fontSize: isMobile ? "13px" : "14px"
  }}>
    <span style={{ 
      color: "#555", 
      fontWeight: "500",
      minWidth: isMobile ? "90px" : "120px",
      fontSize: isMobile ? "12px" : "14px"
    }}>
      {label}:
    </span>
    <span style={{ 
      color: "#1a1a1a", 
      textAlign: "right",
      flex: 1,
      marginLeft: "8px",
      fontWeight: value ? "400" : "300",
      fontSize: isMobile ? "12px" : "14px",
      wordBreak: "break-word"
    }}>
      {value || "—"}
    </span>
  </div>
);

export default React.memo(DronePopup);