// hooks/useDrones.js
import { useState, useEffect, useMemo } from "react";
import { useTable } from '@/contexts/TableContext';

export const MAX_RADIUS_KM = 5000;

export function useDrones({ dateRange = null, region = null } = {}) {
  const [rawDrones, setRawDrones] = useState([]);
  const [loading, setLoading] = useState(true);
  const [droneIcon, setDroneIcon] = useState(null);
  const { tableVersion, currentTable } = useTable();

  const API_URL = process.env.NEXT_PUBLIC_API_URL;

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
    if (!currentTable) return;

    async function fetchDrones() {
      try {
        setLoading(true);
        
        const params = new URLSearchParams();
        
        if (dateRange) {
          if (dateRange.date_from) {
            params.append('date_from', dateRange.date_from);
          }
          if (dateRange.date_to) {
            params.append('date_to', dateRange.date_to);
          }
        }
        
        if (region) {
          params.append('region', region);
        }

        // Добавляем информацию о таблице
        params.append('_v', tableVersion);
        params.append('_t', Date.now());

        const queryString = params.toString();
        const url = `${API_URL}/flights/points${queryString ? `?${queryString}` : ''}`;
        
        console.log("🔄 Fetching drones for table:", currentTable.table_name);
        
        const sessionId = localStorage.getItem('session_id');
        const headers = {};
        
        if (sessionId) {
          headers['X-Session-ID'] = sessionId;
        }
        
        const res = await fetch(url, {
          credentials: 'include',
          headers: headers
        });
        
        if (!res.ok) {
          throw new Error(`HTTP error: ${res.status}`);
        }
        
        const data = await res.json();
        
        let pointsCount = 0;
        if (data.points_by_year) {
          const allPoints = Object.values(data.points_by_year).flat();
          setRawDrones(allPoints);
          pointsCount = allPoints.length;
        } else {
          setRawDrones(data);
          pointsCount = data.length || 0;
        }
        
        console.log(`✅ Загружено ${pointsCount} дронов для таблицы "${currentTable.table_name}"`);
        
      } catch (err) {
        console.error("❌ Ошибка загрузки дронов:", err);
        setRawDrones([]);
      } finally {
        setLoading(false);
      }
    }
    
    fetchDrones();
  }, [API_URL, dateRange, region, tableVersion, currentTable]);

  const drones = useMemo(() => {
    const seen = new Set();
    const filtered = rawDrones
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
    
    console.log(`📍 Отображено ${filtered.length} дронов для таблицы "${currentTable?.table_name}"`);
    return filtered;
  }, [rawDrones, currentTable]);

  return { drones, droneIcon, loading };
}