// hooks/useTimeBounds.js
import { useState, useEffect } from 'react';
import { useTable } from '@/contexts/TableContext';

export function useTimeBounds() {
  const [timeBounds, setTimeBounds] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const { tableVersion, currentTable } = useTable();

  const API_URL = process.env.NEXT_PUBLIC_API_URL;

  useEffect(() => {
    const fetchTimeBounds = async () => {
      // Если таблица не загружена, не делаем запрос
      if (!currentTable) {
        console.log("⏳ Таблица не загружена, ожидаем...");
        setLoading(true);
        return;
      }

      try {
        setLoading(true);
        console.log("🕐 Загрузка временных границ для таблицы:", currentTable.table_name);
        
        const params = new URLSearchParams();
        // Добавляем информацию о таблице для инвалидации кэша
        params.append('_v', tableVersion);
        params.append('_t', Date.now());

        const sessionId = localStorage.getItem('session_id');
        const headers = {};
        
        if (sessionId) {
          headers['X-Session-ID'] = sessionId;
        }

        const response = await fetch(`${API_URL}/stats/time-bounds?${params.toString()}`, {
          headers: headers
        });
        
        if (!response.ok) {
          throw new Error(`HTTP ${response.status}`);
        }
        
        const data = await response.json();
        console.log("✅ Временные границы загружены:", data);
        
        setTimeBounds(data);
        setError(null);
      } catch (err) {
        console.error("❌ Ошибка загрузки временных границ:", err);
        setError(err.message);
        // Fallback на дефолтные значения
        setTimeBounds({
          min_date: "2020-01-01",
          max_date: new Date().toISOString().split('T')[0],
          has_date_data: false
        });
      } finally {
        setLoading(false);
      }
    };

    fetchTimeBounds();
  }, [API_URL, tableVersion, currentTable]); // Добавлены зависимости

  return { timeBounds, loading, error };
}