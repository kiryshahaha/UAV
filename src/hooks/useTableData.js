// hooks/useTableData.js
import { useState, useEffect } from 'react';
import { useTable } from '@/contexts/TableContext';

export function useTableData(fetchFunction, dependencies = []) {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const { tableVersion, currentTable } = useTable();

  useEffect(() => {
    let isMounted = true;

    async function fetchData() {
      if (!currentTable) return;
      
      try {
        setLoading(true);
        setError(null);
        
        console.log(`🔄 Загрузка данных для таблицы: ${currentTable.table_name}`);
        const result = await fetchFunction();
        
        if (isMounted) {
          setData(result);
          console.log(`✅ Данные загружены для таблицы: ${currentTable.table_name}`);
        }
      } catch (err) {
        if (isMounted) {
          console.error('❌ Ошибка загрузки данных:', err);
          setError(err);
        }
      } finally {
        if (isMounted) {
          setLoading(false);
        }
      }
    }

    fetchData();

    return () => {
      isMounted = false;
    };
  }, [tableVersion, currentTable, ...dependencies]);

  return { data, loading, error };
}