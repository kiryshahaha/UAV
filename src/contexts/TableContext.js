'use client'

// contexts/TableContext.js
import React, { createContext, useContext, useState, useEffect } from 'react';
import { tableService } from '@/utils/tableService';

const TableContext = createContext();

export const useTable = () => {
  const context = useContext(TableContext);
  if (!context) {
    throw new Error('useTable must be used within a TableProvider');
  }
  return context;
};

export const TableProvider = ({ children }) => {
  const [currentTable, setCurrentTable] = useState(null);
  const [tableVersion, setTableVersion] = useState(0);
  const [isLoading, setIsLoading] = useState(false);

  // Восстановление таблицы при загрузке
  useEffect(() => {
    const restoreTable = async () => {
      setIsLoading(true);
      try {
        const storedTableName = tableService.getStoredCurrentTable();
        if (storedTableName) {
          console.log("🔄 Восстанавливаем таблицу:", storedTableName);
          await tableService.selectTable(storedTableName);
          const current = await tableService.getCurrentTable();
          setCurrentTable(current);
          setTableVersion(prev => prev + 1);
        }
      } catch (error) {
        console.error("Ошибка восстановления таблицы:", error);
      } finally {
        setIsLoading(false);
      }
    };

    restoreTable();
  }, []);

  const selectTable = async (tableName) => {
    setIsLoading(true);
    try {
      console.log("🎯 Выбираем таблицу глобально:", tableName);
      
      await tableService.selectTable(tableName);
      const current = await tableService.getCurrentTable();
      
      setCurrentTable(current);
      setTableVersion(prev => prev + 1); // Принудительное обновление всех данных
      
      console.log("✅ Таблица установлена глобально:", current.table_name);
      return true;
    } catch (error) {
      console.error("❌ Ошибка выбора таблицы:", error);
      return false;
    } finally {
      setIsLoading(false);
    }
  };

  const refreshTable = () => {
    setTableVersion(prev => prev + 1);
    console.log("🔄 Принудительное обновление данных таблицы");
  };

  const value = {
    currentTable,
    tableVersion,
    isLoading,
    selectTable,
    refreshTable,
  };

  return (
    <TableContext.Provider value={value}>
      {children}
    </TableContext.Provider>
  );
};