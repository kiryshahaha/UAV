// components/ResizableDrawer/ResizableDrawer.jsx
import React, { useState, useEffect, useCallback, memo } from 'react';
import { Resizable } from 'react-resizable';
import styles from './ResizableDrawer.module.css';
import RegionsBarChart from '../RegionsBarChart/RegionsBarChart';
import Loader from '../loader/Loader';
import { useTable } from "@/contexts/TableContext";

const ResizableDrawer = memo(({ onClose, isOpen, dateRange, selectedRegion }) => {
  const [width, setWidth] = useState(0);
  const [isClosing, setIsClosing] = useState(false);
  const [isChartLoading, setIsChartLoading] = useState(true);
  const [regionsData, setRegionsData] = useState(undefined);
  const { tableVersion, currentTable } = useTable();

   useEffect(() => {
        const initialWidth = Math.min(window.innerWidth * 0.7, 1000);
        setWidth(initialWidth);
    }, []);

   const fetchRegionsData = useCallback(async (dateFrom, dateTo) => {
        if (!currentTable) return; // Добавлена проверка на currentTable

        try {
            setIsChartLoading(true);
            
            // Строим URL с параметрами
            const url = new URL('/api/regions', window.location.origin);
            if (dateFrom) url.searchParams.append('date_from', dateFrom);
            if (dateTo) url.searchParams.append('date_to', dateTo);
            
            // Добавляем информацию о таблице
            url.searchParams.append('_v', tableVersion);
            url.searchParams.append('_t', Date.now());

            const sessionId = localStorage.getItem('session_id');
            const headers = {};
            
            if (sessionId) {
                headers['X-Session-ID'] = sessionId;
            }

            console.log('📊 Запрос данных регионов с фильтрами:', { dateFrom, dateTo });

            const response = await fetch(url.toString(), {
                headers: headers
            });
            if (response.ok) {
                const data = await response.json();
                setRegionsData(data);
            } else {
                console.error('Ошибка загрузки данных:', response.status);
                setRegionsData([]);
            }
        } catch (error) {
            console.error('Ошибка загрузки данных:', error);
            setRegionsData([]);
        } finally {
            setIsChartLoading(false);
        }
    }, [tableVersion, currentTable]);

    useEffect(() => {
        if (isOpen && currentTable) { // Добавлена проверка на currentTable
            const dateFrom = dateRange?.date_from;
            const dateTo = dateRange?.date_to;
            fetchRegionsData(dateFrom, dateTo);
        }
    }, [isOpen, fetchRegionsData, dateRange, currentTable]);

  const handleResize = useCallback((e, { size }) => {
    const minWidth = 600;
    const maxWidth = window.innerWidth - 50;
    const newWidth = Math.max(minWidth, Math.min(size.width, maxWidth));
    setWidth(newWidth);
  }, []);

  const handleClose = useCallback(() => {
    setIsClosing(true);
    setTimeout(() => {
      onClose();
      setIsClosing(false);
    }, 300);
  }, [onClose]);

  const handleChartLoaded = useCallback(() => {
    setIsChartLoading(false);
  }, []);

  const drawerClassNames = [
    styles.drawer,
    isClosing ? styles.slideOut : '',
    !isOpen && !isClosing ? styles.hidden : ''
  ].filter(Boolean).join(' ');

  // Получаем текст для заголовка с информацией о фильтрах
  const getHeaderText = () => {
    let text = 'Статистика полетов по регионам';
    
    if (dateRange?.date_from || dateRange?.date_to) {
      text += ' (';
      if (dateRange.date_from && dateRange.date_to) {
        text += `${dateRange.date_from} - ${dateRange.date_to}`;
      } else if (dateRange.date_from) {
        text += `с ${dateRange.date_from}`;
      } else if (dateRange.date_to) {
        text += `по ${dateRange.date_to}`;
      }
      text += ')';
    }
    
    return text;
  };

  return (
    <div className={drawerClassNames} style={{ width }}>
      <Resizable
        width={width}
        height={Infinity}
        onResize={handleResize}
        handle={
          <div 
            className={styles.resizeHandle} 
            title="Перетащите для изменения ширины"
          />
        }
        minConstraints={[600, Infinity]}
        maxConstraints={[window.innerWidth - 50, Infinity]}
        axis="x"
        resizeHandles={['w']}
      >
        <div className={styles.drawerContent}>
          <div className={styles.header}>
            <h2>{getHeaderText()}</h2>
            {onClose && (
              <button 
                className={styles.closeButton}
                onClick={handleClose}
                aria-label="Закрыть панель"
              >
                ×
              </button>
            )}
          </div>
          <div className={styles.scrollableContent}>
            {isChartLoading ? (
              <div className={styles.loaderContainer}>
                <Loader />
              </div>
            ) : (
              <div className={styles.chartContainer}>
                <RegionsBarChart 
                  data={regionsData} 
                  onChartLoaded={handleChartLoaded} 
                />
              </div>
            )}
          </div>
        </div>
      </Resizable>
    </div>
  );
});

ResizableDrawer.displayName = 'ResizableDrawer';

export default ResizableDrawer;