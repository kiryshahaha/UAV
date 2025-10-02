// components/RegionDrawer.jsx
import React,{ useState, useEffect, useCallback, memo } from 'react';
import { Resizable } from 'react-resizable';
import styles from '../resizableDrawer/ResizableDrawer.module.css';
import RegionBarChart from '../RegionChart/RegionBarChart';
import Loader from '../loader/Loader';

const RegionDrawer = memo(({ onClose, isOpen, regionName }) => {
  const [width, setWidth] = useState(0);
  const [isClosing, setIsClosing] = useState(false);
  const [isChartLoading, setIsChartLoading] = useState(true);

  useEffect(() => {
    const initialWidth = Math.min(window.innerWidth / 2, 800);
    setWidth(initialWidth);
  }, []);

  const handleResize = useCallback((e, { size }) => {
    const minWidth = 500;
    const maxWidth = window.innerWidth - 100;
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

  const handleChartLoad = useCallback(() => {
    setIsChartLoading(false);
  }, []);

  const handleChartStartLoading = useCallback(() => {
    setIsChartLoading(true);
  }, []);

  const drawerClassNames = [
    styles.drawer,
    isClosing ? styles.slideOut : '',
    !isOpen && !isClosing ? styles.hidden : ''
  ].filter(Boolean).join(' ');

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
        minConstraints={[500, Infinity]}
        maxConstraints={[window.innerWidth - 100, Infinity]}
        axis="x"
        resizeHandles={['w']}
      >
        <div className={styles.drawerContent}>
          <div className={styles.header}>
            <h2>Статистика полетов - {regionName} регион</h2>
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
          <div className={styles.chartContainer}>
            {isChartLoading && <Loader />}
            <RegionBarChart 
              regionName={regionName} 
              onLoad={handleChartLoad}
              onStartLoading={handleChartStartLoading}
            />
          </div>
        </div>
      </Resizable>
    </div>
  );
});

RegionDrawer.displayName = 'RegionDrawer';

export default RegionDrawer;