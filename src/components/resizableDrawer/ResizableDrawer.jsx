import React,{ useState, useEffect, useCallback, memo } from 'react';
import { Resizable } from 'react-resizable';
import styles from './ResizableDrawer.module.css';
import RegionsBarChart from '../RegionsBarChart/RegionsBarChart';

const ResizableDrawer = memo(({ onClose, isOpen }) => {
  const [width, setWidth] = useState(0);
  const [isClosing, setIsClosing] = useState(false);

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
            <h2>Статистика полетов по регионам</h2>
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
            <RegionsBarChart />
          </div>
        </div>
      </Resizable>
    </div>
  );
});

ResizableDrawer.displayName = 'ResizableDrawer';

export default ResizableDrawer;