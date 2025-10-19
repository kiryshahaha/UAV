import React, { useState, useEffect } from 'react';
import { Bar } from 'react-chartjs-2';
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  BarElement,
  Title,
  Tooltip,
  Legend,
} from 'chart.js';
import styles from '../ResizableDrawer/ResizableDrawer.module.css';


// Регистрируем компоненты Chart.js
ChartJS.register(CategoryScale, LinearScale, BarElement, Title, Tooltip, Legend);

const ITEMS_PER_PAGE = 10;

const RegionsBarChart = ({ data, onChartLoaded }) => {
  const [page, setPage] = useState(0);
  const [isLoading, setIsLoading] = useState(true);

  // Сортируем все данные по убыванию количества полетов
  const sortedAllData = data
    ? [...data].sort((a, b) => (b.num_flights || 0) - (a.num_flights || 0))
    : [];

  // Берем только данные для текущей страницы
  const currentPageData = sortedAllData.slice(
    page * ITEMS_PER_PAGE,
    (page + 1) * ITEMS_PER_PAGE
  );

  // Находим минимальное и максимальное время полета для нормализации
  const flightDurations = sortedAllData
    .filter(item => item.avg_flight_duration)
    .map(item => item.avg_flight_duration);
  
  const minDuration = Math.min(...flightDurations);
  const maxDuration = Math.max(...flightDurations);
  const durationRange = maxDuration - minDuration;

  // Функция для получения цвета в зависимости от времени полета
  const getColorByDuration = (duration, alpha = 0.8) => {
    if (!duration || durationRange === 0) {
      return `rgba(75, 192, 192, ${alpha})`; // Цвет по умолчанию
    }

    // Нормализуем время полета от 0 до 1
    const normalized = (duration - minDuration) / durationRange;
    
    // Для светлой темы - от светлого к темному оттенку сине-зеленого
    // Для темной темы - от светлого к темному оттенку бирюзового
    const isDarkTheme = document.documentElement.dataset.theme === 'dark';
    
    if (isDarkTheme) {
      // Темная тема: от светлого бирюзового к темному
      const hue = 180; // Бирюзовый
      const saturation = 70;
      const lightness = 70 - (normalized * 40); // От светлого к темному
      return `hsla(${hue}, ${saturation}%, ${lightness}%, ${alpha})`;
    } else {
      // Светлая тема: от светлого сине-зеленого к темному
      const hue = 160; // Сине-зеленый
      const saturation = 60;
      const lightness = 80 - (normalized * 40); // От светлого к темному
      return `hsla(${hue}, ${saturation}%, ${lightness}%, ${alpha})`;
    }
  };

  // Определяем цвета в зависимости от темы
  const isDarkTheme = document.documentElement.dataset.theme === 'dark';
  const textColor = isDarkTheme ? '#e9ecef' : '#333';

  // Данные для графика с градиентной окраской
  const chartData = {
    labels: currentPageData.map(d => d.region || 'Не указан'),
    datasets: [
      {
        label: 'Количество полетов',
        data: currentPageData.map(d => d.num_flights || 0),
        backgroundColor: currentPageData.map(d => 
          getColorByDuration(d.avg_flight_duration, 0.7)
        ),
        borderColor: currentPageData.map(d => 
          getColorByDuration(d.avg_flight_duration, 1)
        ),
        borderWidth: 1,
        borderRadius: 4,
      },
    ],
  };

  // Опции графика
  const options = {
    indexAxis: 'y',
    responsive: true,
    maintainAspectRatio: false,
    plugins: {
      legend: {
        position: 'top',
        labels: { 
          color: textColor,
          font: { size: 14 },
        },
      },
      title: {
        display: true,
        text: `Полеты по регионам (${page * ITEMS_PER_PAGE + 1}-${Math.min((page + 1) * ITEMS_PER_PAGE, sortedAllData.length)} из ${sortedAllData.length})`,
        color: textColor,
        font: { size: 16 },
      },
      tooltip: {
        backgroundColor: isDarkTheme ? '#495057' : '#ffffff',
        titleColor: textColor,
        bodyColor: textColor,
        borderColor: isDarkTheme ? '#6c757d' : '#ddd',
        borderWidth: 1,
        callbacks: {
          label: (context) => `${context.dataset.label}: ${context.raw}`,
          afterLabel: (context) => {
            const index = context.dataIndex;
            const avgDuration = currentPageData[index]?.avg_flight_duration || 0;
            const durationText = avgDuration > 0 ? 
              `Среднее время: ${avgDuration.toFixed(1)} мин` : 
              'Данные о времени отсутствуют';
            return durationText;
          },
        },
      },
    },
    scales: {
      x: {
        beginAtZero: true,
        ticks: { 
          color: textColor,
          font: { size: 12 },
        },
        grid: {
          color: isDarkTheme ? 'rgba(255, 255, 255, 0.1)' : 'rgba(0, 0, 0, 0.1)',
        },
        title: {
          display: true,
          text: 'Количество полетов',
          color: textColor,
          font: { size: 14 },
        },
      },
      y: {
        ticks: {
          color: textColor,
          autoSkip: false,
          font: { 
            size: 12,
          },
          padding: 8,
        },
        grid: { display: false },
        afterFit: function(scale) {
          scale.width = Math.max(scale.width, 150);
        },
      },
    },
    layout: {
      padding: {
        left: 10,
        right: 10,
        top: 10,
        bottom: 10
      }
    }
  };

  // Управление состоянием загрузки
  useEffect(() => {
    if (data !== undefined) {
      setIsLoading(false);
      if (onChartLoaded) {
        onChartLoaded();
      }
    }
  }, [data, onChartLoaded]);

  // Сбрасываем страницу при изменении данных
  useEffect(() => {
    setPage(0);
  }, [data]);

  // Состояние загрузки - НЕ ПОКАЗЫВАЕМ НИЧЕГО во время загрузки
  if (isLoading) {
    return null;
  }

  // Обработка пустого состояния (только когда данные загружены и пустые)
  if (!data || data.length === 0) {
    return (
      <div style={{ 
        display: 'flex', 
        alignItems: 'center', 
        justifyContent: 'center', 
        height: '100%',
        color: textColor 
      }}>
        Нет данных для отображения
      </div>
    );
  }

  // Обработка ошибки
  if (data.error) {
    return (
      <div style={{ 
        display: 'flex', 
        flexDirection: 'column',
        alignItems: 'center', 
        justifyContent: 'center', 
        height: '100%',
        color: textColor,
        textAlign: 'center'
      }}>
        <div style={{ fontSize: '18px', marginBottom: '10px' }}>
          Ошибка загрузки данных
        </div>
        <div style={{ fontSize: '14px' }}>{data.error}</div>
      </div>
    );
  }

  const totalPages = Math.ceil(sortedAllData.length / ITEMS_PER_PAGE);

const getArrowColor = (isDisabled) => {
  return isDisabled ? 'rgba(255, 255, 255, 0.5)' : 'white';
};

  // Стили для кнопок пагинации
   const paginationButtonStyles = {
    base: {
      padding: '10px 20px',
      border: 'none',
      borderRadius: '8px',
      fontSize: '14px',
      fontWeight: '500',
      cursor: 'pointer',
      transition: 'all 0.3s ease',
      display: 'flex',
      alignItems: 'center',
      gap: '8px',
      fontFamily: 'inherit',
      transform: 'translateY(0)',
      boxShadow: isDarkTheme 
        ? '0 2px 8px rgba(59, 130, 246, 0.3)'
        : '0 2px 8px rgba(59, 130, 246, 0.2)',
    },
    enabled: {
      backgroundColor: isDarkTheme ? 'rgba(59, 130, 246, 0.8)' : 'rgba(59, 130, 246, 0.9)',
      color: 'white',
    },
    disabled: {
      backgroundColor: isDarkTheme ? 'rgba(107, 114, 128, 0.3)' : 'rgba(156, 163, 175, 0.3)',
      color: isDarkTheme ? 'rgba(156, 163, 175, 0.5)' : 'rgba(107, 114, 128, 0.5)',
      cursor: 'not-allowed',
      boxShadow: 'none',
    },
    hover: {
      backgroundColor: isDarkTheme ? 'rgba(37, 99, 235, 0.9)' : 'rgba(37, 99, 235, 1)',
      transform: 'translateY(-1px)',
      boxShadow: isDarkTheme 
        ? '0 4px 12px rgba(59, 130, 246, 0.4)'
        : '0 4px 12px rgba(59, 130, 246, 0.3)',
    }
  };

   return (
    <div style={{ 
      display: 'flex', 
      flexDirection: 'column', 
      height: '100%',
      minHeight: 0 
    }}>
      {/* График занимает все доступное пространство */}
      <div style={{ flex: 1, minHeight: 0 }}>
        <Bar data={chartData} options={options} />
      </div>
      
      {/* Легенда для времени полета */}
      {durationRange > 0 && (
        <div style={{ 
          display: 'flex', 
          alignItems: 'center', 
          justifyContent: 'center',
          gap: '10px',
          marginTop: '10px',
          fontSize: '12px',
          color: textColor
        }}>
          <span>Меньше времени в воздухе</span>
          <div style={{ display: 'flex', alignItems: 'center', gap: '2px' }}>
            {[0, 0.25, 0.5, 0.75, 1].map((normalized, index) => (
              <div
                key={index}
                style={{
                  width: '20px',
                  height: '20px',
                  backgroundColor: getColorByDuration(minDuration + (normalized * durationRange)),
                  border: `1px solid ${getColorByDuration(minDuration + (normalized * durationRange), 1)}`,
                  borderRadius: '2px'
                }}
                title={`~${Math.round(minDuration + (normalized * durationRange))} мин`}
              />
            ))}
          </div>
          <span>Больше времени в воздухе</span>
        </div>
      )}

      {/* Пагинация внизу */}
      {sortedAllData.length > ITEMS_PER_PAGE && (
        <div style={{ 
          display: 'flex', 
          alignItems: 'center', 
          justifyContent: 'center', 
          gap: '16px',
          marginTop: '20px',
          padding: '16px'
        }}>
          <button
            onClick={() => setPage(p => Math.max(0, p - 1))}
            disabled={page === 0}
            aria-label="Предыдущая страница"
            style={{
              ...paginationButtonStyles.base,
              ...(page === 0 ? paginationButtonStyles.disabled : paginationButtonStyles.enabled)
            }}
            onMouseOver={(e) => {
              if (page !== 0) {
                e.target.style.backgroundColor = paginationButtonStyles.hover.backgroundColor;
                e.target.style.transform = paginationButtonStyles.hover.transform;
                e.target.style.boxShadow = paginationButtonStyles.hover.boxShadow;
              }
            }}
            onMouseOut={(e) => {
              if (page !== 0) {
                e.target.style.backgroundColor = paginationButtonStyles.enabled.backgroundColor;
                e.target.style.transform = paginationButtonStyles.base.transform;
                e.target.style.boxShadow = paginationButtonStyles.base.boxShadow;
              }
            }}
          >
            <svg 
              width="16" 
              height="16" 
              viewBox="0 0 24 24" 
              style={{ 
                pointerEvents: 'none',
                display: 'block'
              }}
            >
              <path 
                d="M15 18l-6-6 6-6" 
                fill="none" 
                stroke="currentColor" 
                strokeWidth="2"
                style={{ pointerEvents: 'none' }}
              />
            </svg>
            Назад
          </button>
          
          <span style={{ 
            color: textColor, 
            margin: '0 16px',
            fontSize: '14px',
            fontWeight: '500',
            minWidth: '120px',
            textAlign: 'center'
          }}>
            Страница {page + 1} из {totalPages}
          </span>
          
          <button
            onClick={() => setPage(p => Math.min(totalPages - 1, p + 1))}
            disabled={page >= totalPages - 1}
            aria-label="Следующая страница"
            style={{
              ...paginationButtonStyles.base,
              ...(page >= totalPages - 1 ? paginationButtonStyles.disabled : paginationButtonStyles.enabled)
            }}
            onMouseOver={(e) => {
              if (page < totalPages - 1) {
                e.target.style.backgroundColor = paginationButtonStyles.hover.backgroundColor;
                e.target.style.transform = paginationButtonStyles.hover.transform;
                e.target.style.boxShadow = paginationButtonStyles.hover.boxShadow;
              }
            }}
            onMouseOut={(e) => {
              if (page < totalPages - 1) {
                e.target.style.backgroundColor = paginationButtonStyles.enabled.backgroundColor;
                e.target.style.transform = paginationButtonStyles.base.transform;
                e.target.style.boxShadow = paginationButtonStyles.base.boxShadow;
              }
            }}
          >
            Вперед
            <svg 
              width="16" 
              height="16" 
              viewBox="0 0 24 24" 
              style={{ 
                pointerEvents: 'none',
                display: 'block'
              }}
            >
              <path 
                d="M9 18l6-6-6-6" 
                fill="none" 
                stroke="currentColor" 
                strokeWidth="2"
                style={{ pointerEvents: 'none' }}
              />
            </svg>
          </button>
        </div>
      )}
    </div>
  );
};

export default React.memo(RegionsBarChart);