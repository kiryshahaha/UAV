import React, { useState, useEffect, memo } from 'react';
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
import styles from './RegionBarChart.module.css';
import Loader from '../loader/Loader';
import { useTable } from "@/contexts/TableContext"; 

// Регистрируем компоненты Chart.js
ChartJS.register(CategoryScale, LinearScale, BarElement, Title, Tooltip, Legend);

const RegionBarChart = memo(({ regionName, dateRange, onLoad, onStartLoading }) => {
  const [chartData, setChartData] = useState(null);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState(null);
  const { tableVersion, currentTable } = useTable();

  // Определяем цвета в зависимости от темы
  const isDarkTheme = document.documentElement.dataset.theme === 'dark';
  const textColor = isDarkTheme ? '#e9ecef' : '#333';
  const gridColor = isDarkTheme ? 'rgba(255, 255, 255, 0.1)' : 'rgba(0, 0, 0, 0.1)';

  // Функция для создания фиолетовых цветов с градиентом
  const getPurpleColors = (data, alpha = 0.8) => {
    if (!data || data.length === 0) return [];
    
    const maxValue = Math.max(...data.map(d => d.flight_count));
    
    return data.map(d => {
      // Создаем градиент от светлого к темному фиолетовому
      const intensity = maxValue > 0 ? d.flight_count / maxValue : 0;
      
      if (isDarkTheme) {
        // Темная тема: от светлого лавандового к темному фиолетовому
        const hue = 270; // Фиолетовый
        const saturation = 70;
        const lightness = 80 - (intensity * 40); // От светлого к темному
        return `hsla(${hue}, ${saturation}%, ${lightness}%, ${alpha})`;
      } else {
        // Светлая тема: от светлого лавандового к темному фиолетовому
        const hue = 270; // Фиолетовый
        const saturation = 60;
        const lightness = 90 - (intensity * 30); // От светлого к темному
        return `hsla(${hue}, ${saturation}%, ${lightness}%, ${alpha})`;
      }
    });
  };

  const fetchData = async () => {
    if (!regionName || !currentTable) { // Добавлена проверка на currentTable
      setChartData(null);
      setIsLoading(false);
      if (onLoad) onLoad();
      return;
    }

    if (onStartLoading) {
      onStartLoading();
    }

    try {
      setIsLoading(true);
      setError(null);

      // Строим URL с параметрами даты
      const url = new URL('/api/regions/monthly', window.location.origin);
      if (dateRange?.date_from) url.searchParams.append('date_from', dateRange.date_from);
      if (dateRange?.date_to) url.searchParams.append('date_to', dateRange.date_to);
      
      // Добавляем информацию о таблице
      url.searchParams.append('_v', tableVersion);
      url.searchParams.append('_t', Date.now());

      const sessionId = localStorage.getItem('session_id');
      const headers = {};
      
      if (sessionId) {
        headers['X-Session-ID'] = sessionId;
      }

      console.log('📊 Запрос месячной статистики региона:', { 
        regionName, 
        dateFrom: dateRange?.date_from, 
        dateTo: dateRange?.date_to 
      });

      const response = await fetch(url.toString(), {
        headers: headers
      });
      
      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`);
      }
      
      const allRegionsData = await response.json();
      
      if (!allRegionsData.regions || allRegionsData.regions.length === 0) {
        setChartData(null);
        return;
      }
      
      // Ищем данные для конкретного региона
      const regionData = allRegionsData.regions.find(region => 
        region.region === regionName
      );

      if (!regionData) {
        setChartData(null);
        return;
      }
      
      setChartData(regionData);
    } catch (error) {
      console.error('Ошибка загрузки данных региона:', error);
      setError(error.message);
    } finally {
      setIsLoading(false);
      if (onLoad) onLoad();
    }
  };

  useEffect(() => {
    fetchData();
  }, [regionName, dateRange, tableVersion, currentTable]);

  // Получаем текст для заголовка с информацией о фильтрах
  const getChartTitle = () => {
    let title = `Статистика полетов - ${regionName}`;
    
    if (dateRange?.date_from || dateRange?.date_to) {
      title += ' (';
      if (dateRange.date_from && dateRange.date_to) {
        title += `${dateRange.date_from} - ${dateRange.date_to}`;
      } else if (dateRange.date_from) {
        title += `с ${dateRange.date_from}`;
      } else if (dateRange.date_to) {
        title += `по ${dateRange.date_to}`;
      }
      title += ')';
    }
    
    return title;
  };

  // Опции графика
  const options = {
    indexAxis: 'x',
    responsive: true,
    maintainAspectRatio: false,
    plugins: {
      legend: {
        display: false,
      },
      title: {
        display: true,
        text: getChartTitle(),
        color: textColor,
        font: {
          size: 16,
          weight: '600'
        },
        padding: {
          bottom: 20
        }
      },
      tooltip: {
        backgroundColor: isDarkTheme ? '#495057' : '#ffffff',
        titleColor: textColor,
        bodyColor: textColor,
        borderColor: isDarkTheme ? '#6c757d' : '#ddd',
        borderWidth: 1,
        callbacks: {
          label: (context) => `Полетов: ${context.raw}`,
        },
      },
    },
    scales: {
      x: {
        ticks: { 
          color: textColor,
          font: { size: 12 },
        },
        grid: {
          color: gridColor,
        },
        title: {
          display: true,
          text: 'Месяцы',
          color: textColor,
          font: { size: 14 },
        },
      },
      y: {
        beginAtZero: true,
        ticks: {
          color: textColor,
          font: { size: 12 },
        },
        grid: {
          color: gridColor,
        },
        title: {
          display: true,
          text: 'Количество полетов',
          color: textColor,
          font: { size: 14 },
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

  // Подготовка данных для графика
  const getChartData = () => {
    if (!chartData || !chartData.monthly_stats || chartData.monthly_stats.length === 0) {
      return {
        labels: [],
        datasets: []
      };
    }

    const monthlyData = chartData.monthly_stats;
    const backgroundColors = getPurpleColors(monthlyData, 0.7);
    const borderColors = getPurpleColors(monthlyData, 1);

    return {
      labels: monthlyData.map(d => d.month),
      datasets: [
        {
          label: 'Количество полетов',
          data: monthlyData.map(d => d.flight_count),
          backgroundColor: backgroundColors,
          borderColor: borderColors,
          borderWidth: 1,
          borderRadius: 4,
        },
      ],
    };
  };

  // Расчет общего количества полетов
  const getTotalFlights = () => {
    if (!chartData || !chartData.monthly_stats) return 0;
    return chartData.monthly_stats.reduce((sum, d) => sum + d.flight_count, 0);
  };

  // Состояние загрузки
  if (isLoading) {
    return (
      <div className={styles.loaderContainer}>
        <Loader />
      </div>
    );
  }

  // Состояние ошибки
  if (error) {
    return (
      <div className={styles.errorContainer}>
        <div className={styles.errorTitle}>Ошибка загрузки данных</div>
        <div className={styles.errorMessage}>{error}</div>
      </div>
    );
  }

  // Регион не найден
  if (!chartData) {
    return (
      <div className={styles.emptyContainer}>
        <div className={styles.emptyTitle}>Информация по региону не найдена</div>
        <div className={styles.regionName}>Регион: {regionName}</div>
        <div className={styles.emptySubtitle}>Нет данных о полетах за выбранный период</div>
      </div>
    );
  }

  // Нет данных для отображения
  if (!chartData.monthly_stats || chartData.monthly_stats.length === 0) {
    return (
      <div className={styles.emptyContainer}>
        <div className={styles.emptyTitle}>Нет данных для отображения</div>
        <div className={styles.emptySubtitle}>Попробуйте изменить параметры фильтрации</div>
      </div>
    );
  }

  return (
    <div className={styles.container}>
      <div className={styles.chartWrapper}>
        <div className={styles.totalFlights}>
          Всего полетов: {getTotalFlights()}
        </div>
        <Bar 
          data={getChartData()} 
          options={options} 
          className={styles.chart}
        />
      </div>
    </div>
  );
});

RegionBarChart.displayName = 'RegionBarChart';
export default RegionBarChart;