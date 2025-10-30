"use client";

import React, { useState, useEffect } from "react";
import {
    Chart as ChartJS,
    CategoryScale,
    LinearScale,
    BarElement,
    Title,
    Tooltip,
    Legend,
} from 'chart.js';
import { Bar } from 'react-chartjs-2';
import styles from './RegionDashboardPopup.module.css';
import { useTable } from "@/contexts/TableContext"

ChartJS.register(
    CategoryScale,
    LinearScale,
    BarElement,
    Title,
    Tooltip,
    Legend
);

const RegionDashboardPopup = ({ isOpen, onClose, dateRange, regionName }) => {
    const [dashboardData, setDashboardData] = useState(null);
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState(null);
    const [isMobile, setIsMobile] = useState(false);
    const { tableVersion, currentTable } = useTable();

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

    // Загружаем данные для конкретного региона
    useEffect(() => {
        if (!isOpen || !regionName || !currentTable) return; 

        const fetchRegionData = async () => {
            setLoading(true);
            setError(null);

            try {
                console.log("🔄 Загрузка данных для региона:", regionName);

                const params = new URLSearchParams();
                if (dateRange?.date_from) params.append('date_from', dateRange.date_from);
                if (dateRange?.date_to) params.append('date_to', dateRange.date_to);
                params.append('region', regionName);

                params.append('_v', tableVersion);
                params.append('_t', Date.now());

                const sessionId = localStorage.getItem('session_id');
                const headers = {};

                if (sessionId) {
                    headers['X-Session-ID'] = sessionId;
                }

                const response = await fetch(`/api/dashboard/stats?${params.toString()}`, {
                    headers: headers
                });

                if (!response.ok) {
                    throw new Error('Ошибка загрузки данных региона');
                }

                const data = await response.json();
                setDashboardData(data);
                console.log("✅ Данные региона загружены:", regionName);
            } catch (err) {
                console.error("❌ Ошибка загрузки данных региона:", err);
                setError(err.message);
            } finally {
                setLoading(false);
            }
        };

        fetchRegionData();
    }, [isOpen, regionName, dateRange, tableVersion, currentTable]);

    // Форматирование времени (минуты в часы и минуты)
    const formatDuration = (minutes) => {
        if (!minutes) return <>0<span className={styles.unitSmall}>мин</span></>;
        const hours = Math.floor(minutes / 60);
        const mins = Math.round(minutes % 60);

        if (hours > 0) {
            return <>{hours}<span className={styles.unitSmall}>ч</span> {mins}<span className={styles.unitSmall}>мин</span></>;
        }
        return <>{mins}<span className={styles.unitSmall}>мин</span></>;
    };

    // Подготовка данных для гистограммы операторов
    const getOperatorsChartData = () => {
        if (!dashboardData?.operators || dashboardData.operators.length === 0) {
            return null;
        }

        // Группируем операторов по типам
        const operatorTypes = {
            'ООО': 0,
            'ИП': 0,
            'Физлицо': 0,
            'Другое': 0
        };

        dashboardData.operators.forEach(operator => {
            const type = operator.type || 'Другое';
            operatorTypes[type] = (operatorTypes[type] || 0) + operator.flight_count;
        });

        // Фильтруем пустые категории
        const labels = [];
        const data = [];
        const backgroundColors = [
            '#00C0E8', // ООО - синий
            '#4CAF50', // ИП - зеленый
            '#FF9800', // Физлицо - оранжевый
            '#9C27B0'  // Другое - фиолетовый
        ];

        Object.entries(operatorTypes).forEach(([type, count], index) => {
            if (count > 0) {
                labels.push(type);
                data.push(count);
            }
        });

        // Рассчитываем проценты
        const totalFlights = data.reduce((sum, value) => sum + value, 0);
        const percentages = data.map(value =>
            totalFlights > 0 ? Math.round((value / totalFlights) * 100) : 0
        );

        return {
            labels,
            datasets: [
                {
                    label: 'Количество полетов',
                    data: data,
                    backgroundColor: backgroundColors.slice(0, labels.length),
                    borderColor: backgroundColors.slice(0, labels.length),
                    borderWidth: 1,
                    borderRadius: 4,
                }
            ],
            percentages
        };
    };

    // Настройки для гистограммы
    const chartOptions = {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
            legend: {
                display: false,
            },
            tooltip: {
                callbacks: {
                    label: function (context) {
                        const value = context.parsed.y;
                        const total = context.dataset.data.reduce((a, b) => a + b, 0);
                        const percentage = total > 0 ? Math.round((value / total) * 100) : 0;
                        return `${value} полетов (${percentage}%)`;
                    }
                }
            }
        },
        scales: {
            y: {
                beginAtZero: true,
                ticks: {
                    color: '#666',
                    font: {
                        size: isMobile ? 8 : 10
                    },
                    maxTicksLimit: 5
                },
                grid: {
                    color: '#f0f0f0'
                }
            },
            x: {
                ticks: {
                    color: '#000',
                    font: {
                        size: isMobile ? 8 : 10,
                        weight: 'bold'
                    },
                    maxRotation: isMobile ? 90 : 45,
                    minRotation: isMobile ? 90 : 45
                },
                grid: {
                    display: false
                }
            }
        },
        layout: {
            padding: {
                left: 5,
                right: 5,
                top: 5,
                bottom: 5
            }
        }
    };

    const operatorsChartData = getOperatorsChartData();

    if (!isOpen) return null;

    return (
        <div className={styles.overlay}>
            <div className={styles.popup}>
                {/* Заголовок */}
                <div className={styles.header}>
                    <div>
                        <h2 className={styles.title}>Статистика региона</h2>
                        <p className={styles.subtitle}>{regionName}</p>
                    </div>
                    <button
                        onClick={onClose}
                        className={styles.closeButton}
                    >
                        ×
                    </button>
                </div>

                {/* Содержимое */}
                <div className={styles.content}>
                    {loading && (
                        <div className={styles.loading}>
                            <div className={styles.loadingContent}>
                                <div className={styles.loadingIcon}></div>
                                <p>Загрузка данных...</p>
                            </div>
                        </div>
                    )}

                    {error && (
                        <div className={styles.error}>
                            <p>Ошибка: {error}</p>
                            <button
                                onClick={() => window.location.reload()}
                                className={styles.retryButton}
                            >
                                Попробовать снова
                            </button>
                        </div>
                    )}

                    {dashboardData && !loading && (
                        <>
                            {/* Общая статистика */}
                            <div className={styles.totalStats}>
                                <div className={styles.totalStatItem}>
                                    <div className={styles.totalStatValue}>
                                        {dashboardData.general_stats?.total_flights || 0}
                                    </div>
                                    <div className={styles.totalStatLabel}>Всего полетов</div>
                                </div>
                                <div className={styles.totalStatItem}>
                                    <div className={styles.totalStatValue}>
                                        {dashboardData.general_stats?.total_operators || 0}
                                    </div>
                                    <div className={styles.totalStatLabel}>Операторов</div>
                                </div>
                                <div className={styles.totalStatItem}>
                                    <div className={styles.totalStatValue}>
                                        {dashboardData.general_stats?.total_aircrafts || 0}
                                    </div>
                                    <div className={styles.totalStatLabel}>Уникальных БПЛА</div>
                                </div>
                            </div>

                            {/* Основные метрики */}
                            <div className={styles.statsGrid}>
                                {/* Средняя продолжительность */}
                                <div className={styles.statCard}>
                                    <h3>Средняя продолжительность</h3>
                                    <div className={`${styles.statValue} ${styles.colorBlue}`}>
                                        {formatDuration(dashboardData.duration_stats?.avg_duration_minutes)}
                                    </div>
                                    <p className={styles.statDescription}>
                                        Медиана: {formatDuration(dashboardData.duration_stats?.median_duration_minutes)}
                                    </p>
                                </div>

                                {/* Статистика по высоте */}
                                <div className={styles.statCard}>
                                    <h3>Высота полетов</h3>
                                    <div className={`${styles.statValue} ${styles.colorOrange}`}>
                                        {Math.round(dashboardData.level_stats?.avg_level || 0)} м
                                    </div>
                                    <p className={styles.statDescription}>
                                        Медиана: {Math.round(dashboardData.level_stats?.median_level || 0)} м
                                        <br />
                                        Макс: {Math.round(dashboardData.level_stats?.max_level || 0)} м
                                    </p>
                                </div>

                                {/* Статистика по радиусу */}
                                <div className={styles.statCard}>
                                    <h3>Радиус полетов</h3>
                                    <div className={`${styles.statValue} ${styles.colorPurple}`}>
                                        {Math.round(dashboardData.radius_stats?.avg_radius || 0)} м
                                    </div>
                                    <p className={styles.statDescription}>
                                        Медиана: {Math.round(dashboardData.radius_stats?.median_radius || 0)} м
                                        <br />
                                        Макс: {Math.round(dashboardData.radius_stats?.max_radius || 0)} м
                                    </p>
                                </div>
                            </div>

                            {/* График операторов */}
                            {operatorsChartData && (
                                <div className={styles.chartSection}>
                                    <h3>Распределение операторов по типам</h3>
                                    <div className={styles.chartContainer}>
                                        <Bar
                                            data={operatorsChartData}
                                            options={chartOptions}
                                        />
                                    </div>
                                    <div className={styles.chartLegend}>
                                        {operatorsChartData.labels.map((label, index) => (
                                            <div key={label} className={styles.legendItem}>
                                                <div
                                                    className={styles.legendColor}
                                                    style={{
                                                        backgroundColor: operatorsChartData.datasets[0].backgroundColor[index]
                                                    }}
                                                ></div>
                                                <span className={styles.legendLabel}>{label}</span>
                                                <span className={styles.legendPercentage}>
                                                    {operatorsChartData.percentages[index]}%
                                                </span>
                                            </div>
                                        ))}
                                    </div>
                                </div>
                            )}

                            {/* Топ операторов */}
                            {dashboardData.operators && dashboardData.operators.length > 0 && (
                                <div className={styles.operatorsSection}>
                                    <h3>Топ операторов</h3>
                                    <div className={styles.operatorsGrid}>
                                        {dashboardData.operators.slice(0, 6).map((operator, index) => (
                                            <div key={index} className={styles.operatorCard}>
                                                <div className={styles.operatorName}>
                                                    {operator.name}
                                                </div>
                                                <div className={styles.operatorDetails}>
                                                    <span>Полетов: {operator.flight_count}</span>
                                                    <span>Тип: {operator.type}</span>
                                                </div>
                                            </div>
                                        ))}
                                    </div>
                                </div>
                            )}

                            {/* Типы БПЛА */}
                            {dashboardData.aircraft_types && dashboardData.aircraft_types.length > 0 && (
                                <div className={styles.uavSection}>
                                    <h3>Типы БПЛА</h3>
                                    <div className={styles.uavList}>
                                        {dashboardData.aircraft_types.slice(0, 8).map((type, index) => {
                                            const totalFlights = dashboardData.general_stats?.total_flights || 0;
                                            const percentage = totalFlights > 0 ?
                                                Math.round((type.count / totalFlights) * 100) : 0;

                                            return (
                                                <div key={index} className={styles.uavItem}>
                                                    <div
                                                        className={styles.uavDot}
                                                        style={{ backgroundColor: getColorByIndex(index) }}
                                                    ></div>
                                                    <span className={styles.uavName}>
                                                        {type.type || "Не указан"}
                                                    </span>
                                                    <div className={styles.uavBarContainer}>
                                                        <div
                                                            className={styles.uavBar}
                                                            style={{
                                                                background: getColorByIndex(index),
                                                                width: `${Math.max(5, percentage)}%`
                                                            }}
                                                        ></div>
                                                    </div>
                                                    <span className={styles.uavPercentage}>
                                                        {percentage}% ({type.count})
                                                    </span>
                                                </div>
                                            );
                                        })}
                                    </div>
                                </div>
                            )}
                        </>
                    )}
                </div>
            </div>
        </div>
    );
};

// Функция для получения цвета по индексу
const getColorByIndex = (index) => {
    const colors = ['#3b82f6', '#10b981', '#f59e0b', '#ef4444', '#8b5cf6', '#f97316', '#84cc16', '#06b6d4'];
    return colors[index % colors.length];
};

export default RegionDashboardPopup;