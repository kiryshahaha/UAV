"use client";

import React, { useState, useEffect, useRef } from "react";
import Image from "next/image";
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
import styles from "./DashboardPopup.module.css";
import { useTable } from "@/contexts/TableContext";

ChartJS.register(
    CategoryScale,
    LinearScale,
    BarElement,
    Title,
    Tooltip,
    Legend
);

const DashboardPopup = ({ isOpen, onClose, dateRange, selectedRegion, darkMode = false }) => {
    const [dashboardData, setDashboardData] = useState(null);
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState(null);
    const [isMobile, setIsMobile] = useState(false);
    const popupRef = useRef(null);

    const { tableVersion, currentTable } = useTable();

    // Определяем мобильное устройство
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


    useEffect(() => {
        if (!isOpen || !currentTable) return;

        const fetchDashboardData = async () => {
            setLoading(true);
            setError(null);

            try {
                console.log("🔄 Загрузка дашборда для таблицы:", currentTable?.table_name);

                const params = new URLSearchParams();
                if (dateRange?.date_from) params.append('date_from', dateRange.date_from);
                if (dateRange?.date_to) params.append('date_to', dateRange.date_to);
                if (selectedRegion) params.append('region', selectedRegion);

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
                    throw new Error('Ошибка загрузки данных дашборда');
                }

                const data = await response.json();
                setDashboardData(data);
            } catch (err) {
                console.error('Ошибка загрузки дашборда:', err);
                setError(err.message);
            } finally {
                setLoading(false);
            }
        };

        fetchDashboardData();
    }, [isOpen, dateRange, selectedRegion, tableVersion, currentTable]);

    // Обработчик клика вне попапа
    useEffect(() => {
        const handleClickOutside = (event) => {
            if (popupRef.current && !popupRef.current.contains(event.target)) {
                onClose();
            }
        };

        if (isOpen) {
            document.addEventListener('mousedown', handleClickOutside);
            document.addEventListener('touchstart', handleClickOutside);
        }

        return () => {
            document.removeEventListener('mousedown', handleClickOutside);
            document.removeEventListener('touchstart', handleClickOutside);
        };
    }, [isOpen, onClose]);

    const fetchDashboardData = async () => {
        setLoading(true);
        setError(null);

        try {
            const params = new URLSearchParams();
            if (dateRange?.date_from) params.append('date_from', dateRange.date_from);
            if (dateRange?.date_to) params.append('date_to', dateRange.date_to);
            if (selectedRegion) params.append('region', selectedRegion);

            const response = await fetch(`/api/dashboard/stats?${params.toString()}`);

            if (!response.ok) {
                throw new Error('Ошибка загрузки данных дашборда');
            }

            const data = await response.json();
            setDashboardData(data);
        } catch (err) {
            console.error('Ошибка загрузки дашборда:', err);
            setError(err.message);
        } finally {
            setLoading(false);
        }
    };

    // Форматирование времени (минуты в часы и минуты)
    const formatDuration = (minutes) => {
        if (!minutes) return <>0<span className={styles.unitSmall}>мин</span></>;
        const hours = Math.floor(minutes / 60);
        const mins = Math.round(minutes % 60);

        if (hours > 0) {
            return <>{hours}<span className={styles.unitSmall}>час</span> {mins}<span className={styles.unitSmall}>мин</span></>;
        }
        return <>{mins}<span className={styles.unitSmall}>мин</span></>;
    };

    const getOperatorsChartData = () => {
        if (!dashboardData?.operators || dashboardData.operators.length === 0) {
            return null;
        }

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

    // Настройки для гистограммы с учетом темы
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
                    color: darkMode ? '#ccc' : '#666',
                    font: {
                        size: isMobile ? 8 : 10
                    },
                    maxTicksLimit: 5
                },
                grid: {
                    color: darkMode ? '#444' : '#f0f0f0'
                }
            },
            x: {
                ticks: {
                    color: darkMode ? '#fff' : '#000',
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
        <div className={`${styles.overlay} ${darkMode ? styles.dark : ''}`}>
            <div className={`${styles.popup} ${darkMode ? styles.dark : ''}`} ref={popupRef}>
                {/* Заголовок и кнопка закрытия */}
                <div className={styles.header}>
                    <h2 className={styles.title}>Дашборд - статистика полетов</h2>
                    <button className={styles.closeButton} onClick={onClose}>
                        <Image
                            src={darkMode ? "/svg/close-white.svg" : "/svg/close.svg"}
                            width={20}
                            height={20}
                            alt="Закрыть"
                        />
                    </button>
                </div>

                {loading && (
                    <div className={styles.loading}>
                        <div className={styles.spinner}></div>
                        <p>Загрузка данных...</p>
                    </div>
                )}

                {error && (
                    <div className={styles.error}>
                        <p>Ошибка: {error}</p>
                        <button onClick={fetchDashboardData} className={styles.retryButton}>
                            Попробовать снова
                        </button>
                    </div>
                )}

                {dashboardData && !loading && (
                    <>
                        <div className={styles.popupUp}>
                            {/* Мобильная версия - вертикальная компоновка */}
                            {isMobile ? (
                                <div className={styles.mobileLayout}>
                                    {/* Активные регионы */}
                                    <div className={styles.popupActiveRegions}>
                                        <div className={styles.activeRegions}>
                                            Количество активных регионов
                                        </div>
                                        <div className={styles.verticalSeparator} />
                                        <div className={styles.activeRegionsNum}>
                                            {dashboardData.general_stats?.total_regions || 0}
                                        </div>
                                    </div>

                                    {/* Самый активный регион */}
                                    <div className={styles.popupMostActiveRegionContainer}>
                                        <div className={styles.popupMostActiveRegion}>
                                            Самый активный регион
                                        </div>
                                        <div className={styles.horizontalSeparator} />
                                        <div className={styles.mostActiveRegion}>
                                            {dashboardData.active_region?.region || "Нет данных"}
                                        </div>
                                        <div className={styles.flightsNum}>
                                            всего {dashboardData.active_region?.flight_count || 0} полетов
                                        </div>
                                    </div>

                                    {/* Средняя высота полета */}
                                    <div className={styles.popupAvgFlightHeightContainer}>
                                        <div className={styles.popupAvgFlightHeight}>
                                            Средняя высота полета
                                        </div>
                                        <div className={styles.horizontalSeparator} />
                                        <div className={styles.popupAvgFlightHeightNum}>
                                            {dashboardData.level_stats?.avg_level
                                                ? <>{Math.round(dashboardData.level_stats.avg_level)}<span className={styles.unitSmall}>м</span></>
                                                : "Н/Д"}
                                        </div>
                                        <div className={styles.horizontalSeparator} />
                                        <div className={styles.popupAvgFlightHeightText}>
                                            Над уровнем моря
                                        </div>
                                    </div>
                                </div>
                            ) : (
                                // Десктопная версия
                                <div className={styles.popupUpLeft}>
                                    <div className={styles.popupLeftLeft}>
                                        {/* Активные регионы */}
                                        <div className={styles.popupActiveRegions}>
                                            <div className={styles.activeRegions}>
                                                Количество активных регионов
                                            </div>
                                            <div className={styles.verticalSeparator} />
                                            <div className={styles.activeRegionsNum}>
                                                {dashboardData.general_stats?.total_regions || 0}
                                            </div>
                                        </div>

                                        {/* Самый активный регион */}
                                        <div className={styles.popupMostActiveRegionContainer}>
                                            <div className={styles.popupMostActiveRegion}>
                                                Самый активный регион
                                            </div>
                                            <div className={styles.horizontalSeparator} />
                                            <div className={styles.mostActiveRegion}>
                                                {dashboardData.active_region?.region || "Нет данных"}
                                            </div>
                                            <div className={styles.flightsNum}>
                                                всего {dashboardData.active_region?.flight_count || 0} полетов
                                            </div>
                                        </div>
                                    </div>

                                    {/* Средняя высота полета */}
                                    <div className={styles.popupAvgFlightHeightContainer}>
                                        <div className={styles.popupAvgFlightHeight}>
                                            Средняя высота полета
                                        </div>
                                        <div className={styles.horizontalSeparator} />
                                        <div className={styles.popupAvgFlightHeightNum}>
                                            {dashboardData.level_stats?.avg_level
                                                ? <>{Math.round(dashboardData.level_stats.avg_level)}<span className={styles.unitSmall}>м</span></>
                                                : "Н/Д"}
                                        </div>
                                        <div className={styles.horizontalSeparator} />
                                        <div className={styles.popupAvgFlightHeightText}>
                                            Над уровнем моря
                                        </div>
                                    </div>
                                </div>
                            )}

                            {/* График операторов */}
                            {!isMobile && (
                                <div className={styles.popupOperatorsTop}>
                                    <div className={styles.operatorsTitle}>
                                        Распределение операторов по типам
                                    </div>
                                    <div className={styles.horizontalSeparator} />

                                    {operatorsChartData ? (
                                        <div className={styles.chartContainer}>
                                            <div className={styles.chartWrapper}>
                                                <Bar
                                                    data={operatorsChartData}
                                                    options={chartOptions}
                                                    height={200}
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
                                    ) : (
                                        <div className={styles.noData}>Нет данных об операторах</div>
                                    )}
                                </div>
                            )}
                        </div>

                        {/* График операторов для мобильных */}
                        {isMobile && (
                            <div className={styles.popupOperatorsTop}>
                                <div className={styles.operatorsTitle}>
                                    Распределение операторов по типам
                                </div>
                                <div className={styles.horizontalSeparator} />

                                {operatorsChartData ? (
                                    <div className={styles.chartContainer}>
                                        <div className={styles.chartWrapper}>
                                            <Bar
                                                data={operatorsChartData}
                                                options={chartOptions}
                                                height={150}
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
                                ) : (
                                    <div className={styles.noData}>Нет данных об операторах</div>
                                )}
                            </div>
                        )}

                        <div className={styles.popupDown}>
                            {/* Среднее время в воздухе */}
                            <div className={styles.popupAvgMinutes}>
                                <div className={styles.avgMinutesTitle}>
                                    Среднее количество минут/часов в воздухе
                                </div>
                                <div className={styles.verticalSeparator} />
                                <div className={styles.avgMinutesNum}>
                                    {formatDuration(dashboardData.duration_stats?.avg_duration_minutes)}
                                </div>
                            </div>

                            {/* Типы БПЛА */}
                            <div className={styles.popupUAVType}>
                                <div className={styles.uavTitle}>
                                    Типы БПЛА
                                </div>
                                <div className={styles.horizontalSeparator} />
                                <div className={styles.uavList}>
                                    {dashboardData.aircraft_types?.slice(0, isMobile ? 2 : 3).map((type, index) => {
                                        const totalFlights = dashboardData.general_stats?.total_flights || 0;
                                        const percentage = totalFlights > 0
                                            ? Math.round((type.count / totalFlights) * 100)
                                            : 0;

                                        const getTypeName = (typeCode) => {
                                            const typeMappings = {
                                                'BLA': 'Беспилотный летательный аппарат',
                                                'AER': 'Аэростатический',
                                                '3BLA': 'Беспилотный (3 осевой)',
                                                '2BLA': 'Беспилотный (2 осевой)',
                                                '3BLA\\nRMK': 'Беспилотный (3 осевой) с примечанием',
                                                'BLA\\nRMK': 'Беспилотный с примечанием'
                                            };

                                            const cleanType = typeCode.replace(/\\n/g, ' ');
                                            return typeMappings[cleanType] || typeCode;
                                        };

                                        return (
                                            <div key={index} className={styles.uavItem}>
                                                <span className={styles.uavTypeName}>
                                                    {getTypeName(type.type)}
                                                </span>
                                                <div className={styles.uavBarContainer}>
                                                    <div
                                                        className={styles.uavBar}
                                                        style={{ width: `${Math.max(5, percentage)}%` }}
                                                    ></div>
                                                </div>
                                                <span className={styles.uavPercentage}>{percentage}%</span>
                                            </div>
                                        );
                                    })}
                                    {(!dashboardData.aircraft_types || dashboardData.aircraft_types.length === 0) && (
                                        <div className={styles.noData}>Нет данных о типах БПЛА</div>
                                    )}
                                </div>
                            </div>
                        </div>

                        {/* Общая статистика внизу */}
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
                    </>
                )}
            </div>
        </div>
    );
}

export default DashboardPopup;