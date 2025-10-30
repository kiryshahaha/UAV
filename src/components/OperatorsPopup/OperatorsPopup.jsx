"use client";

import React, { useState, useEffect, useRef, useCallback, useMemo } from "react";
import Image from "next/image";
import styles from "./OperatorsPopup.module.css";
import { useTable } from '@/contexts/TableContext';

const OPERATOR_TYPES = [
  { id: "all", name: "Все операторы", endpoint: "all" },
  { id: "ooo", name: "ООО", endpoint: "ooo" },
  { id: "ip", name: "ИП", endpoint: "ip" },
  { id: "individual", name: "Физлица", endpoint: "individual" },
  { id: "other", name: "Другие", endpoint: "other" }
];

// Функции валидации и очистки данных
const validateAndCleanOperators = (operators) => {
  if (!operators || !Array.isArray(operators)) return [];

  const cleanedOperators = operators.map(operator => ({
    ...operator,
    name: cleanOperatorName(operator.name)
  }));

  // Объединяем операторов с одинаковыми именами после очистки
  return mergeDuplicateOperators(cleanedOperators);
};

const cleanOperatorName = (name) => {
  if (!name || typeof name !== 'string') return '';

  let cleaned = name.trim();

  // Удаляем телефоны (последовательности цифр длиной 6+)
  cleaned = cleaned.replace(/\b\d{6,}\b/g, '');

  // Заменяем цифру 4 на "Ч" если рядом есть 4 буквы
  cleaned = cleaned.replace(/([а-яА-Яa-zA-Z]{4})4/g, '$1Ч');
  cleaned = cleaned.replace(/4([а-яА-Яa-zA-Z]{4})/g, 'Ч$1');

  // Удаляем ZZZZZ
  cleaned = cleaned.replace(/ZZZZZ/g, '');
  cleaned = cleaned.replace(/\s+ZZZZZ/g, '');
  cleaned = cleaned.replace(/ZZZZZ\s+/g, '');

  // Удаляем все после RMK/ (включая RMK/)
  cleaned = cleaned.replace(/RMK\/.*$/i, '');
  cleaned = cleaned.replace(/RMK.*$/i, '');

  // Удаляем лишние пробелы и специальные символы
  cleaned = cleaned.replace(/\s+/g, ' ').trim();
  cleaned = cleaned.replace(/[^\wа-яА-Я\s\-\.]/g, '');

  // Удаляем одиночные буквы и цифры
  cleaned = cleaned.replace(/\b[а-яА-Яa-zA-Z0-9]\b/g, '');

  // Удаляем лишние пробелы снова
  cleaned = cleaned.replace(/\s+/g, ' ').trim();

  return cleaned || 'Неизвестный оператор';
};

const mergeDuplicateOperators = (operators) => {
  const mergedMap = new Map();

  operators.forEach(operator => {
    const key = operator.name.toLowerCase().trim();
    
    if (!key || key === 'неизвестный оператор') return;

    if (mergedMap.has(key)) {
      const existing = mergedMap.get(key);
      mergedMap.set(key, {
        ...existing,
        flight_count: existing.flight_count + (operator.flight_count || 0),
        unique_aircrafts: existing.unique_aircrafts + (operator.unique_aircrafts || 0),
        regions_covered: Math.max(existing.regions_covered || 0, operator.regions_covered || 0),
        // Используем новые поля из бэкенда
        avg_level_m: calculateWeightedAverage(
          existing.avg_level_m, existing.flight_count,
          operator.avg_level_m, operator.flight_count
        ),
        avg_radius_m: calculateWeightedAverage(
          existing.avg_radius_m, existing.flight_count,
          operator.avg_radius_m, operator.flight_count
        )
      });
    } else {
      mergedMap.set(key, { 
        ...operator,
        // Сохраняем новые поля
        avg_level_m: operator.avg_level_m || operator.avg_level,
        avg_radius_m: operator.avg_radius_m || operator.avg_radius
      });
    }
  });

  return Array.from(mergedMap.values())
    .filter(operator => operator.name && operator.name !== 'Неизвестный оператор')
    .sort((a, b) => (b.flight_count || 0) - (a.flight_count || 0));
};

const calculateWeightedAverage = (avg1, count1, avg2, count2) => {
  if (!avg1 && !avg2) return 0;
  if (!avg1) return avg2;
  if (!avg2) return avg1;
  
  const totalCount = (count1 || 0) + (count2 || 0);
  if (totalCount === 0) return 0;
  
  return ((avg1 * (count1 || 0)) + (avg2 * (count2 || 0))) / totalCount;
};

const OperatorsPopup = ({ isOpen, onClose, dateRange, selectedRegion }) => {
  const [operatorsData, setOperatorsData] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [activeTab, setActiveTab] = useState("all");
  const popupRef = useRef(null);
  const { tableVersion, currentTable } = useTable();

  // Мемоизированная функция загрузки данных
  const fetchOperatorsData = useCallback(async () => {
  setLoading(true);
  setError(null);

  try {
    const activeTabConfig = OPERATOR_TYPES.find(tab => tab.id === activeTab);
    if (!activeTabConfig) return;

    const params = new URLSearchParams();
    if (dateRange?.date_from) params.append('date_from', dateRange.date_from);
    if (dateRange?.date_to) params.append('date_to', dateRange.date_to);
    if (selectedRegion) params.append('region', selectedRegion);
    
    // Добавляем информацию о таблице
    params.append('_v', tableVersion);
    params.append('_t', Date.now());

    const sessionId = localStorage.getItem('session_id');
    const headers = {};
    
    if (sessionId) {
      headers['X-Session-ID'] = sessionId;
    }

    const response = await fetch(`/api/operators/${activeTabConfig.endpoint}?${params.toString()}`, {
      headers: headers
    });

    if (!response.ok) {
      throw new Error('Ошибка загрузки данных операторов');
    }

    const data = await response.json();
    
    console.log('📊 Получены данные операторов:', data);
    
    // Применяем валидацию и очистку данных
    if (data.operators && Array.isArray(data.operators)) {
      const validatedOperators = validateAndCleanOperators(data.operators);
      setOperatorsData({
        ...data,
        operators: validatedOperators,
        total_operators: validatedOperators.length
      });
    } else {
      setOperatorsData(data);
    }
  } catch (err) {
    console.error('Ошибка загрузки операторов:', err);
    setError(err.message);
  } finally {
    setLoading(false);
  }
}, [activeTab, dateRange, selectedRegion, tableVersion]);

useEffect(() => {
  if (isOpen && currentTable) {
    fetchOperatorsData();
  }
}, [isOpen, currentTable, activeTab, dateRange, selectedRegion, tableVersion, fetchOperatorsData]);

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

  // Мемоизированное форматирование чисел
  const formatNumber = useCallback((num) => {
    if (!num && num !== 0) return '0';
    return num.toString().replace(/\B(?=(\d{3})+(?!\d))/g, " ");
  }, []);

  // Мемоизированный заголовок для текущего таба
  const tabTitle = useMemo(() => {
    const tab = OPERATOR_TYPES.find(t => t.id === activeTab);
    return tab ? `Статистика: ${tab.name}` : "Статистика операторов";
  }, [activeTab]);

  // Мемоизированная статистика
  const totalStats = useMemo(() => {
    if (!operatorsData?.operators) return null;
    
    const totalOperators = operatorsData.total_operators || operatorsData.operators.length;
    const totalFlights = operatorsData.operators.reduce((sum, op) => sum + (op.flight_count || 0), 0);
    const totalAircrafts = operatorsData.operators.reduce((sum, op) => sum + (op.unique_aircrafts || 0), 0);
    const totalRegions = operatorsData.operators.reduce((sum, op) => sum + (op.regions_covered || 0), 0);
    
    // Взвешенное среднее по количеству полетов (используем новые поля)
    const totalLevel = operatorsData.operators.reduce((sum, op) => 
      sum + ((op.avg_level_m || 0) * (op.flight_count || 0)), 0);
    const avgLevel = totalFlights > 0 ? totalLevel / totalFlights : 0;

    // Взвешенное среднее для радиуса
    const totalRadius = operatorsData.operators.reduce((sum, op) => 
      sum + ((op.avg_radius_m || 0) * (op.flight_count || 0)), 0);
    const avgRadius = totalFlights > 0 ? totalRadius / totalFlights : 0;

    return {
      totalOperators,
      totalFlights,
      totalAircrafts,
      totalRegions,
      avgLevel: Math.round(avgLevel),
      avgRadius: Math.round(avgRadius)
    };
  }, [operatorsData]);

  if (!isOpen) return null;

  return (
    <div className={styles.overlay}>
      <div className={styles.popup} ref={popupRef}>
        {/* Заголовок и кнопка закрытия */}
        <div className={styles.header}>
          <h2 className={styles.title}>{tabTitle}</h2>
          <button className={styles.closeButton} onClick={onClose}>
            <Image
              src="/svg/close.svg"
              width={20}
              height={20}
              alt="Закрыть"
            />
          </button>
        </div>

        {/* Табы переключения типов операторов */}
        <div className={styles.tabsContainer}>
          <div className={styles.tabs}>
            {OPERATOR_TYPES.map((tab) => (
              <button
                key={tab.id}
                className={`${styles.tab} ${activeTab === tab.id ? styles.active : ''}`}
                onClick={() => setActiveTab(tab.id)}
              >
                <span className={styles.tabText}>{tab.name}</span>
              </button>
            ))}
          </div>
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
            <button onClick={fetchOperatorsData} className={styles.retryButton}>
              Попробовать снова
            </button>
          </div>
        )}

        {operatorsData && !loading && (
          <div className={styles.content}>
            {/* Общая статистика таба */}
            <div className={styles.tabStats}>
              <div className={styles.statItem}>
                <div className={styles.statValue}>
                  {formatNumber(totalStats?.totalOperators || 0)}
                </div>
                <div className={styles.statLabel}>Операторов</div>
              </div>
              
              <div className={styles.statItem}>
                <div className={styles.statValue}>
                  {formatNumber(totalStats?.totalFlights || 0)}
                </div>
                <div className={styles.statLabel}>Всего полётов</div>
              </div>

              <div className={styles.statItem}>
                <div className={styles.statValue}>
                  {formatNumber(totalStats?.totalAircrafts || 0)}
                </div>
                <div className={styles.statLabel}>Уникальных БПЛА</div>
              </div>

              {operatorsData.operator_type && operatorsData.operator_type !== "all" && (
                <div className={styles.statItem}>
                  <div className={styles.statValue}>
                    {operatorsData.operator_type}
                  </div>
                  <div className={styles.statLabel}>Тип операторов</div>
                </div>
              )}
            </div>

            {/* Список операторов */}
            <div className={styles.operatorsList}>
              <div className={styles.listHeader}>
                <span className={styles.rankHeader}>#</span>
                <span className={styles.nameHeader}>Оператор</span>
                <span className={styles.flightsHeader}>Полёты</span>
                <span className={styles.aircraftsHeader}>БПЛА</span>
                <span className={styles.regionsHeader}>Регионы</span>
                <span className={styles.avgLevelHeader}>Высота (м)</span>
                <span className={styles.avgRadiusHeader}>Радиус (м)</span>
              </div>

              <div className={styles.operatorsContainer}>
                {operatorsData.operators && operatorsData.operators.length > 0 ? (
                  operatorsData.operators.map((operator, index) => (
                    <div key={`${operator.name}-${index}`} className={styles.operatorRow}>
                      <div className={styles.rank}>{index + 1}</div>
                      <div className={styles.name}>
                        <span className={styles.nameText} title={operator.name}>
                          {operator.name}
                        </span>
                      </div>
                      <div className={styles.flights}>
                        {formatNumber(operator.flight_count)}
                      </div>
                      <div className={styles.aircrafts}>
                        {formatNumber(operator.unique_aircrafts)}
                      </div>
                      <div className={styles.regions}>
                        {formatNumber(operator.regions_covered)}
                      </div>
                      <div className={styles.avgLevel}>
                        {operator.avg_level_m ? `${Math.round(operator.avg_level_m)}м` : '-'}
                      </div>
                      <div className={styles.avgRadius}>
                        {operator.avg_radius_m ? `${formatNumber(Math.round(operator.avg_radius_m))}м` : '-'}
                      </div>
                    </div>
                  ))
                ) : (
                  <div className={styles.noData}>
                    Нет данных об операторах для выбранного типа
                  </div>
                )}
              </div>
            </div>

            {/* Дополнительная статистика */}
            {operatorsData.operators && operatorsData.operators.length > 0 && (
              <div className={styles.additionalStats}>
                <div className={styles.additionalStat}>
                  <span className={styles.additionalLabel}>Среднее кол-во БПЛА на оператора:</span>
                  <span className={styles.additionalValue}>
                    {Math.round(totalStats?.totalAircrafts / operatorsData.operators.length) || 0}
                  </span>
                </div>
                <div className={styles.additionalStat}>
                  <span className={styles.additionalLabel}>Среднее кол-во регионов:</span>
                  <span className={styles.additionalValue}>
                    {Math.round(totalStats?.totalRegions / operatorsData.operators.length) || 0}
                  </span>
                </div>
                <div className={styles.additionalStat}>
                  <span className={styles.additionalLabel}>Средняя высота полёта:</span>
                  <span className={styles.additionalValue}>
                    {totalStats?.avgLevel || 0} м
                  </span>
                </div>
                <div className={styles.additionalStat}>
                  <span className={styles.additionalLabel}>Средний радиус полёта:</span>
                  <span className={styles.additionalValue}>
                    {formatNumber(totalStats?.avgRadius || 0)} м
                  </span>
                </div>
              </div>
            )}

            {/* Информация о фильтрах */}
            <div className={styles.filtersInfo}>
              {operatorsData.filters && (
                <>
                  {operatorsData.filters.date_from && operatorsData.filters.date_to && (
                    <div className={styles.filterItem}>
                      Период: {operatorsData.filters.date_from} - {operatorsData.filters.date_to}
                    </div>
                  )}
                  {selectedRegion && (
                    <div className={styles.filterItem}>
                      Регион: {selectedRegion}
                    </div>
                  )}
                </>
              )}
            </div>

            {/* Информация о единицах измерения
            <div className={styles.unitsInfo}>
              <div className={styles.unitItem}>
                Единицы измерения: высота - метры, радиус - метры
              </div>
              {operatorsData.statistics_method && (
                <div className={styles.unitItem}>
                  Метод расчёта: {operatorsData.statistics_method}
                </div>
              )}
            </div> */}
          </div>
        )}
      </div>
    </div>
  );
};

export default OperatorsPopup;