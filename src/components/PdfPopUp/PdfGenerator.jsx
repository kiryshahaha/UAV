"use client";

import { useState, useEffect, useRef } from "react";
import { format, isBefore, isAfter, startOfYear, endOfYear, startOfDay, endOfDay, parseISO } from "date-fns";
import { ru } from "date-fns/locale";
import DatePicker from "react-datepicker";
import "react-datepicker/dist/react-datepicker.css";
import styles from "./PDFGenerator.module.css"
import { useTimeBounds } from "@/hooks/useTimeBounds";
import { handlePdfExport, downloadPdf, getAvailableReportTypes } from "@/utils/handlePdfExport";

const PdfGenerator = ({ isOpen, onClose, onGenerate }) => {
  const [selectedRange, setSelectedRange] = useState({
    from: null,
    to: null
  });
  const [isVisible, setIsVisible] = useState(false);
  const [selectedRegion, setSelectedRegion] = useState("all");
  const [reportType, setReportType] = useState("general");
  const [quickSelectionType, setQuickSelectionType] = useState(null);
  const [isRegionDropdownOpen, setIsRegionDropdownOpen] = useState(false);
  const [isReportTypeDropdownOpen, setIsReportTypeDropdownOpen] = useState(false);
  const [regions, setRegions] = useState([]);
  const [isLoadingRegions, setIsLoadingRegions] = useState(false);
  const [generating, setGenerating] = useState(false);
  const regionDropdownRef = useRef(null);
  const reportTypeDropdownRef = useRef(null);

  // Используем хук для получения временных границ
  const { timeBounds, loading: timeBoundsLoading } = useTimeBounds();

  // Типы отчетов
  const reportTypes = getAvailableReportTypes();

  // Получаем динамические даты на основе реальных данных
  const getDynamicDates = () => {
    const today = new Date();
    
    if (!timeBounds) {
      return {
        today,
        currentYear: today.getFullYear(),
        earliestYear: 2020,
        allTimeStart: new Date(2020, 0, 1),
        currentYearStart: startOfYear(today),
        currentYearEnd: endOfDay(today)
      };
    }

    const minDate = parseISO(timeBounds.min_date);
    const maxDate = parseISO(timeBounds.max_date);
    const earliestYear = minDate.getFullYear();
    const currentYear = maxDate.getFullYear();

    return {
      today,
      currentYear,
      earliestYear,
      allTimeStart: minDate,
      allTimeEnd: maxDate,
      currentYearStart: startOfYear(maxDate),
      currentYearEnd: endOfDay(maxDate),
      hasRealData: timeBounds.has_date_data
    };
  };

  // Загрузка регионов из API
  useEffect(() => {
    const loadRegions = async () => {
      setIsLoadingRegions(true);
      try {
        const response = await fetch('/api/cities');
        if (response.ok) {
          const data = await response.json();
          const regionsData = data.regions || data.cities || [];
          
          const formattedRegions = [
            { value: "all", label: "Все регионы" },
            ...regionsData.map(region => ({
              value: region.id || region.name_ru || region.name_en || String(region),
              label: region.name_ru || region.name_en || String(region)
            }))
          ];
          
          setRegions(formattedRegions);
        } else {
          throw new Error(`HTTP ${response.status}`);
        }
      } catch (error) {
        console.error("❌ Ошибка при загрузке регионов:", error);
        setRegions([
          { value: "all", label: "Все регионы" },
          { value: "moscow", label: "Москва" },
          { value: "spb", label: "Санкт-Петербург" },
          { value: "novosibirsk", label: "Новосибирская область" },
          { value: "ekaterinburg", label: "Свердловская область" },
        ]);
      } finally {
        setIsLoadingRegions(false);
      }
    };

    if (isOpen) {
      loadRegions();
    }
  }, [isOpen]);

  // Закрытие dropdown при клике вне компонента
  useEffect(() => {
    const handleClickOutside = (event) => {
      if (regionDropdownRef.current && !regionDropdownRef.current.contains(event.target)) {
        setIsRegionDropdownOpen(false);
      }
      if (reportTypeDropdownRef.current && !reportTypeDropdownRef.current.contains(event.target)) {
        setIsReportTypeDropdownOpen(false);
      }
    };

    document.addEventListener('click', handleClickOutside);
    return () => document.removeEventListener('click', handleClickOutside);
  }, []);

  useEffect(() => {
    if (isOpen) {
      setIsVisible(true);
      setQuickSelectionType(null);
      setIsRegionDropdownOpen(false);
      setIsReportTypeDropdownOpen(false);
      setGenerating(false);

      // Загрузка сохраненного диапазона дат
      const savedRange = localStorage.getItem("userDateRangePreference");
      if (savedRange) {
        try {
          const range = JSON.parse(savedRange);
          if (range.date_from || range.date_to) {
            setSelectedRange({
              from: range.date_from ? new Date(range.date_from) : null,
              to: range.date_to ? new Date(range.date_to) : null
            });
          }
        } catch (error) {
          console.error("Ошибка при загрузке сохраненного диапазона:", error);
        }
      }

      // Загрузка сохраненного региона
      const savedRegion = localStorage.getItem("userRegionPreference");
      if (savedRegion) {
        setSelectedRegion(savedRegion);
      }

      // Загрузка сохраненного типа отчета
      const savedReportType = localStorage.getItem("userReportTypePreference");
      if (savedReportType) {
        setReportType(savedReportType);
      }
    } else {
      setIsVisible(false);
    }
  }, [isOpen]);

  const handleRegionSelect = (regionValue) => {
    setSelectedRegion(regionValue);
    setIsRegionDropdownOpen(false);
    localStorage.setItem("userRegionPreference", regionValue);
  };

  const handleReportTypeSelect = (typeValue) => {
    setReportType(typeValue);
    setIsReportTypeDropdownOpen(false);
    localStorage.setItem("userReportTypePreference", typeValue);
  };

  const toggleRegionDropdown = () => {
    setIsRegionDropdownOpen(!isRegionDropdownOpen);
    setIsReportTypeDropdownOpen(false);
  };

  const toggleReportTypeDropdown = () => {
    setIsReportTypeDropdownOpen(!isReportTypeDropdownOpen);
    setIsRegionDropdownOpen(false);
  };

  const getSelectedRegionLabel = () => {
    const region = regions.find(r => r.value === selectedRegion);
    return region ? region.label : "Все регионы";
  };

  const getSelectedReportTypeLabel = () => {
    const report = reportTypes.find(r => r.value === reportType);
    return report ? report.label : "Общий отчет";
    
  };

  const getSelectedReportTypeDescription = () => {
    const report = reportTypes.find(r => r.value === reportType);
    return report ? report.description : "Общая статистика по всем показателям";
  };

  // Выбор типа диапазона
  const handleRangeTypeSelect = (type) => {
    const dates = getDynamicDates();
    
    switch (type) {
      case "currentYear":
        setSelectedRange({
          from: dates.currentYearStart,
          to: dates.currentYearEnd
        });
        break;
      case "allTime":
        setSelectedRange({
          from: dates.allTimeStart,
          to: dates.allTimeEnd
        });
        break;
      case "custom":
        // Для кастомного выбора показываем DatePicker
        break;
      default:
        return;
    }
    
    setQuickSelectionType(type);
  };

  // Выбор начальной даты
  const handleStartDateSelect = (date) => {
    setSelectedRange(prev => ({
      ...prev,
      from: date
    }));
  };

  // Выбор конечной даты
  const handleEndDateSelect = (date) => {
    setSelectedRange(prev => ({
      ...prev,
      to: date
    }));
  };

  const handleGeneratePdf = async () => {
    const dates = getDynamicDates();
    
    if (selectedRange.from && isAfter(selectedRange.from, dates.today)) {
      alert("Нельзя выбрать будущую дату");
      return;
    }
    if (selectedRange.to && isAfter(selectedRange.to, dates.today)) {
      alert("Нельзя выбрать будущую дату");
      return;
    }

    const dateFrom = selectedRange.from ? format(selectedRange.from, "yyyy-MM-dd") : null;
    const dateTo = selectedRange.to ? format(selectedRange.to, "yyyy-MM-dd") : null;
    const regionToUse = selectedRegion === "all" ? null : selectedRegion;

    setGenerating(true);
    
    try {
      const params = {
        report_type: reportType,
        date_from: dateFrom,
        date_to: dateTo,
        regions: regionToUse ? [regionToUse] : []
      };

      console.log("📊 Параметры для генерации PDF:", params);

      // Используем утилиту для генерации PDF
      const result = await handlePdfExport(params);

      if (result.success) {
        // Скачиваем PDF
        const downloadSuccess = downloadPdf(result.pdfUrl, result.fileName);
        
        if (downloadSuccess) {
          // Если есть кастомная функция onGenerate, вызываем ее
          if (onGenerate) {
            await onGenerate(params);
          }
          
          // Показываем сообщение об успехе
          alert(`PDF отчет успешно создан: ${result.fileName} (${(result.fileSize / 1024 / 1024).toFixed(2)} MB)`);
        } else {
          throw new Error('Не удалось скачать файл');
        }
      } else {
        throw new Error(result.message);
      }

      onClose();
    } catch (error) {
      console.error('Ошибка генерации PDF отчета:', error);
      alert(`Произошла ошибка при генерации PDF отчета: ${error.message}`);
    } finally {
      setGenerating(false);
    }
  };

  const handleOverlayClick = (e) => {
    if (e.target === e.currentTarget) {
      onClose();
    }
  };

  const formatDateRange = () => {
    if (!selectedRange.from && !selectedRange.to) {
      return "Дата не выбрана";
    }
    
    if (selectedRange.from && selectedRange.to) {
      return `${format(selectedRange.from, "d MMM yyyy", { locale: ru })} - ${format(selectedRange.to, "d MMM yyyy", { locale: ru })}`;
    }
    
    if (selectedRange.from) {
      return `С ${format(selectedRange.from, "d MMM yyyy", { locale: ru })}`;
    }
    
    if (selectedRange.to) {
      return `По ${format(selectedRange.to, "d MMM yyyy", { locale: ru })}`;
    }
  };

  // Получение названия выбранного региона
  const getSelectedRegionName = () => {
    const region = regions.find(r => r.value === selectedRegion);
    return region ? region.label : "Все регионы";
  };

  // Получение минимальной и максимальной дат для DatePicker
  const getDatePickerBounds = () => {
    const dates = getDynamicDates();
    const yearsRange = Array.from(
      { length: dates.currentYear - dates.earliestYear + 1 }, 
      (_, i) => dates.earliestYear + i
    );
    
    return {
      minDate: dates.allTimeStart,
      maxDate: dates.allTimeEnd,
      yearsRange,
      hasRealData: dates.hasRealData
    };
  };

  if (!isVisible) return null;

  const dateBounds = getDatePickerBounds();

  return (
    <div className={styles.overlay} onClick={handleOverlayClick}>
      <div className={styles.popup}>
        <div className={styles.header}>
          <h2 className={styles.title}>Генерация PDF отчета</h2>
          <p className={styles.subtitle}>Выберите параметры для формирования отчета в PDF формате</p>
          
          {/* Информация о доступных данных */}
          {dateBounds.hasRealData && (
            <div className={styles.dataInfo}>
              Данные доступны с {format(dateBounds.minDate, "d MMM yyyy", { locale: ru })} по {format(dateBounds.maxDate, "d MMM yyyy", { locale: ru })}
            </div>
          )}
        </div>

        {/* Выбор типа отчета */}
        <div className={styles.reportTypeSelection}>
          <label className={styles.reportTypeLabel}>
            Тип отчета
          </label>
          <div className={styles.customSelectContainer} ref={reportTypeDropdownRef}>
            <button
              className={`${styles.customSelectTrigger} ${isReportTypeDropdownOpen ? styles.customSelectTriggerOpen : ''}`}
              onClick={toggleReportTypeDropdown}
              type="button"
            >
            <div style={{ display: 'flex', flexDirection: 'column', gap: '8px', flex: 1 }}>
                <span style={{ fontSize: '14px', fontWeight: '600', color: '#1a1a1a' }}>
                    {getSelectedReportTypeLabel()}
                </span>
                <span style={{ fontSize: '12px', color: '#666', lineHeight: '1.3' }}>
                    {getSelectedReportTypeDescription()}
                </span>
            </div>
              <span className={`${styles.customSelectArrow} ${isReportTypeDropdownOpen ? styles.customSelectArrowOpen : ''}`}>
                ▼
              </span>
            </button>
            
            <div className={`${styles.reportTypeDropdown} ${isReportTypeDropdownOpen ? styles.reportTypeDropdownOpen : ''}`}>
              {reportTypes.map(report => (
                <div
                  key={report.value}
                  className={`${styles.reportTypeDropdownItem} ${reportType === report.value ? styles.reportTypeDropdownItemSelected : ''}`}
                  onClick={() => handleReportTypeSelect(report.value)}
                >
                  <div className={styles.reportTypeItemContent}>
                    <span className={styles.reportTypeItemTitle}>{report.label}</span>
                    <span className={styles.reportTypeItemDescription}>{report.description}</span>
                  </div>
                </div>
              ))}
            </div>
          </div>
        </div>

        {/* Быстрый выбор периода */}
        <div className={styles.quickSelection}>
          <button
            className={`${styles.rangeOption} ${quickSelectionType === "allTime" ? styles.rangeOptionSelected : ""}`}
            onClick={() => handleRangeTypeSelect("allTime")}
            type="button"
          >
            <span className={styles.rangeTitle}>Всё время</span>
            <span className={styles.rangeDescription}>
              {dateBounds.hasRealData 
                ? `С ${format(dateBounds.minDate, "d MMM yyyy", { locale: ru })} по ${format(dateBounds.maxDate, "d MMM yyyy", { locale: ru })}`
                : "Все доступные данные"
              }
            </span>
          </button>
          
          <button
            className={`${styles.rangeOption} ${quickSelectionType === "currentYear" ? styles.rangeOptionSelected : ""}`}
            onClick={() => handleRangeTypeSelect("currentYear")}
            type="button"
          >
            <span className={styles.rangeTitle}>Текущий год</span>
            <span className={styles.rangeDescription}>
              {dateBounds.hasRealData 
                ? `С 1 января ${dateBounds.yearsRange[dateBounds.yearsRange.length - 1]} по ${format(dateBounds.maxDate, "d MMM yyyy", { locale: ru })}`
                : `Текущий год ${new Date().getFullYear()}`
              }
            </span>
          </button>
        </div>

        {/* Кастомный выбор периода */}
        <div className={styles.customSelection}>
          <div className={styles.datePickers}>
            <div className={styles.datePickerGroup}>
              <label className={styles.datePickerLabel}>Начальная дата</label>
              <DatePicker
                selected={selectedRange.from}
                onChange={handleStartDateSelect}
                selectsStart
                startDate={selectedRange.from}
                endDate={selectedRange.to}
                maxDate={dateBounds.maxDate}
                minDate={dateBounds.minDate}
                locale={ru}
                dateFormat="dd MMM yyyy"
                placeholderText="Выберите начальную дату"
                className={styles.datePickerInput}
                wrapperClassName={styles.datePickerContainer}
              />
            </div>
            
            <div className={styles.datePickerGroup}>
              <label className={styles.datePickerLabel}>Конечная дата</label>
              <DatePicker
                selected={selectedRange.to}
                onChange={handleEndDateSelect}
                selectsEnd
                startDate={selectedRange.from}
                endDate={selectedRange.to}
                minDate={selectedRange.from || dateBounds.minDate}
                maxDate={dateBounds.maxDate}
                locale={ru}
                dateFormat="dd MMM yyyy"
                placeholderText="Выберите конечную дату"
                className={styles.datePickerInput}
                wrapperClassName={styles.datePickerContainer}
              />
            </div>
          </div>
        </div>

        {/* Выбор региона */}
        <div className={styles.regionSelection}>
          <label className={styles.regionLabel}>
            Регион для анализа
          </label>
          <div className={styles.customSelectContainer} ref={regionDropdownRef}>
            <button
              className={`${styles.customSelectTrigger} ${isRegionDropdownOpen ? styles.customSelectTriggerOpen : ''}`}
              onClick={toggleRegionDropdown}
              type="button"
              disabled={isLoadingRegions}
            >
              <span className={styles.customSelectValue}>
                {isLoadingRegions ? "Загрузка регионов..." : getSelectedRegionLabel()}
              </span>
              <span className={`${styles.customSelectArrow} ${isRegionDropdownOpen ? styles.customSelectArrowOpen : ''}`}>
                ▼
              </span>
            </button>
            
            {!isLoadingRegions && (
              <div className={`${styles.regionDropdown} ${isRegionDropdownOpen ? styles.regionDropdownOpen : ''}`}>
                {regions.map(region => (
                  <div
                    key={region.value}
                    className={`${styles.regionDropdownItem} ${selectedRegion === region.value ? styles.regionDropdownItemSelected : ''}`}
                    onClick={() => handleRegionSelect(region.value)}
                  >
                    {region.label}
                  </div>
                ))}
              </div>
            )}
          </div>
        </div>

        {/* Предпросмотр выбора */}
        <div className={styles.selectedPreview}>
          <div className={styles.previewRow}>
            <span className={styles.previewLabel}>Тип отчета:</span>
            <span className={styles.previewValue}>{getSelectedReportTypeLabel()}</span>
          </div>
          <div className={styles.previewRow}>
            <span className={styles.previewLabel}>Период:</span>
            <span className={styles.previewValue}>{formatDateRange()}</span>
          </div>
          <div className={styles.previewRow}>
            <span className={styles.previewLabel}>Регион:</span>
            <span className={styles.previewValue}>{getSelectedRegionName()}</span>
          </div>
        </div>

        {/* Действия */}
        <div className={styles.actions}>
          <button
            className={styles.cancelButton}
            onClick={onClose}
            type="button"
            disabled={generating}
          >
            Отмена
          </button>
          
          <button
            className={styles.generateButton}
            onClick={handleGeneratePdf}
            disabled={(!selectedRange.from && !selectedRange.to) || generating}
            type="button"
          >
            {generating ? (
              <>
                <div className={styles.spinner}></div>
                Генерация PDF...
              </>
            ) : (
              "Сгенерировать PDF отчет"
            )}
          </button>
        </div>
      </div>
    </div>
  );
};

export default PdfGenerator;