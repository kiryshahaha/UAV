"use client";

import { useState, useEffect, useRef } from "react";
import { format, isBefore, isAfter, startOfYear, endOfYear, startOfDay, endOfDay, parseISO } from "date-fns";
import { ru } from "date-fns/locale";
import DatePicker from "react-datepicker";
import "react-datepicker/dist/react-datepicker.css";
import styles from "./PresentationGenerator.module.css";
import { useTimeBounds } from "@/hooks/useTimeBounds";
import { handlePresentationClick } from "@/utils/handlePresentationClick"; 

const PresentationGenerator = ({ isOpen, onClose, onGenerate }) => {
  const [selectedRange, setSelectedRange] = useState({
    from: null,
    to: null
  });
  const [isVisible, setIsVisible] = useState(false);
  const [selectedRegion, setSelectedRegion] = useState("all");
  const [quickSelectionType, setQuickSelectionType] = useState(null);
  const [isRegionDropdownOpen, setIsRegionDropdownOpen] = useState(false);
  const [regions, setRegions] = useState([]);
  const [isLoadingRegions, setIsLoadingRegions] = useState(false);
  const [generating, setGenerating] = useState(false);
  const regionDropdownRef = useRef(null);

  const { timeBounds, loading: timeBoundsLoading } = useTimeBounds();

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
        console.error("Ошибка при загрузке регионов:", error);
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

  useEffect(() => {
    const handleClickOutside = (event) => {
      if (regionDropdownRef.current && !regionDropdownRef.current.contains(event.target)) {
        setIsRegionDropdownOpen(false);
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

      const savedRegion = localStorage.getItem("userRegionPreference");
      if (savedRegion) {
        setSelectedRegion(savedRegion);
      }
    } else {
      setIsVisible(false);
    }
  }, [isOpen]);

  const handleRegionSelect = (regionValue) => {
    setSelectedRegion(regionValue);
    setIsRegionDropdownOpen(false);
  };

  const toggleRegionDropdown = () => {
    setIsRegionDropdownOpen(!isRegionDropdownOpen);
  };

  const getSelectedRegionLabel = () => {
    const region = regions.find(r => r.value === selectedRegion);
    return region ? region.label : "Все регионы";
  };

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
        break;
      default:
        return;
    }
    
    setQuickSelectionType(type);
  };

  const handleStartDateSelect = (date) => {
    setSelectedRange(prev => ({
      ...prev,
      from: date
    }));
  };

  const handleEndDateSelect = (date) => {
    setSelectedRange(prev => ({
      ...prev,
      to: date
    }));
  };

  const handleGeneratePresentation = async () => {
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
        date_from: dateFrom, 
        date_to: dateTo,
        region: regionToUse
      };

      console.log(" Параметры для генерации:", params);

      const result = await handlePresentationClick(params);

      if (result.success) {
        if (onGenerate) {
          await onGenerate(params);
        }
        
        alert(`Презентация успешно создана: ${result.fileName}`);
      } else {
        throw new Error(result.message);
      }

      onClose();
    } catch (error) {
      console.error('Ошибка генерации презентации:', error);
      alert(`Произошла ошибка при генерации презентации: ${error.message}`);
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

  const getSelectedRegionName = () => {
    const region = regions.find(r => r.value === selectedRegion);
    return region ? region.label : "Все регионы";
  };

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
          <h2 className={styles.title}>Выберите период и регион для презентации</h2>
          <p className={styles.subtitle}>Выберите готовый диапазон или настройте вручную</p>
          
          {dateBounds.hasRealData && (
            <div className={styles.dataInfo}>
              Данные доступны с {format(dateBounds.minDate, "d MMM yyyy", { locale: ru })} по {format(dateBounds.maxDate, "d MMM yyyy", { locale: ru })}
            </div>
          )}
        </div>

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

        <div className={styles.selectedPreview}>
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
            onClick={handleGeneratePresentation}
            disabled={(!selectedRange.from && !selectedRange.to) || generating}
            type="button"
          >
            {generating ? (
              <>
                <div className={styles.spinner}></div>
                Генерация...
              </>
            ) : (
              "Сгенерировать презентацию"
            )}
          </button>
        </div>
      </div>
    </div>
  );
};

export default PresentationGenerator;