"use client";

import { useState, useEffect, useRef } from "react";
import { format, isBefore, isAfter, startOfYear, endOfYear, startOfDay, endOfDay, parseISO } from "date-fns";
import { ru } from "date-fns/locale";
import DatePicker from "react-datepicker";
import "react-datepicker/dist/react-datepicker.css";
import styles from "./DatePickerPopup.module.css";
import { useTimeBounds } from "@/hooks/useTimeBounds";

const DatePickerPopup = ({ isOpen, onClose, onDateSelect }) => {
  const [selectedRange, setSelectedRange] = useState({
    from: null,
    to: null
  });
  const [isVisible, setIsVisible] = useState(false);
  const [currentStep, setCurrentStep] = useState("range");
  const [tempStartDate, setTempStartDate] = useState(null);
  const [tempEndDate, setTempEndDate] = useState(null);
  const [isFirstTime, setIsFirstTime] = useState(true);
  const [selectedRegion, setSelectedRegion] = useState("all");
  const [quickSelectionType, setQuickSelectionType] = useState(null);
  const [isRegionDropdownOpen, setIsRegionDropdownOpen] = useState(false);
  const [regions, setRegions] = useState([]);
  const [isLoadingRegions, setIsLoadingRegions] = useState(false);
  const regionDropdownRef = useRef(null);

  // Используем хук для получения временных границ
  const { timeBounds, loading: timeBoundsLoading } = useTimeBounds();

  // Получаем динамические даты на основе реальных данных
  const getDynamicDates = () => {
    const today = new Date();
    
    if (!timeBounds) {
      // Fallback если границы еще не загружены
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
          console.log("✅ Регионы загружены:", formattedRegions.length);
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
    };

    document.addEventListener('click', handleClickOutside);
    return () => document.removeEventListener('click', handleClickOutside);
  }, []);

  useEffect(() => {
    if (isOpen) {
      setIsVisible(true);
      setCurrentStep("range");
      setQuickSelectionType(null);
      setIsRegionDropdownOpen(false);
      
      const hasPreference = localStorage.getItem("hasDatePreference");
      setIsFirstTime(!hasPreference);

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
            setTempStartDate(range.date_from ? new Date(range.date_from) : null);
            setTempEndDate(range.date_to ? new Date(range.date_to) : null);
          } else if (range.from || range.to) {
            setSelectedRange({
              from: range.from ? new Date(range.from) : null,
              to: range.to ? new Date(range.to) : null
            });
            setTempStartDate(range.from ? new Date(range.from) : null);
            setTempEndDate(range.to ? new Date(range.to) : null);
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

  // Шаг 1: Выбор типа диапазона
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
        setCurrentStep("start");
        return;
      default:
        return;
    }
    
    setQuickSelectionType(type);
  };

  const handleQuickSelectionConfirm = () => {
    if (quickSelectionType) {
      const dateFrom = selectedRange.from ? format(selectedRange.from, "yyyy-MM-dd") : null;
      const dateTo = selectedRange.to ? format(selectedRange.to, "yyyy-MM-dd") : null;
      const regionToSave = selectedRegion === "all" ? null : selectedRegion;
      
      localStorage.setItem("userDateRangePreference", JSON.stringify({
        date_from: dateFrom,
        date_to: dateTo
      }));
      localStorage.setItem("hasDatePreference", "true");
      localStorage.setItem("userRegionPreference", regionToSave || "");
      
      setCurrentStep("confirm");
    }
  };

  // Шаг 2: Выбор начальной даты
  const handleStartDateSelect = (date) => {
    setTempStartDate(date);
  };

  const handleConfirmStart = () => {
    if (tempStartDate) {
      setCurrentStep("end");
    }
  };

  // Шаг 3: Выбор конечной даты
  const handleEndDateSelect = (date) => {
    setTempEndDate(date);
  };

  const handleConfirmEnd = () => {
    if (tempEndDate) {
      if (tempStartDate && tempEndDate) {
        const from = isBefore(tempStartDate, tempEndDate) ? tempStartDate : tempEndDate;
        const to = isBefore(tempStartDate, tempEndDate) ? tempEndDate : tempStartDate;
        setSelectedRange({ from, to });
      }
      setCurrentStep("confirm");
    }
  };

  const handleBack = () => {
    switch (currentStep) {
      case "start":
        setCurrentStep("range");
        setQuickSelectionType(null);
        break;
      case "end":
        setCurrentStep("start");
        break;
      case "confirm":
        if (tempStartDate && tempEndDate) {
          setCurrentStep("end");
        } else {
          setCurrentStep("range");
          setQuickSelectionType(null);
        }
        break;
      default:
        setCurrentStep("range");
        setQuickSelectionType(null);
    }
  };

  const handleConfirm = () => {
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
    
    localStorage.setItem("userDateRangePreference", JSON.stringify({
      date_from: dateFrom,
      date_to: dateTo
    }));
    localStorage.setItem("hasDatePreference", "true");

    const regionToSave = selectedRegion === "all" ? null : selectedRegion;
    localStorage.setItem("userRegionPreference", regionToSave || "");
    
    onDateSelect({
      date_from: dateFrom,
      date_to: dateTo,
      region: regionToSave
    });
    onClose();
  };

  const handleSkip = () => {
    localStorage.setItem("hasDatePreference", "skipped");
    onClose();
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

  const getStepTitle = () => {
    switch (currentStep) {
      case "range":
        return "Выберите период и регион";
      case "start":
        return "Выберите начальную дату";
      case "end":
        return "Выберите конечную дату";
      case "confirm":
        return "Подтвердите выбор";
      default:
        return "Выберите период и регион";
    }
  };

  const getStepSubtitle = () => {
    switch (currentStep) {
      case "range":
        return isFirstTime 
          ? "Выберите готовый диапазон или настройте вручную" 
          : "Измените период или выберите новый";
      case "start":
        return "Укажите дату начала периода";
      case "end":
        return "Укажите дату окончания периода";
      case "confirm":
        return "Проверьте выбранный диапазон дат и регион";
      default:
        return "Выберите диапазон дат для просмотра статистики";
    }
  };

  // Получение названия выбранного региона
  const getSelectedRegionName = () => {
    const region = regions.find(r => r.value === selectedRegion);
    return region ? region.label : "Все регионы";
  };

  // Получение описания быстрого выбора
  const getQuickSelectionDescription = () => {
    const dates = getDynamicDates();
    
    switch (quickSelectionType) {
      case "allTime":
        return `Весь период с ${format(dates.allTimeStart, "d MMM yyyy", { locale: ru })} по ${format(dates.allTimeEnd, "d MMM yyyy", { locale: ru })}`;
      case "currentYear":
        return `Текущий год с ${format(dates.currentYearStart, "d MMM yyyy", { locale: ru })} по ${format(dates.currentYearEnd, "d MMM yyyy", { locale: ru })}`;
      default:
        return "";
    }
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
          <h2 className={styles.title}>{getStepTitle()}</h2>
          <p className={styles.subtitle}>{getStepSubtitle()}</p>
          
          {/* Информация о доступных данных */}
          {dateBounds.hasRealData && (
            <div className={styles.dataInfo}>
              Данные доступны с {format(dateBounds.minDate, "d MMM yyyy", { locale: ru })} по {format(dateBounds.maxDate, "d MMM yyyy", { locale: ru })}
            </div>
          )}
        </div>

        {/* Индикатор прогресса */}
        <div className={styles.progress}>
          <div className={`${styles.progressStep} ${currentStep === "range" ? styles.active : ""}`}>
            <span className={styles.stepNumber}>1</span>
            <span className={styles.stepLabel}>Период</span>
          </div>
          <div className={`${styles.progressStep} ${currentStep === "start" ? styles.active : ""}`}>
            <span className={styles.stepNumber}>2</span>
            <span className={styles.stepLabel}>Начало</span>
          </div>
          <div className={`${styles.progressStep} ${currentStep === "end" ? styles.active : ""}`}>
            <span className={styles.stepNumber}>3</span>
            <span className={styles.stepLabel}>Конец</span>
          </div>
          <div className={`${styles.progressStep} ${currentStep === "confirm" ? styles.active : ""}`}>
            <span className={styles.stepNumber}>4</span>
            <span className={styles.stepLabel}>Подтверждение</span>
          </div>
        </div>

        {/* Шаг 1: Выбор типа диапазона и региона */}
        {currentStep === "range" && (
          <div className={styles.rangeSelection}>
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

            {/* Выбор региона */}
            <div className={styles.regionSelection}>
              <label className={styles.regionLabel}>
                Регион
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
              <div className={styles.regionDescription}>
                {isLoadingRegions ? "Загрузка списка регионов..." : "Выберите регион для просмотра статистики"}
              </div>
            </div>

            {/* Кастомный выбор периода */}
            <div className={styles.customSelection}>
              <button
                className={styles.customOption}
                onClick={() => handleRangeTypeSelect("custom")}
                type="button"
              >
                <span className={styles.customTitle}>
                  {isFirstTime ? 'Настроить период вручную' : 'Выбрать другой период'}
                </span>
                <span className={styles.customDescription}>
                  {dateBounds.hasRealData 
                    ? `Выберите даты в диапазоне ${format(dateBounds.minDate, "d MMM yyyy", { locale: ru })} - ${format(dateBounds.maxDate, "d MMM yyyy", { locale: ru })}`
                    : 'Выберите точные даты начала и окончания'
                  }
                </span>
              </button>
            </div>

            {/* Предпросмотр быстрого выбора */}
            {/* {quickSelectionType && (
              <div className={styles.quickSelectionPreview}>
                <div className={styles.previewCard}>
                  <div className={styles.previewHeader}>
                    <span className={styles.previewTitle}>Выбранный период:</span>
                    <span className={styles.previewValue}>
                      {getQuickSelectionDescription()}
                    </span>
                  </div>
                  <div className={styles.previewHeader}>
                    <span className={styles.previewTitle}>Выбранный регион:</span>
                    <span className={styles.previewValue}>
                      {getSelectedRegionLabel()}
                    </span>
                  </div>
                </div>
              </div>
            )} */}
          </div>
        )}

        {/* Шаг 2: Выбор начальной даты */}
        {currentStep === "start" && (
          <div className={styles.dateSelection}>
            <div className={styles.datePickerWrapper}>
              <DatePicker
                selected={tempStartDate}
                onChange={handleStartDateSelect}
                selectsStart
                startDate={tempStartDate}
                endDate={tempEndDate}
                maxDate={dateBounds.maxDate}
                minDate={dateBounds.minDate}
                locale={ru}
                dateFormat="dd MMM yyyy"
                placeholderText="Выберите начальную дату"
                className={styles.datePickerInput}
                wrapperClassName={styles.datePickerContainer}
                calendarClassName={styles.datePickerCalendar}
                dayClassName={() => styles.datePickerDay}
                todayButton="Сегодня"
                showYearDropdown
                scrollableYearDropdown
                yearDropdownItemNumber={dateBounds.yearsRange.length}
                showMonthDropdown
                useWeekdaysShort
                inline
                renderCustomHeader={({
                  date,
                  decreaseMonth,
                  increaseMonth,
                  prevMonthButtonDisabled,
                  nextMonthButtonDisabled,
                }) => (
                  <div className={styles.customHeader}>
                    <button
                      type="button"
                      onClick={decreaseMonth}
                      disabled={prevMonthButtonDisabled}
                      className={styles.navButton}
                    >
                      <span className={styles.navIcon}>‹</span>
                    </button>
                    
                    <div className={styles.monthYearContainer}>
                      <select
                        value={date.getMonth()}
                        onChange={({ target: { value } }) => {
                          const newDate = new Date(date);
                          newDate.setMonth(parseInt(value, 10));
                          handleStartDateSelect(newDate);
                        }}
                        className={styles.monthSelect}
                      >
                        {[...Array(12)].map((_, i) => (
                          <option key={i} value={i}>
                            {format(new Date(date.getFullYear(), i, 1), "MMMM", { locale: ru })}
                          </option>
                        ))}
                      </select>
                      
                      <select
                        value={date.getFullYear()}
                        onChange={({ target: { value } }) => {
                          const newDate = new Date(date);
                          newDate.setFullYear(parseInt(value, 10));
                          handleStartDateSelect(newDate);
                        }}
                        className={styles.yearSelect}
                      >
                        {dateBounds.yearsRange.map(year => (
                          <option key={year} value={year}>
                            {year}
                          </option>
                        ))}
                      </select>
                    </div>
                    
                    <button
                      type="button"
                      onClick={increaseMonth}
                      disabled={nextMonthButtonDisabled}
                      className={styles.navButton}
                    >
                      <span className={styles.navIcon}>›</span>
                    </button>
                  </div>
                )}
              />
            </div>
            
            <div className={styles.selectedPreview}>
              <span className={styles.previewLabel}>Начальная дата:</span>
              <span className={styles.previewValue}>
                {tempStartDate ? format(tempStartDate, "d MMM yyyy", { locale: ru }) : "Не выбрана"}
              </span>
            </div>
          </div>
        )}

        {/* Шаг 3: Выбор конечной даты */}
        {currentStep === "end" && (
          <div className={styles.dateSelection}>
            <div className={styles.datePickerWrapper}>
              <DatePicker
                selected={tempEndDate}
                onChange={handleEndDateSelect}
                selectsEnd
                startDate={tempStartDate}
                endDate={tempEndDate}
                minDate={tempStartDate || dateBounds.minDate}
                maxDate={dateBounds.maxDate}
                locale={ru}
                dateFormat="dd MMM yyyy"
                placeholderText="Выберите конечную дату"
                className={styles.datePickerInput}
                wrapperClassName={styles.datePickerContainer}
                calendarClassName={styles.datePickerCalendar}
                dayClassName={() => styles.datePickerDay}
                todayButton="Сегодня"
                showYearDropdown
                scrollableYearDropdown
                yearDropdownItemNumber={dateBounds.yearsRange.length}
                showMonthDropdown
                useWeekdaysShort
                inline
                renderCustomHeader={({
                  date,
                  decreaseMonth,
                  increaseMonth,
                  prevMonthButtonDisabled,
                  nextMonthButtonDisabled,
                }) => (
                  <div className={styles.customHeader}>
                    <button
                      type="button"
                      onClick={decreaseMonth}
                      disabled={prevMonthButtonDisabled}
                      className={styles.navButton}
                    >
                      <span className={styles.navIcon}>‹</span>
                    </button>
                    
                    <div className={styles.monthYearContainer}>
                      <select
                        value={date.getMonth()}
                        onChange={({ target: { value } }) => {
                          const newDate = new Date(date);
                          newDate.setMonth(parseInt(value, 10));
                          handleEndDateSelect(newDate);
                        }}
                        className={styles.monthSelect}
                      >
                        {[...Array(12)].map((_, i) => (
                          <option key={i} value={i}>
                            {format(new Date(date.getFullYear(), i, 1), "MMMM", { locale: ru })}
                          </option>
                        ))}
                      </select>
                      
                      <select
                        value={date.getFullYear()}
                        onChange={({ target: { value } }) => {
                          const newDate = new Date(date);
                          newDate.setFullYear(parseInt(value, 10));
                          handleEndDateSelect(newDate);
                        }}
                        className={styles.yearSelect}
                      >
                        {dateBounds.yearsRange.map(year => (
                          <option key={year} value={year}>
                            {year}
                          </option>
                        ))}
                      </select>
                    </div>
                    
                    <button
                      type="button"
                      onClick={increaseMonth}
                      disabled={nextMonthButtonDisabled}
                      className={styles.navButton}
                    >
                      <span className={styles.navIcon}>›</span>
                    </button>
                  </div>
                )}
              />
            </div>
            
            <div className={styles.selectedPreview}>
              <span className={styles.previewLabel}>Начальная дата:</span>
              <span className={styles.previewValue}>
                {tempStartDate ? format(tempStartDate, "d MMM yyyy", { locale: ru }) : "Не выбрана"}
              </span>
              <span className={styles.previewLabel}>Конечная дата:</span>
              <span className={styles.previewValue}>
                {tempEndDate ? format(tempEndDate, "d MMM yyyy", { locale: ru }) : "Не выбрана"}
              </span>
            </div>
          </div>
        )}

        {/* Шаг 4: Подтверждение */}
        {currentStep === "confirm" && (
          <div className={styles.confirmation}>
            <div className={styles.finalSelection}>
              <div className={styles.dateCard}>
                <span className={styles.dateLabel}>Выбранный период:</span>
                <span className={styles.dateRange}>
                  {formatDateRange()}
                </span>
              </div>
              
              <div className={styles.regionCard}>
                <span className={styles.regionCardLabel}>Выбранный регион:</span>
                <span className={styles.regionCardValue}>
                  {getSelectedRegionName()}
                </span>
              </div>
              
              <div className={styles.dateDetails}>
                <div className={styles.dateDetail}>
                  <span className={styles.detailLabel}>Начало:</span>
                  <span className={styles.detailValue}>
                    {selectedRange.from ? format(selectedRange.from, "d MMM yyyy", { locale: ru }) : "Не указано"}
                  </span>
                </div>
                <div className={styles.dateDetail}>
                  <span className={styles.detailLabel}>Окончание:</span>
                  <span className={styles.detailValue}>
                    {selectedRange.to ? format(selectedRange.to, "d MMM yyyy", { locale: ru }) : "Не указано"}
                  </span>
                </div>
              </div>
            </div>
          </div>
        )}

        {/* Действия */}
        <div className={styles.actions}>
          {currentStep !== "range" && (
            <button
              className={styles.backButton}
              onClick={handleBack}
              type="button"
            >
              Назад
            </button>
          )}
          
          <button
            className={styles.skipButton}
            onClick={handleSkip}
            type="button"
          >
            Пропустить
          </button>
          
          {currentStep === "range" && quickSelectionType && (
            <button
              className={styles.confirmButton}
              onClick={handleQuickSelectionConfirm}
              type="button"
            >
              Далее
            </button>
          )}
          
          {currentStep === "start" && (
            <button
              className={styles.confirmButton}
              onClick={handleConfirmStart}
              disabled={!tempStartDate}
              type="button"
            >
              Далее
            </button>
          )}
          
          {currentStep === "end" && (
            <button
              className={styles.confirmButton}
              onClick={handleConfirmEnd}
              disabled={!tempEndDate}
              type="button"
            >
              Далее
            </button>
          )}
          
          {currentStep === "confirm" && (
            <button
              className={styles.confirmButton}
              onClick={handleConfirm}
              disabled={!selectedRange.from && !selectedRange.to}
              type="button"
            >
              Подтвердить
            </button>
          )}
        </div>
      </div>
    </div>
  );
};

export default DatePickerPopup;