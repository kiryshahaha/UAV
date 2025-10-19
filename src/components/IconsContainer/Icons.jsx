// Icons.jsx
import React, { useState, useRef, useEffect } from "react";
import Image from "next/image";
import styles from "./Icons.module.css";
import DatePickerTrigger from "../DatePickerPopup/DatePickerTrigger";
import DashboardPopup from "../DashboardPopup/DashboardPopup";
import OperatorsPopup from "../OperatorsPopup/OperatorsPopup";

import PresentationGenerator from "../PowerpointPopUp/PresentationGenerator";
import PdfGenerator from "../PdfPopUp/PdfGenerator"
const Icons = ({
  onBrushClick,
  onStatsClick,
  user,
  onOpenDatePicker,
  selectedDate,
  dateRange,
  selectedRegion,
  onOpenTableManager,
}) => {
  const [isPDFGeneratorOpen, setIsPDFGeneratorOpen] = useState(false);
  const [isPresentationGeneratorOpen, setIsPresentationGeneratorOpen] =
    useState(false);
  const [isStatsExpanded, setIsStatsExpanded] = useState(false);
  const [isDashboardOpen, setIsDashboardOpen] = useState(false);
  const [isOperatorsOpen, setIsOperatorsOpen] = useState(false);
  const isAdmin = user?.role === "admin";
  const statsContainerRef = useRef(null);

  // Обработчик клика вне компонента
  useEffect(() => {
    const handleClickOutside = (event) => {
      if (
        statsContainerRef.current &&
        !statsContainerRef.current.contains(event.target)
      ) {
        setIsStatsExpanded(false);
      }
    };

    document.addEventListener("mousedown", handleClickOutside);
    return () => {
      document.removeEventListener("mousedown", handleClickOutside);
    };
  }, []);

  const handleStatsClick = () => {
    setIsStatsExpanded(!isStatsExpanded);
  };

  const handleMainStatsClick = () => {
    setIsStatsExpanded(false);
    if (onStatsClick) {
      onStatsClick();
    }
  };

  const handleDashboardClick = () => {
    setIsStatsExpanded(false);
    setIsDashboardOpen(true);
  };

  const handleCloseDashboard = () => {
    setIsDashboardOpen(false);
  };

  const handleOperatorsClick = () => {
    setIsStatsExpanded(false);
    setIsOperatorsOpen(true);
  };

  const handleCloseOperators = () => {
    setIsOperatorsOpen(false);
  };

  const handleTableManagerClick = () => {
    if (onOpenTableManager) {
      onOpenTableManager();
    }
  };
  const handlePresentationClick = () => {
    setIsStatsExpanded(false);
    setIsPresentationGeneratorOpen(true);
  };
  const handleClosePresentationGenerator = () => {
    setIsPresentationGeneratorOpen(false);
  };

  const handleClosePDFGenerator = () => {
    setIsPDFGeneratorOpen(false);
  };

  const handleGeneratePDF = async (params) => {
    console.log("PDF отчет сгенерирован с параметрами:", params);
  };
  const handlePDFClick = () => {
    setIsStatsExpanded(false);
    setIsPDFGeneratorOpen(true);
  };
  return (
    <>
      <div className={styles.iconsContainer}>
        {/* Table Manager - для всех пользователей, но с разными правами */}
        <div className={styles.icon} onClick={handleTableManagerClick}>
          <div className={styles.imageWrapper}>
            <Image
              src="/svg/table.svg"
              fill
              style={{ objectFit: "contain", cursor: "pointer" }}
              alt="table-manager-icon"
            />
          </div>
        </div>

        {/* Кнопка выбора даты */}
        <div className={styles.icon}>
          <DatePickerTrigger
            onOpen={onOpenDatePicker}
            selectedDate={selectedDate}
          />
        </div>

        {/* Контейнер для статистики с выдвигающимися кнопками */}
        <div className={styles.statsContainer} ref={statsContainerRef}>
          <div
            className={`${styles.icon} ${isStatsExpanded ? styles.active : ""}`}
            onClick={handleStatsClick}
          >
            <div className={styles.imageWrapper}>
              <Image
                src="/svg/statsGroup.svg"
                fill
                style={{ objectFit: "contain", cursor: "pointer" }}
                alt="stat-icon"
              />
            </div>
          </div>

          <div
            className={`${styles.statsSubmenu} ${
              isStatsExpanded ? styles.expanded : ""
            }`}
          >
            <div className={styles.submenuIcon} onClick={handleMainStatsClick}>
              <div className={styles.imageWrapper}>
                <Image
                  src="/svg/stat.svg"
                  fill
                  style={{ objectFit: "contain", cursor: "pointer" }}
                  alt="main-stat-icon"
                />
              </div>
            </div>

            <div className={styles.submenuIcon} onClick={handleOperatorsClick}>
              <div className={styles.imageWrapper}>
                <Image
                  src="/svg/operator.svg"
                  fill
                  style={{ objectFit: "contain", cursor: "pointer" }}
                  alt="operator-icon"
                />
              </div>
            </div>

            <div className={styles.submenuIcon} onClick={handleDashboardClick}>
              <div className={styles.imageWrapper}>
                <Image
                  src="/svg/dashboard.svg"
                  fill
                  style={{ objectFit: "contain", cursor: "pointer" }}
                  alt="dashboard-icon"
                />
              </div>
            </div>

            {/*кнопка презентации */}
            <div
              className={styles.submenuIcon}
              onClick={handlePresentationClick}
            >
              <div className={styles.imageWrapper}>
                <Image
                  src="/svg/presentation.svg"
                  fill
                  style={{ objectFit: "contain", cursor: "pointer" }}
                  alt="dashboard-icon"
                />
              </div>
            </div>

            {/*кнопка pdf отчета */}
            <div className={styles.submenuIcon} onClick={handlePDFClick}>
              <div className={styles.imageWrapper}>
                <Image
                  src="/svg/pdf.svg"
                  fill
                  style={{ objectFit: "contain", cursor: "pointer" }}
                  alt="dashboard-icon"
                />
              </div>
            </div>
          </div>
        </div>

        {/* Кисть для смены карты */}
        <div
          className={styles.icon}
          onClick={() => onBrushClick && onBrushClick()}
        >
          <div className={styles.imageWrapper}>
            <Image
              src="/svg/brush.svg"
              fill
              style={{ objectFit: "contain", cursor: "pointer" }}
              alt="brush-icon"
            />
          </div>
        </div>
      </div>

      {/* Попап дашборда */}
      <DashboardPopup
        isOpen={isDashboardOpen}
        onClose={handleCloseDashboard}
        dateRange={dateRange}
        selectedRegion={selectedRegion}
      />

      <OperatorsPopup
        isOpen={isOperatorsOpen}
        onClose={handleCloseOperators}
        dateRange={dateRange}
        selectedRegion={selectedRegion}
      />
      {/* Генератор презентаций */}
      <PresentationGenerator
        isOpen={isPresentationGeneratorOpen}
        onClose={handleClosePresentationGenerator}
        dateRange={dateRange}
        selectedRegion={selectedRegion}
      />
      {/* Генератор PDF */}
      <PdfGenerator
        isOpen={isPDFGeneratorOpen}
        onClose={handleClosePDFGenerator}
        onGenerate={handleGeneratePDF}
        dateRange={dateRange}
        selectedRegion={selectedRegion}
      />
    </>
  );
};

export default Icons;
