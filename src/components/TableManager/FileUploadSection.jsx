import React, { useState } from "react";
import styles from "./TableManager.module.css";
import { tableService } from "@/utils/tableService";

const FileUploadSection = ({ onUploadSuccess, onUploadError }) => {
  const [isLoading, setIsLoading] = useState(false);
  const [uploadType, setUploadType] = useState('new');

  const handleFileUpload = async (event) => {
    const file = event.target.files[0];
    if (!file) return;

    // Проверка типа файла
    if (!file.name.match(/\.(xlsx|xls)$/)) {
      onUploadError("Разрешены только файлы .xlsx и .xls");
      return;
    }

    setIsLoading(true);

    try {
      const result = await tableService.uploadFile(file, uploadType);

      if (result.success) {
        onUploadSuccess(result);
        // Сбрасываем input
        event.target.value = '';
      } else {
        onUploadError(result.message || result.error);
      }
    } catch (error) {
      onUploadError(error.message);
    } finally {
      setIsLoading(false);
    }
  };

  return (
    <div className={styles.uploadSection}>
      <h3>Загрузка Excel-файлов</h3>
      
      {/* Выбор типа загрузки */}
      <div className={styles.uploadTypeSelector}>
        <label>Тип загрузки:</label>
        <div className={styles.uploadTypeOptions}>
          <label className={styles.radioLabel}>
            <input
              type="radio"
              value="new"
              checked={uploadType === 'new'}
              onChange={(e) => setUploadType(e.target.value)}
            />
            📝 Новая таблица
          </label>
          <label className={styles.radioLabel}>
            <input
              type="radio"
              value="append"
              checked={uploadType === 'append'}
              onChange={(e) => setUploadType(e.target.value)}
            />
            ➕ Дозагрузить в текущую
          </label>
        </div>
      </div>

      {/* Загрузка файла */}
      <div className={styles.uploadArea}>
        <label htmlFor="upload-input" className={styles.uploadLabel}>
          <div className={styles.uploadIcon}>
            📁
          </div>
          <span>
            {uploadType === 'new' 
              ? 'Выберите файл для создания новой таблицы' 
              : 'Выберите файл для добавления данных'
            }
          </span>
          <span className={styles.fileTypes}>(.xlsx, .xls)</span>
        </label>
        <input
          id="upload-input"
          type="file"
          accept=".xlsx, .xls"
          onChange={handleFileUpload}
          style={{ display: "none" }}
          disabled={isLoading}
        />
      </div>

      {/* Информация о типах загрузки */}
      <div className={styles.uploadInfo}>
        {uploadType === 'new' ? (
          <div className={styles.infoBox}>
            <h4>Создание новой таблицы</h4>
            <ul>
              <li>Будет создана таблица с уникальным именем</li>
              <li>Данные будут полностью проанализированы</li>
              <li>Автоматически добавятся регионы</li>
              <li>Таблица станет доступна всем пользователям</li>
            </ul>
          </div>
        ) : (
          <div className={styles.infoBox}>
            <h4>Дозагрузка данных</h4>
            <ul>
              <li>Данные будут добавлены в текущую активную таблицу</li>
              <li>Структура данных должна совпадать</li>
              <li>Автоматически обновятся индексы</li>
              <li>Существующие данные не затрагиваются</li>
            </ul>
          </div>
        )}
      </div>

      {/* Индикатор загрузки */}
      {isLoading && (
        <div className={styles.loading}>
          <div className={styles.spinner}></div>
          <span>Загрузка файла...</span>
        </div>
      )}
    </div>
  );
};

export default FileUploadSection;