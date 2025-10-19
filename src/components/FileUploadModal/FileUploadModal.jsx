// FileUploadModal.jsx
import React, { useState } from "react";
import Image from "next/image";
import styles from "./FileUploadModal.module.css";
import { tableService } from "@/utils/tableService";

const FileUploadModal = ({ isOpen, onClose, user, onTablesUpdate }) => {
  const [message, setMessage] = useState("");
  const [isLoading, setIsLoading] = useState(false);
  const [uploadType, setUploadType] = useState('new'); // 'new' или 'append'

  const handleFileUpload = async (event) => {
    const file = event.target.files[0];
    if (!file) return;

    // Проверка типа файла
    if (!file.name.match(/\.(xlsx|xls)$/)) {
      setMessage("Ошибка: разрешены только файлы .xlsx и .xls");
      return;
    }

    setIsLoading(true);
    setMessage("");

    try {
      const result = await tableService.uploadFile(file, uploadType);

      if (result.success) {
        setMessage(`✅ ${result.message}`);
        
        // Обновляем список таблиц в родительском компоненте
        if (onTablesUpdate) {
          onTablesUpdate();
        }

        setTimeout(() => {
          onClose();
          setMessage("");
        }, 3000);
      } else {
        setMessage(`❌ ${result.message || result.error}`);
      }
    } catch (error) {
      setMessage(`❌ Ошибка загрузки: ${error.message}`);
    } finally {
      setIsLoading(false);
    }
  };

  if (!isOpen) return null;

  return (
    <div className={styles.modalOverlay} onClick={onClose}>
      <div className={styles.modalContent} onClick={(e) => e.stopPropagation()}>
        <div className={styles.modalHeader}>
          <h2>Загрузка файла</h2>
          <button className={styles.closeButton} onClick={onClose}>
            <Image src="/svg/close.svg" width={20} height={20} alt="close" />
          </button>
        </div>

        <div className={styles.modalBody}>
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
                Новая таблица
              </label>
              <label className={styles.radioLabel}>
                <input
                  type="radio"
                  value="append"
                  checked={uploadType === 'append'}
                  onChange={(e) => setUploadType(e.target.value)}
                />
                Дозагрузить в текущую
              </label>
            </div>
          </div>

          {/* Загрузка файла */}
          <div className={styles.uploadSection}>
            <label htmlFor="upload-input" className={styles.uploadLabel}>
              <div className={styles.uploadIcon}>
                <Image
                  src="/svg/Load.svg"
                  width={40}
                  height={40}
                  alt="upload"
                />
              </div>
              <span>
                {uploadType === 'new' 
                  ? 'Создать новую таблицу' 
                  : 'Добавить данные в текущую таблицу'
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
              <p>Будет создана новая таблица с уникальным именем</p>
            ) : (
              <p>Данные будут добавлены в текущую активную таблицу</p>
            )}
          </div>

          {/* Сообщение о статусе */}
          {message && (
            <div className={`${styles.message} ${
              message.includes("❌") ? styles.error : styles.success
            }`}>
              {message}
            </div>
          )}

          {/* Индикатор загрузки */}
          {isLoading && (
            <div className={styles.loading}>
              <div className={styles.spinner}></div>
              <span>Загрузка файла...</span>
            </div>
          )}
        </div>
      </div>
    </div>
  );
};

export default FileUploadModal;