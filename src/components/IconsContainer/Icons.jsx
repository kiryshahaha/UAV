import React, { useState } from 'react';
import Image from 'next/image';
import styles from './Icons.module.css';

const Icons = () => {
    const [message, setMessage] = useState(''); // Для сообщений об успехе/ошибке

    const handleFileUpload = async (event) => {
        const file = event.target.files[0];
        if (!file) return;

        const formData = new FormData();
        formData.append('file', file);

        try {
            const response = await fetch('http://localhost:8000/api/upload', {
                method: 'POST',
                body: formData,
            });

            if (response.ok) {
                const result = await response.json();
                setMessage(`Успешно загружено: ${result.message || 'Данные обработаны'}`);
            } else {
                const error = await response.json();
                setMessage(`Ошибка: ${error.detail || 'Не удалось загрузить файл'}`);
            }
        } catch (error) {
            setMessage(`Ошибка сети: ${error.message}`);
        }
    };

    return (
        <div className={styles.iconsContainer}>
            <div className={styles.icon}>
                {/* Кнопка для загрузки файла */}
                <div className={styles.imageWrapper}>
                    <label htmlFor="upload-input">
                        <Image 
                            src='/svg/Load.svg' 
                            fill
                            style={{objectFit: 'contain', cursor: 'pointer'}}
                            alt='download-icon' 
                        />
                    </label>
                    <input
                        id="upload-input"
                        type="file"
                        accept=".xlsx, .xls"
                        onChange={handleFileUpload}
                        style={{ display: 'none' }} // Скрываем input, используем иконку как label
                    />
                </div>
                {message && <p className={styles.message}>{message}</p>}
            </div>
            <div className={styles.icon}>
                <div className={styles.imageWrapper}>
                    <Image 
                        src='/svg/stat.svg' 
                        fill
                        style={{objectFit: 'contain'}}
                        alt='stat-icon' 
                    />
                </div>
            </div>
            <div className={styles.icon}>
                <div className={styles.imageWrapper}>
                    <Image 
                        src='/svg/brush.svg' 
                        fill
                        style={{objectFit: 'contain'}}
                        alt='brush-icon' 
                    />
                </div>
            </div>
        </div>
    );
};

export default Icons;