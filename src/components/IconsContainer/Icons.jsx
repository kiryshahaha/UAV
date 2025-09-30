'use client'

import React, { useRef } from 'react'
import Image from 'next/image'
import styles from './Icons.module.css'

const Icons = () => {
    const fileInputRef = useRef(null)

    const handleUploadClick = () => {
        // Клик по скрытому input файла
        fileInputRef.current?.click()
    }

    const handleFileChange = async (event) => {
        const file = event.target.files[0]
        if (!file) return

        // Проверяем что это Excel файл
        const isExcel = file.name.endsWith('.xlsx') || file.name.endsWith('.xls')
        if (!isExcel) {
            alert('Пожалуйста, выберите файл Excel (.xlsx или .xls)')
            return
        }

        // Создаем FormData для отправки файла
        const formData = new FormData()
        formData.append('file', file)

        try {
            // Показываем уведомление о начале загрузки
            alert(`Начинается загрузка файла: ${file.name}`)

            // Отправляем файл на бэкенд
            const response = await fetch('http://localhost:8000/api/upload-excel', {
                method: 'POST',
                body: formData,
            })

            if (!response.ok) {
                throw new Error(`Ошибка сервера: ${response.status}`)
            }

            const result = await response.json()
            
            // Показываем результат
            alert(`✅ Файл успешно загружен!\nФайл: ${result.filename}\nСтатус: ${result.message}`)
            
            // Очищаем input чтобы можно было выбрать тот же файл снова
            event.target.value = ''

        } catch (error) {
            console.error('Ошибка загрузки файла:', error)
            alert(`❌ Ошибка при загрузке файла: ${error.message}`)
            
            // Очищаем input при ошибке
            event.target.value = ''
        }
    }

    return (
        <div className={styles.iconsContainer}>
            {/* Скрытый input для выбора файла */}
            <input
                type="file"
                ref={fileInputRef}
                onChange={handleFileChange}
                accept=".xlsx,.xls"
                style={{ display: 'none' }}
            />
            
            {/* Иконка для загрузки файла - ТЕПЕРЬ С ОБРАБОТЧИКОМ */}
            <div className={styles.icon} onClick={handleUploadClick} style={{ cursor: 'pointer' }}>
                <div className={styles.imageWrapper}>
                    <Image 
                        src='/svg/Load.svg' 
                        fill
                        style={{objectFit: 'contain'}}
                        alt='download-icon' 
                    />
                </div>
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
    )
}

export default Icons