// components/TableManager/TableManager.jsx
import React, { useState, useEffect } from "react";
import Image from "next/image";
import styles from "./TableManager.module.css";
import { tableService } from "@/utils/tableService";
import { useTable } from '@/contexts/TableContext';

const TableManager = ({ isOpen, onClose, onTableSelect, onTablesUpdate, user }) => {
    const [tables, setTables] = useState([]);
    const [loading, setLoading] = useState(false);
    const [uploading, setUploading] = useState(false);
    const [activeView, setActiveView] = useState("tables");
    const [searchTerm, setSearchTerm] = useState("");
    const [selectedFile, setSelectedFile] = useState(null);
    const [uploadType, setUploadType] = useState("new");

    const { currentTable, selectTable, isLoading: tableLoading } = useTable();

    const isAdmin = user?.role === "admin";

    // Загрузить список таблиц
    const checkCookies = () => {
        console.log("🍪 Current cookies:", document.cookie);
    };

    // В функции loadTables добавьте проверку cookies
    const loadTables = async () => {
        setLoading(true);
        try {
            console.log("🔄 Загрузка таблиц...");

            const data = await tableService.getTables();
            console.log("✅ Таблицы загружены:", data);
            setTables(data.tables || []);

        } catch (error) {
            console.error("❌ Ошибка загрузки таблиц:", error);
            alert("Не удалось загрузить список таблиц. Проверьте консоль для деталей.");
        } finally {
            setLoading(false);
        }
    };

    // Выбрать таблицу
    const handleSelectTable = async (table) => {
        try {
            setLoading(true);
            console.log("🎯 Выбираем таблицу:", table.table_name);

            const success = await selectTable(table.table_name);

            if (success) {
                // Обновляем список таблиц
                await loadTables();

                if (onTablesUpdate) {
                    onTablesUpdate();
                }

                console.log("✅ Таблица успешно выбрана:", table.table_name);
            }
        } catch (error) {
            console.error("Ошибка выбора таблицы:", error);
            alert("Ошибка при выборе таблицы: " + error.message);
        } finally {
            setLoading(false);
        }
    };

    // Удалить таблицу
    const handleDeleteTable = async (table) => {
        if (!window.confirm(`Удалить таблицу "${table.table_name}"?`)) return;

        try {
            await tableService.deleteTable(table.table_name);

            // Обновляем список
            await loadTables();

            if (onTablesUpdate) {
                onTablesUpdate();
            }
        } catch (error) {
            console.error("Ошибка удаления:", error);
        }
    };

    // Загрузить файл
    const handleFileUpload = async () => {
        if (!selectedFile) return;

        setUploading(true);
        try {
            const result = await tableService.uploadFile(selectedFile, uploadType);

            if (result.success) {
                // Сбрасываем форму
                setSelectedFile(null);
                setUploadType("new");

                // Переключаемся на список таблиц и обновляем
                setActiveView("tables");
                await loadTables();

                if (onTablesUpdate) {
                    onTablesUpdate();
                }
            }
        } catch (error) {
            console.error("Ошибка загрузки:", error);
        } finally {
            setUploading(false);
        }
    };

    // Обработчик выбора файла
    const handleFileSelect = (event) => {
        const file = event.target.files[0];
        if (file && file.name.match(/\.(xlsx|xls)$/)) {
            setSelectedFile(file);
        }
    };

    // Фильтрация таблиц по поиску
    const filteredTables = tables.filter(table =>
        table.table_name.toLowerCase().includes(searchTerm.toLowerCase()) ||
        (table.original_filename && table.original_filename.toLowerCase().includes(searchTerm.toLowerCase()))
    );

    useEffect(() => {
        if (isOpen) {
            loadTables();
            setSearchTerm("");
            setSelectedFile(null);
            setUploadType("new");
        }
    }, [isOpen]);

    if (!isOpen) return null;

    return (
        <div className={styles.overlay} onClick={onClose}>
            <div className={styles.modal} onClick={(e) => e.stopPropagation()}>

                {/* Header */}
                <div className={styles.header}>
                    <div className={styles.headerContent}>
                        <div className={styles.titleSection}>
                            <div className={styles.icon}>
                                <Image src="/svg/tableWhite.svg" width={24} height={24} alt="Tables" />
                            </div>
                            <div>
                                <h2>Управление данными</h2>
                                <p>Работа с таблицами и наборами данных</p>
                            </div>
                        </div>
                        <button className={styles.closeBtn} onClick={onClose}>
                            <Image src="/svg/close.svg" width={20} height={20} alt="Close" />
                        </button>
                    </div>

                    {/* Navigation */}
                    <div className={styles.nav}>
                        <button
                            className={`${styles.navItem} ${activeView === "tables" ? styles.active : ""}`}
                            onClick={() => setActiveView("tables")}
                        >
                            Таблицы
                            <span className={styles.badge}>{tables.length}</span>
                        </button>

                        {isAdmin && (
                            <button
                                className={`${styles.navItem} ${activeView === "upload" ? styles.active : ""}`}
                                onClick={() => setActiveView("upload")}
                            >
                                Загрузить данные
                            </button>
                        )}
                    </div>
                </div>

                {/* Content */}
                <div className={styles.content}>

                    {activeView === "tables" ? (
                        /* Tables View */
                        <div className={styles.tablesView}>

                            {/* Search and Actions */}
                            <div className={styles.actionsBar}>
                                <div className={styles.searchBox}>
                                    <Image src="/svg/search.svg" width={16} height={16} alt="Search" />
                                    <input
                                        type="text"
                                        placeholder="Поиск таблиц..."
                                        value={searchTerm}
                                        onChange={(e) => setSearchTerm(e.target.value)}
                                        className={styles.searchInput}
                                    />
                                </div>
                                <button
                                    className={styles.refreshBtn}
                                    onClick={loadTables}
                                    disabled={loading}
                                >
                                    <Image src="/svg/carrier.svg" width={16} height={16} alt="Refresh" />
                                    {loading ? "Обновление..." : "Обновить"}
                                </button>
                            </div>

                            {/* Tables List */}
                            <div className={styles.tablesList}>
                                {loading ? (
                                    <div className={styles.loadingState}>
                                        <div className={styles.spinner}></div>
                                        <p>Загрузка таблиц...</p>
                                    </div>
                                ) : filteredTables.length === 0 ? (
                                    <div className={styles.emptyState}>
                                        <div className={styles.emptyIcon}>📊</div>
                                        <h3>Таблицы не найдены</h3>
                                        <p>{searchTerm ? "Попробуйте изменить поисковый запрос" : "Нет доступных таблиц"}</p>
                                    </div>
                                ) : (
                                    <div className={styles.tableGrid}>
                                        {filteredTables.map((table) => (
                                            <div
                                                key={table.table_name}
                                                className={`${styles.tableCard} ${currentTable?.table_name === table.table_name ? styles.active : ''
                                                    }`}
                                            >
                                                <div className={styles.tableHeader}>
                                                    <div className={styles.tableIcon}>
                                                        <Image src="/svg/excel.svg" width={20} height={20} alt="Table" />
                                                    </div>
                                                    <div className={styles.tableInfo}>
                                                        <h4 className={styles.tableName}>{table.table_name}</h4>
                                                        <p className={styles.tableMeta}>
                                                            {table.original_filename || "Импортированная таблица"}
                                                        </p>
                                                    </div>
                                                    {currentTable?.table_name === table.table_name && (
                                                        <div className={styles.currentBadge}>Текущая</div>
                                                    )}
                                                </div>

                                                <div className={styles.tableStats}>
                                                    <div className={styles.stat}>
                                                        <span className={styles.statValue}>{table.records_count || 0}</span>
                                                        <span className={styles.statLabel}>записей</span>
                                                    </div>
                                                    {table.description && (
                                                        <div className={styles.description}>
                                                            {table.description}
                                                        </div>
                                                    )}
                                                </div>

                                                <div className={styles.tableActions}>
                                                    <button
                                                        className={`${styles.actionBtn} ${styles.primary}`}
                                                        onClick={() => handleSelectTable(table)}
                                                        disabled={currentTable?.table_name === table.table_name}
                                                    >
                                                        {currentTable?.table_name === table.table_name ? (
                                                            <>Выбрана</>
                                                        ) : (
                                                            <>Выбрать</>
                                                        )}
                                                    </button>

                                                    {isAdmin && (
                                                        <button
                                                            className={`${styles.actionBtn} ${styles.danger}`}
                                                            onClick={() => handleDeleteTable(table)}
                                                        >
                                                            Удалить
                                                        </button>
                                                    )}
                                                </div>
                                            </div>
                                        ))}
                                    </div>
                                )}
                            </div>
                        </div>
                    ) : (
                        /* Upload View */
                        <div className={styles.uploadView}>
                            <div className={styles.uploadCard}>
                                <div className={styles.uploadHeader}>
                                    <div>
                                        <h3>Загрузка данных</h3>
                                        <p>Импортируйте данные из Excel-файла</p>
                                    </div>
                                </div>

                                {/* Upload Type Selection */}
                                <div className={styles.uploadType}>
                                    <label className={styles.uploadTypeLabel}>Тип загрузки:</label>
                                    <div className={styles.uploadTypeOptions}>
                                        <label className={styles.radioOption}>
                                            <input
                                                type="radio"
                                                value="new"
                                                checked={uploadType === "new"}
                                                onChange={(e) => setUploadType(e.target.value)}
                                            />
                                            <span className={styles.radioCustom}></span>
                                            <div className={styles.radioContent}>
                                                <strong>Новая таблица</strong>
                                                <span>Создать полностью новую таблицу</span>
                                            </div>
                                        </label>

                                        <label className={styles.radioOption}>
                                            <input
                                                type="radio"
                                                value="append"
                                                checked={uploadType === "append"}
                                                onChange={(e) => setUploadType(e.target.value)}
                                            />
                                            <span className={styles.radioCustom}></span>
                                            <div className={styles.radioContent}>
                                                <strong>Добавить данные</strong>
                                                <span>Дополнить текущую таблицу</span>
                                            </div>
                                        </label>
                                    </div>
                                </div>

                                {/* File Upload Area */}
                                <div className={styles.uploadArea}>
                                    <input
                                        type="file"
                                        id="file-upload"
                                        accept=".xlsx, .xls"
                                        onChange={handleFileSelect}
                                        className={styles.fileInput}
                                    />

                                    <label htmlFor="file-upload" className={styles.uploadDropzone}>
                                        {selectedFile ? (
                                            <div className={styles.fileSelected}>
                                                <div className={styles.fileIcon}>📄</div>
                                                <div className={styles.fileInfo}>
                                                    <strong>{selectedFile.name}</strong>
                                                    <span>{(selectedFile.size / 1024 / 1024).toFixed(2)} MB</span>
                                                </div>
                                                <button
                                                    type="button"
                                                    className={styles.removeFile}
                                                    onClick={(e) => {
                                                        e.stopPropagation();
                                                        setSelectedFile(null);
                                                    }}
                                                >
                                                    <Image src="/svg/close.svg" width={16} height={16} alt="Remove" />
                                                </button>
                                            </div>
                                        ) : (
                                            <div className={styles.uploadPrompt}>
                                                <div className={styles.uploadPromptIcon}><Image src="/svg/Load.svg" width={36} height={36} alt="Upload" /></div>
                                                <div style={{ flexDirection: 'column', display: "flex" }}>
                                                    <strong>Выберите файл Excel</strong>
                                                    <span>Перетащите или нажмите для выбора</span>
                                                    <span className={styles.fileTypes}>Поддерживаемые форматы: .xlsx, .xls</span>
                                                </div>
                                            </div>
                                        )}
                                    </label>

                                    {/* Upload Button */}
                                    <button
                                        className={styles.uploadButton}
                                        onClick={handleFileUpload}
                                        disabled={!selectedFile || uploading}
                                    >
                                        {uploading ? (
                                            <>
                                                <div className={styles.spinner}></div>
                                                Загрузка...
                                            </>
                                        ) : (
                                            <>
                                                Начать загрузку
                                            </>
                                        )}
                                    </button>
                                </div>
                            </div>
                        </div>
                    )}
                </div>
            </div>
        </div>
    );
};

export default TableManager;