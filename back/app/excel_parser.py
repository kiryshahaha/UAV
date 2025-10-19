# excel_parser.py
import pandas as pd
import os
from dotenv import load_dotenv
import logging
from sqlalchemy import text
import time

logger = logging.getLogger(__name__)

class ExcelParser:
    """Парсер Excel файлов с загрузкой по требованию"""
    
    def __init__(self):
        self.excel_file_path = os.getenv('EXCEL_FILE_PATH', '/home/xshow/Загрузки/2025.xlsx')
        # УБИРАЕМ авто-парсинг при создании экземпляра
        # self.auto_parse_on_init()
    
    def parse_uploaded_file(self, file_path: str, table_name: str = None) -> dict:
        """
        Парсинг загруженного файла по требованию
        Возвращает результат парсинга
        """
        try:
            if not os.path.exists(file_path):
                logger.error(f"Файл не найден: {file_path}")
                return {"error": "Файл не найден"}
            
            logger.info(f" Начинаем парсинг файла: {file_path}")
            
            from data_processor import DataProcessor
            from database import SessionLocal
            
            db = SessionLocal()
            try:
                # Если имя таблицы не указано, генерируем его
                if not table_name:
                    import time
                    table_name = f"excel_data_{int(time.time())}"
                
                # Проверяем существование таблицы
                table_exists = db.execute(text("""
                    SELECT EXISTS (
                        SELECT 1 FROM information_schema.tables 
                        WHERE table_schema = 'public' AND table_name = :table_name
                    )
                """), {"table_name": table_name}).scalar()
                
                if table_exists:
                    record_count = db.execute(text(f"SELECT COUNT(*) FROM {table_name}")).scalar()
                    if record_count > 0:
                        logger.info(f" Таблица {table_name} уже существует с {record_count} записями")
                        # Можно либо пропустить, либо перезаписать - пока пропускаем
                        db.close()
                        return {
                            "warning": f"Таблица {table_name} уже существует",
                            "records_count": record_count,
                            "table_name": table_name
                        }
                
                logger.info(" Чтение данных из Excel...")
                
                # Читаем все листы
                all_sheets = self.read_all_excel_sheets_from_path(file_path)
                logger.info(f" Прочитано листов: {len(all_sheets)}")
                
                data_processor = DataProcessor(db_session=db)
                total_records = 0
                sheets_processed = []
                
                for sheet_name, df in all_sheets.items():
                    logger.info(f" Обработка листа: {sheet_name}, строк: {len(df)}")
                    
                    from data_processor import DataProcessor as DP
                    df_cleaned = DP.clean_dataframe(df)
                    
                    if not df_cleaned.empty:
                        df_decoded = data_processor.decode_flight_plan_fields(df_cleaned)
                        result = data_processor.save_to_table_with_id(df_decoded, table_name)
                        
                        sheet_records = result.get("added", 0)
                        total_records += sheet_records
                        sheets_processed.append({
                            "sheet_name": sheet_name,
                            "records": sheet_records,
                            "original_rows": len(df)
                        })
                        
                        logger.info(f" Лист '{sheet_name}': сохранено {sheet_records} записей")
                
                logger.info(f" Парсинг завершен! Всего загружено: {total_records} записей")
                
                # Автоматическая обработка дат и регионов
                if total_records > 0:
                    processing_result = self.process_dates_and_regions(table_name)
                    
                    return {
                        "success": True,
                        "message": f"Успешно обработано {len(sheets_processed)} листов",
                        "table_name": table_name,
                        "total_records": total_records,
                        "sheets_processed": sheets_processed,
                        "processing_result": processing_result
                    }
                else:
                    return {
                        "error": "Не удалось обработать данные - нет записей для сохранения",
                        "sheets_processed": sheets_processed
                    }
                
            except Exception as e:
                logger.error(f" Ошибка парсинга: {e}")
                return {"error": f"Ошибка парсинга: {str(e)}"}
            finally:
                db.close()
                
        except Exception as e:
            logger.error(f" Ошибка при парсинге файла: {e}")
            return {"error": f"Ошибка при парсинге файла: {str(e)}"}

    def process_dates_and_regions(self, table_name: str) -> dict:
        """Обработка дат и регионов после парсинга"""
        from database import SessionLocal
        
        db = SessionLocal()
        try:
            result = {
                "dates_processed": 0,
                "regions_processed": 0
            }
            
            # Обработка дат
            dates_updated = self.auto_process_dates(db, table_name)
            result["dates_processed"] = dates_updated
            
            # Даем время на загрузку RegionService
            logger.info(" Ожидание загрузки RegionService...")
            time.sleep(2)
            
            # Обработка регионов
            regions_updated = self.auto_process_regions(db, table_name)
            result["regions_processed"] = regions_updated
            
            logger.info(f" Обработка завершена: {dates_updated} дат, {regions_updated} регионов")
            return result
            
        except Exception as e:
            logger.error(f" Ошибка обработки дат и регионов: {e}")
            return {"error": str(e)}
        finally:
            db.close()

    def read_all_excel_sheets_from_path(self, file_path: str):
        """Чтение всех страниц Excel файла по указанному пути"""
        try:
            all_sheets = pd.read_excel(
                io=file_path,
                sheet_name=None
            )
            return all_sheets
        except Exception as e:
            logger.error(f" Ошибка при чтении Excel файла {file_path}: {e}")
            raise

    # Остальные методы остаются без изменений
    def auto_process_dates(self, db, table_name):
        """Автоматическая обработка дат после парсинга"""
        try:
            logger.info(" Авто-обработка дат...")
            
            result = db.execute(text("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = :table_name 
                AND column_name = 'date'
            """), {"table_name": table_name})
            
            column_exists = result.fetchone()
            
            if not column_exists:
                logger.info(" Добавляем колонку date...")
                db.execute(text(f"ALTER TABLE {table_name} ADD COLUMN date DATE"))
                db.commit()
                logger.info(" Добавлена колонка date")
            
            logger.info(" Преобразование дат из dof в date...")
            
            update_query = text(f"""
                UPDATE {table_name} 
                SET date = TO_DATE(dof, 'YYMMDD')
                WHERE dof IS NOT NULL 
                AND LENGTH(dof) = 6
                AND dof ~ '^[0-9]+$'
                AND date IS NULL
            """)
            
            result = db.execute(update_query)
            db.commit()
            
            updated_count = result.rowcount
            logger.info(f" Авто-обработка дат завершена. Обновлено: {updated_count} записей")
            
            return updated_count
            
        except Exception as e:
            logger.error(f" Ошибка авто-обработки дат: {e}")
            try:
                db.rollback()
            except:
                pass
            return 0

    def auto_process_regions(self, db, table_name):
        """Автоматическая обработка регионов после парсинга"""
        try:
            from region_service import region_service
            
            # Принудительно загружаем shapefile если он еще не загружен
            if not hasattr(region_service, 'gdf') or region_service.gdf is None:
                logger.info(" Загрузка shapefile...")
                region_service.ensure_shapefile_loaded()
                
            if not hasattr(region_service, 'gdf') or region_service.gdf is None or region_service.gdf.empty:
                logger.warning(" Shapefile не загружен - пропускаем обработку регионов")
                return 0
                
            logger.info(f" Shapefile загружен: {len(region_service.gdf)} регионов")
            logger.info(" Авто-обработка регионов...")
            
            result = db.execute(text("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = :table_name 
                AND column_name = 'region_calculated'
            """), {"table_name": table_name})
            
            column_exists = result.fetchone()
            
            if not column_exists:
                db.execute(text(f"ALTER TABLE {table_name} ADD COLUMN region_calculated VARCHAR(200)"))
                db.commit()
                logger.info(" Добавлена колонка region_calculated")
            
            updated_count = region_service.add_region_to_flight_data(db, table_name=table_name, batch_size=10000)
            logger.info(f" Авто-обработка регионов завершена. Обновлено: {updated_count} записей")
            return updated_count
            
        except Exception as e:
            logger.error(f" Ошибка авто-обработки регионов: {e}")
            return 0

    def read_all_excel_sheets(self):
        """Чтение всех страниц Excel файла (для обратной совместимости)"""
        return self.read_all_excel_sheets_from_path(self.excel_file_path)

    def get_sheet_names(self):
        """Получить список всех страниц в Excel файле"""
        try:
            excel_file = pd.ExcelFile(self.excel_file_path)
            return excel_file.sheet_names
        except Exception as e:
            raise Exception(f" Ошибка при получении списка страниц: {e}")
    
    def read_excel_sheet(self, sheet_name):
        """Чтение конкретной страницы Excel файла"""
        try:
            return pd.read_excel(
                io=self.excel_file_path,
                sheet_name=sheet_name
            )
        except Exception as e:
            raise Exception(f" Ошибка при чтении страницы '{sheet_name}': {e}")
    
    def get_sheet_columns_info(self, sheet_name):
        """Получить информацию о колонках конкретной страницы"""
        try:
            df = pd.read_excel(
                io=self.excel_file_path,
                sheet_name=sheet_name,
                nrows=1
            )
            return {
                'columns': list(df.columns),
                'dtypes': {col: str(df[col].dtype) for col in df.columns}
            }
        except Exception as e:
            raise Exception(f" Ошибка при получении информации о колонках страницы '{sheet_name}': {e}")
    
    def get_all_sheets_info(self):
        """Получить информацию о всех страницах и их колонках"""
        try:
            sheets_info = {}
            sheet_names = self.get_sheet_names()
            
            for sheet_name in sheet_names:
                sheets_info[sheet_name] = self.get_sheet_columns_info(sheet_name)
            
            return sheets_info
        except Exception as e:
            raise Exception(f" Ошибка при получении информации о всех страницах: {e}")
    
    def read_excel(self, sheet_name=None):
        """Чтение Excel файла (обратная совместимость)"""
        if sheet_name:
            return self.read_excel_sheet(sheet_name)
        else:
            sheet_names = self.get_sheet_names()
            if sheet_names:
                return self.read_excel_sheet(sheet_names[0])
            else:
                raise Exception(" В Excel файле нет страниц")
    
    def get_columns_info(self, sheet_name=None):
        """Получить информацию о колонках (обратная совместимость)"""
        if sheet_name:
            return self.get_sheet_columns_info(sheet_name)
        else:
            sheet_names = self.get_sheet_names()
            if sheet_names:
                return self.get_sheet_columns_info(sheet_names[0])
            else:
                raise Exception(" В Excel файле нет страниц")
            
                
    def append_to_existing_table(self, file_path: str) -> dict:
        """
        Дозагрузка данных в текущую активную таблицу
        """
        try:
            if not os.path.exists(file_path):
                return {"error": "Файл не найден"}
            
            logger.info(f"🔄 Дозагрузка данных из файла: {file_path}")
            
            from data_processor import DataProcessor
            from database import SessionLocal
            
            db = SessionLocal()
            try:
                #  УПРОЩЕННАЯ ЛОГИКА ПОИСКА ТАБЛИЦЫ
                target_table = None
                
                # Способ 1: Ищем активную таблицу через user_tables
                active_table_result = db.execute(text("""
                    SELECT table_name 
                    FROM user_tables 
                    WHERE is_active = true
                    LIMIT 1
                """))
                active_table = active_table_result.scalar()
                
                if active_table:
                    target_table = active_table
                    logger.info(f" Найдена активная таблица: {target_table}")
                else:
                    # Способ 2: Берем последнюю таблицу с префиксом excel_data_
                    logger.info(" Активная таблица не найдена, ищем последнюю таблицу...")
                    result = db.execute(text("""
                        SELECT table_name 
                        FROM information_schema.tables 
                        WHERE table_schema = 'public' 
                        AND table_name LIKE 'excel_data_%'
                        ORDER BY table_name DESC
                        LIMIT 1
                    """))
                    target_table = result.scalar()
                    
                    if target_table:
                        logger.info(f" Найдена последняя таблица: {target_table}")
                    else:
                        return {"error": "Не найдено ни одной таблицы для дозагрузки"}
                
                # Проверяем существование целевой таблицы
                table_exists = db.execute(text("""
                    SELECT EXISTS (
                        SELECT 1 FROM information_schema.tables 
                        WHERE table_schema = 'public' AND table_name = :table_name
                    )
                """), {"table_name": target_table}).scalar()
                
                if not table_exists:
                    return {"error": f"Целевая таблица {target_table} не существует"}
                
                #  ПОЛУЧАЕМ СЛЕДУЮЩИЙ ДОСТУПНЫЙ ID
                max_id_result = db.execute(text(f'SELECT MAX(id) FROM "{target_table}"'))
                max_id = max_id_result.scalar() or 0
                next_id = max_id + 1
                logger.info(f"🎯 Следующий доступный ID: {next_id}")
                
                # Получаем текущую структуру таблицы
                columns_result = db.execute(text("""
                    SELECT column_name, data_type 
                    FROM information_schema.columns 
                    WHERE table_name = :table_name 
                    ORDER BY ordinal_position
                """), {"table_name": target_table})
                
                existing_columns = {row[0]: row[1] for row in columns_result}
                logger.info(f" Структура таблицы {target_table}: {list(existing_columns.keys())}")
                
                logger.info(" Чтение данных из Excel для дозагрузки...")
                all_sheets = self.read_all_excel_sheets_from_path(file_path)
                logger.info(f" Прочитано листов: {len(all_sheets)}")
                
                data_processor = DataProcessor(db_session=db)
                total_added = 0
                sheets_processed = []
                
                for sheet_name, df in all_sheets.items():
                    logger.info(f" Обработка листа для дозагрузки: {sheet_name}, строк: {len(df)}")
                    
                    # Очищаем и обрабатываем данные
                    df_cleaned = DataProcessor.clean_dataframe(df)
                    
                    if not df_cleaned.empty:
                        df_decoded = data_processor.decode_flight_plan_fields(df_cleaned)
                        
                        #  ОПТИМИЗАЦИЯ: Приводим структуру к целевой таблице
                        df_aligned = self._align_dataframe_structure(df_decoded, existing_columns)
                        
                        #  ГЕНЕРИРУЕМ НОВЫЕ УНИКАЛЬНЫЕ ID
                        if 'id' in df_aligned.columns:
                            # Сбрасываем существующие ID и генерируем новые
                            df_aligned = df_aligned.reset_index(drop=True)
                            df_aligned['id'] = range(next_id, next_id + len(df_aligned))
                            next_id += len(df_aligned)
                            logger.info(f"🆕 Сгенерированы ID с {df_aligned['id'].min()} по {df_aligned['id'].max()}")
                        
                        # Сохраняем с добавлением (append)
                        result = data_processor.append_to_table(df_aligned, target_table)
                        
                        sheet_added = result.get("added", 0)
                        total_added += sheet_added
                        sheets_processed.append({
                            "sheet_name": sheet_name,
                            "records": sheet_added,
                            "original_rows": len(df)
                        })
                        
                        logger.info(f" Лист '{sheet_name}': добавлено {sheet_added} записей")
                    else:
                        logger.warning(f" Лист '{sheet_name}' пуст после очистки")
                
                # Обработка дат и регионов для новых данных
                if total_added > 0:
                    processing_result = self.process_new_records_dates_and_regions(db, target_table)
                    
                    return {
                        "success": True,
                        "message": f"Дозагружено {len(sheets_processed)} листов в {target_table}",
                        "table_name": target_table,
                        "records_added": total_added,
                        "sheets_processed": sheets_processed,
                        "processing_result": processing_result
                    }
                else:
                    return {
                        "warning": "Нет новых данных для добавления",
                        "sheets_processed": sheets_processed
                    }
                
            except Exception as e:
                logger.error(f" Ошибка дозагрузки: {e}")
                return {"error": f"Ошибка дозагрузки: {str(e)}"}
            finally:
                db.close()
                
        except Exception as e:
            logger.error(f" Ошибка при дозагрузке файла: {e}")
            return {"error": f"Ошибка при дозагрузке файла: {str(e)}"}
    
    def _find_target_table_for_append(self, db) -> str:
        """
        Автоматически находит таблицу для дозагрузки
        Стратегия: ищем таблицу с самым большим количеством записей или текущую активную
        """
        try:
            # Сначала пытаемся найти текущую активную таблицу через TableService
            try:
                from table_service import TableService
                table_service = TableService(db)
                
                # Получаем все таблицы и ищем активную
                tables = table_service.get_all_tables()
                for table in tables:
                    if getattr(table, 'is_active', False):
                        logger.info(f" Найдена активная таблица: {table.table_name}")
                        return table.table_name
            except Exception as e:
                logger.warning(f" Не удалось получить активную таблицу через TableService: {e}")
            
            # Если активной нет, ищем таблицу с наибольшим количеством записей
            result = db.execute(text("""
                SELECT table_name
                FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name LIKE 'excel_data_%'
                ORDER BY table_name DESC
                LIMIT 1
            """))
            
            latest_table = result.scalar()
            
            if latest_table:
                logger.info(f" Найдена последняя таблица: {latest_table}")
                return latest_table
            
            # Если нет таблиц с префиксом excel_data_, ищем любую таблицу с данными
            result = db.execute(text("""
                SELECT table_name
                FROM information_schema.tables 
                WHERE table_schema = 'public'
                AND table_name NOT LIKE 'pg_%'
                AND table_name NOT LIKE 'sql_%'
                AND table_name NOT IN ('user_tables', 'user_sessions')
                ORDER BY table_name DESC
                LIMIT 1
            """))
            
            fallback_table = result.scalar()
            
            if fallback_table:
                logger.info(f" Используем таблицу: {fallback_table}")
                return fallback_table
            
            logger.error(" Не найдено подходящих таблиц для дозагрузки")
            return None
            
        except Exception as e:
            logger.error(f" Ошибка при поиске таблицы для дозагрузки: {e}")
            return None

    def _align_dataframe_structure(self, df: pd.DataFrame, target_columns: dict) -> pd.DataFrame:
        """
        Приводит структуру DataFrame к структуре целевой таблицы
        С правильным сопоставлением колонок!
        """
        result_df = pd.DataFrame()
        
        #  СОПОСТАВЛЕНИЕ КОЛОНОК: старые имена -> новые имена
        column_mapping = {
            'DOF': 'dof',
            'DEP': 'dep', 
            'DEST': 'dest',
            'REG': 'reg',
            'OPR': 'opr',
            'TYP': 'typ',
            'STS': 'sts',
            'RMK': 'rmk',
            'EET': 'eet',
            'ORGN': 'tsentr_es_orvd',
            'PER': 'shr',
            'DLE': 'arr'
        }
        
        for col_name, col_type in target_columns.items():
            # Пытаемся найти колонку в исходных данных
            source_col = None
            
            # 1. Прямое совпадение
            if col_name in df.columns:
                source_col = col_name
            # 2. Сопоставление через mapping
            elif col_name in column_mapping.values():
                # Находим ключ по значению
                for source_name, target_name in column_mapping.items():
                    if target_name == col_name and source_name in df.columns:
                        source_col = source_name
                        break
            # 3. Обратное сопоставление (из mapping)
            elif col_name.upper() in column_mapping:
                mapped_name = column_mapping[col_name.upper()]
                if mapped_name in df.columns:
                    source_col = mapped_name
            
            if source_col and source_col in df.columns:
                result_df[col_name] = df[source_col]
                logger.info(f" Сопоставлено: {source_col} -> {col_name}")
            else:
                # Добавляем недостающие колонки с NULL значениями
                result_df[col_name] = None
                logger.debug(f" Колонка {col_name} не найдена в исходных данных")
        
        # Логируем результат
        logger.info(f" Результат сопоставления: {len(result_df)} строк, {len(result_df.columns)} колонок")
        logger.info(f" Колонки результата: {list(result_df.columns)}")
        
        return result_df

    def process_new_records_dates_and_regions(self, db, table_name: str) -> dict:
        """Обработка дат и регионов только для новых записей"""
        try:
            result = {
                "dates_processed": 0,
                "regions_processed": 0
            }
            
            # Обработка дат только для записей без даты
            dates_updated = self._process_new_dates(db, table_name)
            result["dates_processed"] = dates_updated
            
            # Обработка регионов только для записей без региона
            regions_updated = self._process_new_regions(db, table_name)
            result["regions_processed"] = regions_updated
            
            logger.info(f" Обработка новых записей: {dates_updated} дат, {regions_updated} регионов")
            return result
            
        except Exception as e:
            logger.error(f" Ошибка обработки новых записей: {e}")
            return {"error": str(e)}

    def _process_new_dates(self, db, table_name: str) -> int:
        """Обработка дат только для новых записей"""
        try:
            # Проверяем наличие колонки date
            result = db.execute(text("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = :table_name 
                AND column_name = 'date'
            """), {"table_name": table_name})
            
            if not result.fetchone():
                return 0
            
            #  ИСПРАВЛЕНИЕ: Обрабатываем записи где dof есть, но date пустое
            update_query = text(f"""
                UPDATE "{table_name}" 
                SET date = TO_DATE(dof, 'YYMMDD')
                WHERE dof IS NOT NULL 
                AND LENGTH(TRIM(dof)) = 6
                AND dof ~ '^[0-9]+$'
                AND date IS NULL
            """)
            
            result = db.execute(update_query)
            db.commit()
            
            updated_count = result.rowcount
            logger.info(f" Обработано дат для новых записей: {updated_count}")
            return updated_count
            
        except Exception as e:
            db.rollback()
            logger.error(f" Ошибка обработки новых дат: {e}")
            return 0
    
    def _process_new_regions(self, db, table_name: str) -> int:
        """Обработка регионов только для новых записей"""
        try:
            from region_service import region_service
            
            # Проверяем наличие колонки region_calculated
            result = db.execute(text("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = :table_name 
                AND column_name = 'region_calculated'
            """), {"table_name": table_name})
            
            if not result.fetchone():
                return 0
            
            #  ИСПРАВЛЕНИЕ: Обрабатываем записи где region_calculated пустое
            # Получаем записи без региона
            records_without_region = db.execute(text(f"""
                SELECT COUNT(*) FROM "{table_name}" 
                WHERE region_calculated IS NULL 
                AND (dep IS NOT NULL OR dest IS NOT NULL)
            """)).scalar()
            
            if records_without_region > 0:
                logger.info(f" Найдено {records_without_region} записей без региона")
                updated_count = region_service.add_region_to_flight_data(db, table_name=table_name, batch_size=10000)
                return updated_count
            else:
                logger.info(" Все записи уже имеют регионы")
                return 0
                
        except Exception as e:
            logger.error(f" Ошибка обработки новых регионов: {e}")
            return 0
