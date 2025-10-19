
from sqlalchemy.orm import Session
from sqlalchemy import text
from models import UserTable, UserSession
from schemas import TableInfoResponse
import uuid
from datetime import datetime
from typing import List, Dict, Optional
from sqlalchemy.exc import SQLAlchemyError
import logging
logger = logging.getLogger(__name__)
class TableService:
    def __init__(self, db: Session):
        self.db = db
    
    # В классе TableService заменяем метод get_all_tables:

# В классе TableService заменяем метод get_all_tables:

    def get_all_tables(self) -> List[TableInfoResponse]:
        """Получить только таблицы с данными Excel с АКТУАЛЬНЫМИ counts"""
        try:
            # 🔥 Принудительно обновляем статистику всех таблиц
            self.db.execute(text("ANALYZE;"))
            self.db.commit()
            
            tables = self.db.execute(text("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_type = 'BASE TABLE'
                ORDER BY table_name
            """))
            
            result = []
            for table in tables:
                table_name = table[0]

                if not table_name.startswith("excel_data_"):
                    continue

                # 🔥 ВСЕГДА получаем актуальное количество записей из таблицы
                try:
                    count_result = self.db.execute(text(f'SELECT COUNT(*) FROM "{table_name}"'))
                    record_count = count_result.scalar() or 0
                except Exception as e:
                    logger.warning(f"⚠️ Ошибка получения count для {table_name}: {e}")
                    record_count = 0

                if record_count == 0:
                    continue

                # Обновляем запись в user_tables если есть расхождение
                user_table = self.db.query(UserTable).filter(UserTable.table_name == table_name).first()
                if user_table:
                    if user_table.records_count != record_count:
                        user_table.records_count = record_count
                        self.db.commit()
                        logger.info(f"🔄 Авто-обновление records_count для {table_name}: {record_count}")
                    
                    is_active = user_table.is_active
                    original_filename = user_table.original_filename
                    upload_date = user_table.upload_date.isoformat() if user_table.upload_date else None
                    description = user_table.description
                else:
                    is_active = False
                    original_filename = table_name
                    upload_date = None
                    description = None

                result.append(TableInfoResponse(
                    table_name=table_name,
                    original_filename=original_filename,
                    upload_date=upload_date,
                    records_count=record_count,  # 🔥 Всегда актуальное значение
                    description=description,
                    is_active=is_active
                ))

            return result

        except Exception as e:
            logger.error(f"❌ Критическая ошибка получения таблиц: {e}")
            return []


    def set_current_table(self, session_id: str, table_name: str) -> bool:
        """Установить текущую таблицу для сессии"""
        try:
            # Проверяем существование таблицы
            table_exists = self.db.execute(text("""
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.tables 
                    WHERE table_schema = 'public' AND table_name = :table_name
                )
            """), {"table_name": table_name}).scalar()
            
            if not table_exists:
                return False
            
            # Обновляем или создаем сессию
            session = self.db.query(UserSession).filter(UserSession.session_id == session_id).first()
            if session:
                session.current_table = table_name
                session.updated_at = datetime.utcnow()
            else:
                session = UserSession(session_id=session_id, current_table=table_name)
                self.db.add(session)
            
            # Сбрасываем флаги активной таблицы
            self.db.query(UserTable).update({"is_active": False})
            
            # Устанавливаем новую активную таблицу
            user_table = self.db.query(UserTable).filter(UserTable.table_name == table_name).first()
            if user_table:
                user_table.is_active = True
            
            self.db.commit()
            return True
            
        except Exception as e:
            self.db.rollback()
            print(f"Ошибка установки таблицы: {e}")
            return False
    
    def get_current_table(self, session_id: str) -> Optional[str]:
        """Получить текущую таблицу для сессии"""
        try:
            session = self.db.query(UserSession).filter(UserSession.session_id == session_id).first()
            if session and session.current_table:
                # Проверяем что таблица еще существует
                table_exists = self.db.execute(text("""
                    SELECT EXISTS (
                        SELECT 1 FROM information_schema.tables 
                        WHERE table_schema = 'public' AND table_name = :table_name
                    )
                """), {"table_name": session.current_table}).scalar()
                
                if table_exists:
                    return session.current_table
            
            # Если нет текущей таблицы, берем первую доступную
            tables = self.get_all_tables()
            if tables:
                return tables[0].table_name
            
            return None
            
        except Exception as e:
            print(f"Ошибка получения текущей таблицы: {e}")
            return None
    
    def register_table(self, table_name: str, original_filename: str, description: str = None) -> bool:
        """Зарегистрировать новую таблицу"""
        try:
            count_result = self.db.execute(text(f'SELECT COUNT(*) FROM "{table_name}"'))
            record_count = count_result.scalar() or 0

            user_table = UserTable(
                table_name=table_name,
                original_filename=original_filename,
                description=description,
                records_count=record_count,
                upload_date=datetime.utcnow()
            )
            self.db.add(user_table)
            self.db.commit()
            return True

        except SQLAlchemyError as e:
            print(f"Ошибка регистрации таблицы: {e}")
            # Безопасно закрываем и пересоздаём сессию
            try:
                self.db.rollback()
            except Exception:
                pass
            self.db.close()
            from database import SessionLocal  # где у тебя создаётся sessionmaker
            self.db = SessionLocal()
            return False
    
    def delete_table(self, table_name: str) -> bool:
        """Удаляет таблицу из базы данных и запись о ней из user_tables"""
        try:
            print(f"🧹 Удаление таблицы '{table_name}'...")

            # Проверяем существование таблицы
            exists = self.db.execute(text("""
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.tables
                    WHERE table_schema = 'public'
                    AND table_name = :table_name
                )
            """), {"table_name": table_name}).scalar()

            if not exists:
                print(f"⚠️ Таблица '{table_name}' не найдена")
                return False

            # Удаляем таблицу
            print(f"   📛 DROP TABLE {table_name}")
            self.db.execute(text(f'DROP TABLE IF EXISTS "{table_name}" CASCADE;'))

            # Удаляем запись из user_tables
            print("   🗑 Удаляем запись из user_tables...")
            deleted = self.db.query(UserTable).filter(UserTable.table_name == table_name).delete()
            print(f"   ✅ Удалено записей из user_tables: {deleted}")

            self.db.commit()
            print(f"✅ Таблица '{table_name}' успешно удалена")
            return True

        except Exception as e:
            self.db.rollback()
            import traceback
            print("❌ Ошибка при удалении таблицы:")
            traceback.print_exc()
            return False
        
        
    def generate_table_name(self, custom_name: str) -> str:
        """Генерирует безопасное имя таблицы из пользовательского названия"""
        import re
        import time
        
        # Очищаем название: убираем спецсимволы, заменяем пробелы на подчеркивания
        safe_name = re.sub(r'[^a-zA-Z0-9а-яА-Я_]', '_', custom_name)
        safe_name = re.sub(r'_+', '_', safe_name).strip('_')
        
        # Если имя пустое, используем timestamp
        if not safe_name:
            safe_name = f"table_{int(time.time())}"
        else:
            # Добавляем timestamp для уникальности
            safe_name = f"{safe_name}_{int(time.time())}"
        
        # Ограничиваем длину (PostgreSQL ограничение - 63 символа)
        safe_name = safe_name[:50]
        
        return safe_name.lower()

    def table_name_exists(self, table_name: str) -> bool:
        """Проверяет, существует ли таблица с таким именем"""
        try:
            exists = self.db.execute(text("""
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.tables 
                    WHERE table_schema = 'public' AND table_name = :table_name
                )
            """), {"table_name": table_name}).scalar()
            return exists
        except Exception:
            return False

    def get_table_by_custom_name(self, custom_name: str) -> Optional[UserTable]:
        """Находит таблицу по пользовательскому названию"""
        return self.db.query(UserTable).filter(
            UserTable.original_filename == custom_name
        ).first()

    def update_table_records_count(self, table_name: str) -> bool:
        """Обновляет количество записей в user_tables для указанной таблицы"""
        try:
            # Получаем актуальное количество записей из таблицы
            count_result = self.db.execute(text(f'SELECT COUNT(*) FROM "{table_name}"'))
            actual_count = count_result.scalar() or 0
            
            # Обновляем запись в user_tables
            user_table = self.db.query(UserTable).filter(UserTable.table_name == table_name).first()
            if user_table:
                user_table.records_count = actual_count
                self.db.commit()
                logger.info(f"📊 Обновлен records_count для {table_name}: {actual_count} записей")
                return True
            else:
                logger.warning(f"⚠️ Таблица {table_name} не найдена в user_tables")
                return False
                
        except Exception as e:
            self.db.rollback()
            logger.error(f"❌ Ошибка обновления records_count для {table_name}: {e}")
            return False
    

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
                # 🔥 УПРОЩЕННАЯ ЛОГИКА ПОИСКА ТАБЛИЦЫ
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
                    logger.info(f"🎯 Найдена активная таблица: {target_table}")
                else:
                    # Способ 2: Берем последнюю таблицу с префиксом excel_data_
                    logger.info("🔍 Активная таблица не найдена, ищем последнюю таблицу...")
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
                        logger.info(f"📊 Найдена последняя таблица: {target_table}")
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
                
                # 🔥 ПОЛУЧАЕМ СЛЕДУЮЩИЙ ДОСТУПНЫЙ ID
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
                logger.info(f"📊 Структура таблицы {target_table}: {list(existing_columns.keys())}")
                
                logger.info("📊 Чтение данных из Excel для дозагрузки...")
                all_sheets = self.read_all_excel_sheets_from_path(file_path)
                logger.info(f"📄 Прочитано листов: {len(all_sheets)}")
                
                data_processor = DataProcessor(db_session=db)
                total_added = 0
                sheets_processed = []
                
                for sheet_name, df in all_sheets.items():
                    logger.info(f"🔧 Обработка листа для дозагрузки: {sheet_name}, строк: {len(df)}")
                    
                    # Очищаем и обрабатываем данные
                    df_cleaned = DataProcessor.clean_dataframe(df)
                    
                    if not df_cleaned.empty:
                        df_decoded = data_processor.decode_flight_plan_fields(df_cleaned)
                        
                        # 🔥 ОПТИМИЗАЦИЯ: Приводим структуру к целевой таблице
                        df_aligned = self._align_dataframe_structure(df_decoded, existing_columns)
                        
                        # 🔥 ГЕНЕРИРУЕМ НОВЫЕ УНИКАЛЬНЫЕ ID
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
                        
                        logger.info(f"✅ Лист '{sheet_name}': добавлено {sheet_added} записей")
                    else:
                        logger.warning(f"⚠️ Лист '{sheet_name}' пуст после очистки")
                
                # Обработка дат и регионов для новых данных
                if total_added > 0:
                    processing_result = self.process_new_records_dates_and_regions(db, target_table)
                    
                    # 🔥 ОБНОВЛЯЕМ RECORDS_COUNT В USER_TABLES
                    try:
                        from table_service import TableService
                        table_service = TableService(db)
                        table_service.update_table_records_count(target_table)
                    except Exception as e:
                        logger.warning(f"⚠️ Не удалось обновить records_count: {e}")
                    
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
                logger.error(f"❌ Ошибка дозагрузки: {e}")
                return {"error": f"Ошибка дозагрузки: {str(e)}"}
            finally:
                db.close()
                
        except Exception as e:
            logger.error(f"❌ Ошибка при дозагрузке файла: {e}")
            return {"error": f"Ошибка при дозагрузке файла: {str(e)}"}
