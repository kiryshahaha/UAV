import os
import pandas as pd
from dotenv import load_dotenv
from config.database import DatabaseConfig
from parsers.excel_parser import ExcelParser
from parsers.data_processor import DataProcessor
from loaders.postgres_loader import PostgresLoader

def main():
    """Основная функция с автоматическим выбором режима обработки"""
    # Загрузка переменных окружения
    load_dotenv()
    
    # Получаем строку подключения к базе данных
    db_config = DatabaseConfig()
    db_connection_string = db_config.get_connection_string()
    
    # Проверяем, включены ли авиационные шаблоны
    use_aviation_templates = os.getenv('USE_AVIATION_TEMPLATES', 'false').lower() == 'true'

    if use_aviation_templates:
        print("Обнаружены настройки авиационных шаблонов - переключение на расширенную обработку...")
        try:
            from templates.template_integration import main_with_templates
            main_with_templates()
            return
        except ImportError as e:
            print(f"Ошибка импорта модуля шаблонов: {e}")
            print("Переключение на стандартную обработку...")
        except Exception as e:
            print(f"Ошибка в обработке с шаблонами: {e}")
            print("Переключение на стандартную обработку...")

    # Стандартная обработка
    main_standard(db_connection_string)

def main_standard(db_connection_string):
    """Стандартная функция для парсинга и загрузки всех страниц Excel в таблицу excel_data_result_1"""
    # Используем фиксированное имя таблицы
    table_name = "excel_data_result_1"

    try:
        # Инициализация классов с передачей строки подключения
        excel_parser = ExcelParser()
        data_processor = DataProcessor(db_connection_string)
        postgres_loader = PostgresLoader()

        print("=" * 60)
        print("СТАНДАРТНАЯ ОБРАБОТКА EXCEL В POSTGRESQL")
        print("=" * 60)
        print(f"РЕЖИМ: Загрузка в таблицу {table_name} с уникальными ID")

        # Получаем список всех страниц
        print("\n1. Получение списка страниц Excel...")
        sheet_names = excel_parser.get_sheet_names()
        print(f"Найдено страниц: {len(sheet_names)}")
        print(f"Названия страниц: {sheet_names}")

        # Читаем все страницы
        print("\n3. Чтение всех страниц...")
        all_sheets_data = excel_parser.read_all_excel_sheets()

        # Объединяем все страницы в одну таблицу
        print("\n4. Объединение всех страниц в одну таблицу...")
        combined_df_list = []

        for sheet_name, df in all_sheets_data.items():
            if df.empty:
                print(f"  Страница '{sheet_name}': ПУСТАЯ - пропускаем")
                continue
                
            print(f"  Обработка страницы '{sheet_name}': {df.shape[0]} строк, {df.shape[1]} колонок")

            # Добавляем колонку с названием страницы для идентификации
            df['source_sheet'] = sheet_name

            # Очищаем DataFrame
            df_cleaned = data_processor.clean_dataframe(df)
            df_decoded = data_processor.decode_flight_plan_fields(df_cleaned)
            
            if not df_decoded.empty:
                combined_df_list.append(df_decoded)
                print(f"    Добавлено: {df_decoded.shape[0]} строк")
            else:
                print(f"    Предупреждение: Страница '{sheet_name}' пустая после обработки")

        # Объединяем все DataFrame
        if combined_df_list:
            combined_df = pd.concat(combined_df_list, ignore_index=True, sort=False)
            print(f"\nОбъединенная таблица: {combined_df.shape[0]} строк, {combined_df.shape[1]} колонок")

            # Загружаем в таблицу excel_data_result_1 с уникальными ID
            print(f"\n5. Загрузка данных в таблицу '{table_name}' с уникальными ID...")
            try:
                result = data_processor.save_to_table_with_id(combined_df, table_name)

                print(f"\nРЕЗУЛЬТАТ ЗАГРУЗКИ:")
                print(f"Успешно загружено строк: {result['added']}")
                print(f"Всего обработано: {result['total']}")

                # Проверка загрузки
                print("\n6. Проверка загрузки данных...")
                count = pd.read_sql_query(f"SELECT COUNT(*) as count FROM {table_name}", postgres_loader.engine)['count'][0]
                print(f"Всего строк в таблице: {count}")

                # Проверяем наличие ID
                has_id = pd.read_sql_query(f"""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_name = '{table_name}' AND column_name = 'id'
                """, postgres_loader.engine)
                
                if not has_id.empty:
                    print("Уникальный ID: ДОБАВЛЕН ✓")
                else:
                    print("Уникальный ID: ОТСУТСТВУЕТ ✗")

            except Exception as e:
                print(f"\nОШИБКА ПРИ СОХРАНЕНИИ: {e}")
                import traceback
                traceback.print_exc()

        else:
            print("Нет данных для загрузки")
            
        print("\n" + "=" * 60)
        print("ПАРСИНГ И ЗАГРУЗКА ДАННЫХ ЗАВЕРШЕНЫ!")
        print("=" * 60)

    except Exception as e:
        print(f"\nОШИБКА: {e}")
        import traceback
        traceback.print_exc()

def show_system_info():
    """Показать информацию о системе и доступных возможностях"""
    print("=" * 70)
    print("СИСТЕМА ОБРАБОТКИ EXCEL ДАННЫХ")
    print("=" * 70)

    print("\nДОСТУПНЫЕ РЕЖИМЫ ОБРАБОТКИ:")
    print("   1. СТАНДАРТНЫЙ РЕЖИМ:")
    print("      - Загрузка данных из Excel в таблицу excel_data_result_1")
    print("      - Автоматическая очистка и нормализация данных")
    print("      - Добавление уникального ID для каждой записи")
    print("      - Объединение всех страниц в одну таблицу")
    print("")
    print("   2. АВИАЦИОННЫЕ ШАБЛОНЫ:")
    print("      - Структурирование данных согласно авиационным стандартам")
    print("      - Автоматическое определение типа сообщения (FPL, DEP, ARR, и др.)")
    print("      - Валидация данных по авиационным требованиям")

    print("\nЦЕЛЕВАЯ ТАБЛИЦА: excel_data_result_1")
    print("УНИКАЛЬНЫЕ ID: ВКЛЮЧЕНЫ")
    print("=" * 70)

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == '--info':
        show_system_info()
    else:
        main()