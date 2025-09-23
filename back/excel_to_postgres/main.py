
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
    main_standard()

def main_standard():
    """Стандартная функция для парсинга и загрузки всех страниц Excel в PostgreSQL"""
    
    # Параметры из .env
    base_table_name = os.getenv('TABLE_NAME', 'excel_data')
    merge_sheets = os.getenv('MERGE_SHEETS', 'false').lower() == 'true'
    
    try:
        # Инициализация классов
        excel_parser = ExcelParser()
        data_processor = DataProcessor()
        postgres_loader = PostgresLoader()
        
        print("=" * 60)
        print("СТАНДАРТНАЯ ОБРАБОТКА EXCEL В POSTGRESQL")
        print("=" * 60)
        
        # Получаем список всех страниц
        print("\n1. Получение списка страниц Excel...")
        sheet_names = excel_parser.get_sheet_names()
        print(f"Найдено страниц: {len(sheet_names)}")
        print(f"Названия страниц: {sheet_names}")
        
        # Получаем информацию о всех страницах
        print("\n2. Анализ структуры всех страниц...")
        all_sheets_info = excel_parser.get_all_sheets_info()
        for sheet_name, info in all_sheets_info.items():
            print(f"  Страница '{sheet_name}': {len(info['columns'])} колонок")
            # Показываем как будет называться таблица
            clean_sheet_name = data_processor.clean_sheet_name_for_table(sheet_name)
            table_name_preview = f"{base_table_name}_{clean_sheet_name}"
            print(f"    -> Таблица будет называться: '{table_name_preview}'")
        
        # Читаем все страницы
        print("\n3. Чтение всех страниц...")
        all_sheets_data = excel_parser.read_all_excel_sheets()
        
        if merge_sheets:
            # Вариант 1: Объединяем все страницы в одну таблицу
            print("\n4. Объединение всех страниц в одну таблицу...")
            combined_df_list = []
            
            for sheet_name, df in all_sheets_data.items():
                print(f"  Обработка страницы '{sheet_name}': {df.shape[0]} строк, {df.shape[1]} колонок")
                
                # Добавляем колонку с названием страницы для идентификации
                df['source_sheet'] = sheet_name
                
                # Очищаем DataFrame
                df_cleaned = data_processor.clean_dataframe(df)
                df_decoded = data_processor.decode_flight_plan_fields(df_cleaned)
                combined_df_list.append(df_decoded)
            
            # Объединяем все DataFrame
            if combined_df_list:
                # Используем concat с ignore_index=True для сброса индексов
                combined_df = pd.concat(combined_df_list, ignore_index=True, sort=False)
                
                print(f"\nОбъединенная таблица: {combined_df.shape[0]} строк, {combined_df.shape[1]} колонок")
                
                # Загружаем в одну таблицу
                table_name = base_table_name
                print(f"\n5. Загрузка объединенных данных в таблицу '{table_name}'...")
                postgres_loader.load_data(combined_df, table_name, if_exists='replace')
                
                # Проверка загрузки
                print("\n6. Проверка загрузки данных...")
                count = pd.read_sql_query(f"SELECT COUNT(*) as count FROM {table_name}", postgres_loader.engine)['count'][0]
                print(f"Загружено строк: {count}")
        else:
            # Вариант 2: Создаем отдельную таблицу для каждой страницы
            print("\n4. Создание отдельных таблиц для каждой страницы...")
            
            successful_loads = 0
            total_rows = 0
            
            for sheet_name, df in all_sheets_data.items():
                print(f"\n  Обработка страницы '{sheet_name}':")
                print(f"    Исходные данные: {df.shape[0]} строк, {df.shape[1]} колонок")
                
                # Очищаем DataFrame
                df_cleaned = data_processor.clean_dataframe(df)
                df_decoded = data_processor.decode_flight_plan_fields(df_cleaned)
                
                # Проверяем, есть ли данные после очистки
                if df_cleaned.empty:
                    print(f"    Предупреждение: Страница '{sheet_name}' пустая после очистки, пропускаем")
                    continue
                
                # Генерируем имя таблицы
                clean_sheet_name = data_processor.clean_sheet_name_for_table(sheet_name)
                table_name = f"{base_table_name}_{clean_sheet_name}"
                
                try:
                    # Загружаем данные
                    postgres_loader.load_data(df_decoded, table_name, if_exists='replace')
                    successful_loads += 1
                    total_rows += len(df_cleaned)
                    print(f"    Успешно загружено в таблицу '{table_name}': {len(df_cleaned)} строк")
                    
                except Exception as e:
                    print(f"    Ошибка загрузки страницы '{sheet_name}': {e}")
            
            # Общая статистика
            print(f"\n5. Общая статистика:")
            print(f"   Успешно обработано страниц: {successful_loads} из {len(all_sheets_data)}")
            print(f"   Общее количество загруженных строк: {total_rows}")
            
            # Проверка загрузки для каждой таблицы
            if successful_loads > 0:
                print(f"\n6. Проверка загрузки данных по таблицам:")
                
                for sheet_name, df in all_sheets_data.items():
                    if not df.empty:
                        clean_sheet_name = data_processor.clean_sheet_name_for_table(sheet_name)
                        table_name = f"{base_table_name}_{clean_sheet_name}"
                        try:
                            count = pd.read_sql_query(f"SELECT COUNT(*) as count FROM {table_name}", postgres_loader.engine)['count'][0]
                            print(f"   Таблица '{table_name}': {count} строк")
                        except Exception as e:
                            print(f"   Таблица '{table_name}': ошибка проверки - {e}")
        
        print("\n" + "=" * 60)
        print("ПАРСИНГ И ЗАГРУЗКА ВСЕХ СТРАНИЦ УСПЕШНО ЗАВЕРШЕНЫ!")
        print("=" * 60)
        
        # Показываем подсказку о возможности использования шаблонов
        print("\nСОВЕТ:")
        print("   Для обработки авиационных данных можно использовать специальные шаблоны.")
        print("   Добавьте в .env файл: USE_AVIATION_TEMPLATES=true")
        print("   Это позволит структурировать данные согласно авиационным стандартам.")
        
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
    print("      - Прямая загрузка данных из Excel в PostgreSQL")
    print("      - Автоматическая очистка и нормализация названий колонок")
    print("      - Создание отдельной таблицы для каждого листа")
    print("")
    print("   2. АВИАЦИОННЫЕ ШАБЛОНЫ:")
    print("      - Структурирование данных согласно авиационным стандартам")
    print("      - Автоматическое определение типа сообщения (FPL, DEP, ARR, и др.)")
    print("      - Валидация данных по авиационным требованиям")
    print("      - Генерация подробных отчетов о процессинге")
    
    print("\nНАСТРОЙКИ (.env файл):")
    print("   DB_* - настройки подключения к PostgreSQL")
    print("   EXCEL_FILE_PATH - путь к Excel файлу")
    print("   TABLE_NAME - базовое имя таблиц")
    print("   USE_AVIATION_TEMPLATES - включить авиационные шаблоны")
    print("   AUTO_DETECT_MESSAGE_TYPE - автоопределение типа сообщения")
    print("   GENERATE_REPORTS - генерация отчетов")
    
    print("\nЗАПУСК:")
    print("   python main.py - автоматический выбор режима")
    print("   python templates/template_integration.py - принудительно с шаблонами")
    
    # Проверяем текущие настройки
    load_dotenv()
    use_templates = os.getenv('USE_AVIATION_TEMPLATES', 'false').lower() == 'true'
    print(f"\nТЕКУЩИЙ РЕЖИМ: {'Авиационные шаблоны' if use_templates else 'Стандартная обработка'}")
    
    print("=" * 70)


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == '--info':
        show_system_info()
    else:
        main()