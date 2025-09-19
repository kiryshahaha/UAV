import os
from dotenv import load_dotenv
from config.database import DatabaseConfig
from parsers.excel_parser import ExcelParser
from parsers.data_processor import DataProcessor
from loaders.postgres_loader import PostgresLoader

def main():
    """Основная функция для запуска парсинга и загрузки"""
    
    # Загрузка переменных окружения
    load_dotenv()
    
    # Параметры из .env
    table_name = os.getenv('TABLE_NAME', 'excel_data')
    
    try:
        # Инициализация классов
        excel_parser = ExcelParser()
        data_processor = DataProcessor()
        postgres_loader = PostgresLoader()
        
        print("=" * 50)
        print("НАЧАЛО ПАРСИНГА EXCEL В POSTGRESQL")
        print("=" * 50)
        
        # Получаем информацию о колонках
        print("\n1. Получение информации о колонках Excel...")
        columns_info = excel_parser.get_columns_info()
        print(f"Колонки в Excel: {columns_info['columns']}")
        print(f"Типы данных: {columns_info['dtypes']}")
        
        # Чтение и обработка данных
        print("\n2. Чтение и обработка данных...")
        df = excel_parser.read_excel()
        df_cleaned = data_processor.clean_dataframe(df)
        
        # Загрузка в PostgreSQL
        print("\n3. Загрузка данных в PostgreSQL...")
        postgres_loader.load_data(df_cleaned, table_name, if_exists='replace')
        
        # Простая проверка что данные загружены
        print("\n4. Проверка загрузки данных...")
        # Используем pandas для проверки
        import pandas as pd
        count = pd.read_sql_query(f"SELECT COUNT(*) as count FROM {table_name}", postgres_loader.engine)['count'][0]
        print(f"Загружено строк: {count}")
            
        # Получим информацию о колонках через pandas
        df_info = pd.read_sql_query(f"SELECT * FROM {table_name} LIMIT 0", postgres_loader.engine)
        print(f"Колонки в таблице: {list(df_info.columns)}")
        
        print("\n" + "=" * 50)
        print("ПАРСИНГ И ЗАГРУЗКА УСПЕШНО ЗАВЕРШЕНЫ!")
        print("=" * 50)
        
    except Exception as e:
        print(f"\nОШИБКА: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()