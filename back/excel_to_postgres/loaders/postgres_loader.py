from sqlalchemy import create_engine, text
import psycopg2
import pandas as pd
from sqlalchemy import text
from config.database import DatabaseConfig

class PostgresLoader:
    """Загрузчик данных в PostgreSQL"""
    
    def __init__(self):
        self.db_config = DatabaseConfig()
        self.engine = create_engine(self.db_config.get_connection_string()) # type: ignore
    
    def create_table(self, table_name, dtypes):
        """
        Создание таблицы в PostgreSQL
        
        Args:
            table_name (str): Название таблицы
            dtypes (dict): Словарь с типами данных колонок
        """
        try:
            with self.engine.connect() as conn:
                # Проверяем существование таблицы
                check_query = text(
                    f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = '{table_name}')"
                )
                table_exists = conn.execute(check_query).scalar()
                
                if table_exists:
                    print(f"Таблица {table_name} уже существует")
                    return
                
                # Создаем SQL для создания таблицы
                columns_sql = []
                for col_name, col_type in dtypes.items():
                    columns_sql.append(f'"{col_name}" {col_type.__name__}')
                
                create_table_sql = f"""
                    CREATE TABLE {table_name} (
                        {', '.join(columns_sql)}
                    )
                """
                
                conn.execute(text(create_table_sql))
                conn.commit()
                print(f"Таблица {table_name} успешно создана")
                
        except Exception as e:
            raise Exception(f"Ошибка при создании таблицы: {e}")
    
    def load_data(self, df, table_name, if_exists='replace'):
        """
        Загрузка данных в PostgreSQL
        
        Args:
            df (DataFrame): DataFrame с данными
            table_name (str): Название таблицы
            if_exists (str): Стратегия при существующей таблице ('replace', 'append', 'fail')
        """
        try:
            # Получаем типы данных для маппинга
            from parsers.data_processor import DataProcessor
            dtypes = DataProcessor.map_pandas_to_postgres_types(df)
            
            # Создаем таблицу если нужно
            if if_exists == 'replace':
                self.create_table(table_name, dtypes)
            
            # Загружаем данные
            df.to_sql(
                table_name,
                self.engine,
                if_exists=if_exists,
                index=False,
                dtype=dtypes,
                method='multi'  # Для более быстрой вставки
            )
            
            print(f"Данные успешно загружены в таблицу {table_name}")
            
        except Exception as e:
            raise Exception(f"Ошибка при загрузке данных: {e}")
    
def get_table_info(self, table_name):
    """Получить информацию о таблице"""
    try:
        # Простой способ через pandas
        import pandas as pd
        query = f"SELECT * FROM {table_name} LIMIT 0"
        df = pd.read_sql_query(query, self.engine)
        return [{'column_name': col, 'data_type': str(dtype)} for col, dtype in df.dtypes.items()]
    except Exception as e:
        raise Exception(f"Ошибка при получении информации о таблице: {e}")