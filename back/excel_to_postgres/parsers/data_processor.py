import pandas as pd
import re

class DataProcessor:
    """Обработчик данных"""
    
    @staticmethod
    def clean_column_names(columns):
        """
        Очистка и нормализация названий колонок
        
        Args:
            columns (list): Список названий колонок
            
        Returns:
            list: Очищенные названия колонок
        """
        cleaned_columns = []
        for col in columns:
            # Приводим к нижнему регистру
            col = col.lower()
            # Заменяем пробелы и специальные символы на подчеркивания
            col = re.sub(r'[\s\-]+', '_', col)
            # Удаляем все не-ASCII символы
            col = re.sub(r'[^a-z0-9_]', '', col)
            # Удаляем ведущие и trailing подчеркивания
            col = col.strip('_')
            # Если после очистки колонка пустая, даем дефолтное имя
            if not col:
                col = f'column_{len(cleaned_columns)}'
            cleaned_columns.append(col)
        
        return cleaned_columns
    
    @staticmethod
    def clean_dataframe(df):
        """
        Очистка DataFrame
        
        Args:
            df (DataFrame): Исходный DataFrame
            
        Returns:
            DataFrame: Очищенный DataFrame
        """
        # Сохраняем оригинальные колонки для логирования
        original_columns = df.columns.tolist()
        
        # Очищаем названия колонок
        df.columns = DataProcessor.clean_column_names(df.columns)
        
        # Удаляем полностью пустые строки
        df = df.dropna(how='all')
        
        # Удаляем полностью пустые колонки
        df = df.dropna(axis=1, how='all')
        
        # Заменяем оставшиеся NaN на None для PostgreSQL
        df = df.where(pd.notnull(df), None)
        
        # Логируем изменения
        print(f"Оригинальные колонки: {original_columns}")
        print(f"Очищенные колонки: {df.columns.tolist()}")
        print(f"Размер данных после очистки: {df.shape}")
        
        return df
    
    @staticmethod
    def map_pandas_to_postgres_types(df):
        """
        Маппинг типов данных pandas -> PostgreSQL
        
        Args:
            df (DataFrame): DataFrame для анализа
            
        Returns:
            dict: Словарь с маппингом типов
        """
        from sqlalchemy import types as sa_types
        
        type_mapping = {
            'int64': sa_types.BIGINT,
            'float64': sa_types.FLOAT,
            'datetime64[ns]': sa_types.TIMESTAMP,
            'bool': sa_types.BOOLEAN,
            'object': sa_types.TEXT
        }
        
        dtypes = {}
        for col in df.columns:
            pandas_type = str(df[col].dtype)
            postgres_type = type_mapping.get(pandas_type, sa_types.TEXT)
            dtypes[col] = postgres_type
        
        return dtypes