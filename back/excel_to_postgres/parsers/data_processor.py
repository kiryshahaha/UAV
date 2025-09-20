import pandas as pd
import re
import unicodedata

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
            # Конвертируем в строку если не строка
            col = str(col)
            
            # Нормализуем unicode символы (например, кириллицу)
            col = unicodedata.normalize('NFKD', col)
            
            # Заменяем кириллицу на латиницу (базовая транслитерация)
            cyrillic_to_latin = {
                'а': 'a', 'б': 'b', 'в': 'v', 'г': 'g', 'д': 'd', 'е': 'e', 'ё': 'yo',
                'ж': 'zh', 'з': 'z', 'и': 'i', 'й': 'y', 'к': 'k', 'л': 'l', 'м': 'm',
                'н': 'n', 'о': 'o', 'п': 'p', 'р': 'r', 'с': 's', 'т': 't', 'у': 'u',
                'ф': 'f', 'х': 'h', 'ц': 'ts', 'ч': 'ch', 'ш': 'sh', 'щ': 'sch',
                'ъ': '', 'ы': 'y', 'ь': '', 'э': 'e', 'ю': 'yu', 'я': 'ya',
                'А': 'A', 'Б': 'B', 'В': 'V', 'Г': 'G', 'Д': 'D', 'Е': 'E', 'Ё': 'YO',
                'Ж': 'ZH', 'З': 'Z', 'И': 'I', 'Й': 'Y', 'К': 'K', 'Л': 'L', 'М': 'M',
                'Н': 'N', 'О': 'O', 'П': 'P', 'Р': 'R', 'С': 'S', 'Т': 'T', 'У': 'U',
                'Ф': 'F', 'Х': 'H', 'Ц': 'TS', 'Ч': 'CH', 'Ш': 'SH', 'Щ': 'SCH',
                'Ъ': '', 'Ы': 'Y', 'Ь': '', 'Э': 'E', 'Ю': 'YU', 'Я': 'YA'
            }
            
            for cyr, lat in cyrillic_to_latin.items():
                col = col.replace(cyr, lat)
            
            # Приводим к нижнему регистру
            col = col.lower()
            
            # Заменяем пробелы, дефисы и другие символы на подчеркивания
            col = re.sub(r'[\s\-\.\/\\]+', '_', col)
            
            # Удаляем все символы кроме букв, цифр и подчеркиваний
            col = re.sub(r'[^a-z0-9_]', '', col)
            
            # Удаляем множественные подчеркивания
            col = re.sub(r'_+', '_', col)
            
            # Удаляем ведущие и trailing подчеркивания
            col = col.strip('_')
            
            # Если после очистки колонка пустая или начинается с цифры, даем дефолтное имя
            if not col or col[0].isdigit():
                col = f'column_{len(cleaned_columns)}'
            
            # Проверяем уникальность
            original_col = col
            counter = 1
            while col in cleaned_columns:
                col = f"{original_col}_{counter}"
                counter += 1
                
            cleaned_columns.append(col)
        
        return cleaned_columns
    
    @staticmethod
    def clean_sheet_name_for_table(sheet_name):
        """
        Специальная функция для очистки названий страниц для имен таблиц
        
        Args:
            sheet_name (str): Название страницы
            
        Returns:
            str: Очищенное название для использования в имени таблицы
        """
        # Используем ту же логику что и для колонок, но возвращаем одно значение
        cleaned_names = DataProcessor.clean_column_names([sheet_name])
        return cleaned_names[0] if cleaned_names else 'unknown_sheet'
    
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