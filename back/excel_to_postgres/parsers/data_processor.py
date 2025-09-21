import pandas as pd
import re
import unicodedata
import logging
from datetime import datetime

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
    
    def decode_flight_plan_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Дешифрует сырые данные из полей сообщения о плане запуска.
        Ищет буквенные признаки в каждой ячейке строки и распределяет значения по столбцам.
        """
        # Настройка логирования
        logging.basicConfig(level=logging.DEBUG)
        logger = logging.getLogger(__name__)

        # Список всех возможных буквенных признаков
        prefixes = [
            'DOF/', 'STS/', 'DEP/', 'DEST/', 'TYP/', 'REG/',
            'EET/', 'OPR/', 'ORGN/', 'PER/', 'DLE/'
        ]

        # Создаем новые столбцы для каждого признака
        for prefix in prefixes:
            df[prefix.strip('/')] = ''
        df['RMK'] = ''  # Для RMK/ берем всё после признака
        df['departure_time'] = None  # Время вылета
        df['arrival_time'] = None    # Время прибытия

        # Регулярные выражения для времени
        time_pattern_icao = r'(?<![/])([A-Z]{4})(\d{4})\b'  # UUDC0600
        time_pattern_hms = r'(\d{2}:\d{2}(?::\d{2})?)'  # 09:14 или 09:14:00

        # Проходим по каждой строке
        for idx, row in df.iterrows():
            departure_time = None
            arrival_time = None

            # Проходим по каждой ячейке в строке
            for col in df.columns:
                cell = str(row[col])

                # Ищем все признаки в ячейке
                for prefix in prefixes:
                    if prefix in cell:
                        # Извлекаем значение после признака
                        value = cell.split(prefix)[1].split(' ')[0].split(')')[0].split('/')[0]
                        df.at[idx, prefix.strip('/')] = value

                # Обработка RMK/ (берем всё после признака)
                if 'RMK/' in cell:
                    rmk_value = cell.split('RMK/')[1].split(')')[0].strip()
                    df.at[idx, 'RMK'] = rmk_value

                # Поиск времени в текущей ячейке
                times = []
                # Ищем все вхождения времени в формате 09:14 или 09:14:00
                for match in re.finditer(time_pattern_hms, cell):
                    times.append(match.group(1))
                # Ищем все вхождения времени в формате UUDC0600
                for match in re.finditer(time_pattern_icao, cell):
                    time_str = match.group(2)
                    times.append(f"{time_str[:2]}:{time_str[2:]}")

                # Если нашли времена в текущей ячейке
                if times:
                    if len(times) == 1:
                        # Если время одно, это время отправления
                        departure_time = times[0]
                    elif len(times) >= 2:
                        # Если времен два или больше, первое — отправление, второе — прибытие
                        departure_time = times[0]
                        arrival_time = times[1]

            # Если время прибытия не найдено, ищем его в других ячейках строки
            if departure_time and not arrival_time:
                logger.debug(f"Поиск времени прибытия в других ячейках строки {idx}...")
                for col in df.columns:
                    cell = str(row[col])
                    # Ищем время в формате UUDC0600 или 09:14:00, но не равное времени отправления
                    for match in re.finditer(time_pattern_hms, cell):
                        current_time = match.group(1)
                        if current_time != departure_time:
                            logger.debug(f"Найдено время: {current_time} в ячейке {col}")
                            arrival_time = current_time
                            break
                    if arrival_time:
                        break
                    for match in re.finditer(time_pattern_icao, cell):
                        time_str = match.group(2)
                        current_time = f"{time_str[:2]}:{time_str[2:]}"
                        if current_time != departure_time:
                            logger.debug(f"Найдено время: {current_time} в ячейке {col}")
                            arrival_time = current_time
                            break
                    if arrival_time:
                        break

            # Проверка и обмен времен отправления и прибытия, если время прибытия раньше времени отправления
            if departure_time and arrival_time:
                # Добавляем :00, если времени в формате чч:мм
                if len(departure_time.split(':')) == 2:
                    departure_time += ':00'
                if len(arrival_time.split(':')) == 2:
                    arrival_time += ':00'

                departure_dt = datetime.strptime(departure_time, "%H:%M:%S")
                arrival_dt = datetime.strptime(arrival_time, "%H:%M:%S")

                if arrival_dt < departure_dt:
                    departure_time, arrival_time = arrival_time, departure_time

            # Записываем найденные времена
            df.at[idx, 'departure_time'] = departure_time
            df.at[idx, 'arrival_time'] = arrival_time

            # # Логируем, если время не распознано
            # if not departure_time:
            #     logger.warning(f"Время отправления не распознано в строке {idx}.")
            # if not arrival_time:
            #     logger.warning(f"Время прибытия не распознано в строке {idx}.")
            # else:
            #     logger.debug(f"Время прибытия в строке {idx}: {arrival_time}")

        return df