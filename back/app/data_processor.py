# data_processor.py
import pandas as pd
import re
import unicodedata
import logging
from datetime import datetime
from sqlalchemy import create_engine, inspect, text, types as sa_types
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError

class DataProcessor:
    """Упрощенный обработчик данных с добавлением уникальных ID"""

    def __init__(self, db_connection_string=None, db_session=None):
        """Инициализация с подключением к базе данных или существующей сессией"""
        if db_session:
            self.db = db_session
            self.engine = db_session.bind
        elif db_connection_string:
            self.engine = create_engine(db_connection_string)
            self.db = None
        else:
            raise ValueError("Необходимо указать либо db_connection_string, либо db_session")
            
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)

        # Настройка формата логов
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)

    @staticmethod
    def clean_column_names(columns):
        """Очистка и нормализация названий колонок"""
        cleaned_columns = []
        for col in columns:
            col = str(col)
            col = unicodedata.normalize('NFKD', col)

            # Заменяем кириллицу на латиницу
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

            col = col.lower()
            col = re.sub(r'[\s\-\.\/\\]+', '_', col)
            col = re.sub(r'[^a-z0-9_]', '', col)
            col = re.sub(r'_+', '_', col)
            col = col.strip('_')

            if not col or col[0].isdigit():
                col = f'column_{len(cleaned_columns)}'

            original_col = col
            counter = 1
            while col in cleaned_columns:
                col = f"{original_col}_{counter}"
                counter += 1

            cleaned_columns.append(col)

        return cleaned_columns

    @staticmethod
    def clean_sheet_name_for_table(sheet_name):
        """Очистка названий страниц для имен таблиц"""
        cleaned_names = DataProcessor.clean_column_names([sheet_name])
        return cleaned_names[0] if cleaned_names else 'unknown_sheet'

    @staticmethod
    def clean_dataframe(df):
        """Очистка DataFrame от пустых строк и колонок"""
        if df.empty:
            return df

        original_columns = df.columns.tolist()
        df.columns = DataProcessor.clean_column_names(df.columns)
        df = df.dropna(how='all')
        df = df.dropna(axis=1, how='all')
        df = df.where(pd.notnull(df), None)

        return df

    @staticmethod
    def map_pandas_to_postgres_types(df):
        """Маппинг типов данных pandas -> PostgreSQL"""
        type_mapping = {
            'int64': sa_types.BigInteger,
            'int32': sa_types.Integer,
            'float64': sa_types.Float,
            'float32': sa_types.Float,
            'datetime64[ns]': sa_types.TIMESTAMP,
            'bool': sa_types.Boolean,
            'object': sa_types.Text,
            'string': sa_types.Text
        }

        dtypes = {}
        for col in df.columns:
            pandas_type = str(df[col].dtype)

            if pandas_type == 'object':
                non_null_values = df[col].dropna()
                if not non_null_values.empty:
                    sample_value = non_null_values.iloc[0]
                    if isinstance(sample_value, (int, float)):
                        pandas_type = 'float64' if isinstance(sample_value, float) else 'int64'
                    elif isinstance(sample_value, bool):
                        pandas_type = 'bool'
                    elif isinstance(sample_value, datetime):
                        pandas_type = 'datetime64[ns]'

            postgres_type = type_mapping.get(pandas_type, sa_types.Text)
            dtypes[col] = postgres_type

        return dtypes

    def decode_flight_plan_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        """Дешифрует сырые данные из полей сообщения о плане запуска"""
        if df.empty:
            return df

        df = df.copy()

        prefixes = [
            'DOF/', 'STS/', 'DEP/', 'DEST/', 'TYP/', 'REG/',
            'EET/', 'OPR/', 'ORGN/', 'PER/', 'DLE/'
        ]

        for prefix in prefixes:
            df[prefix.strip('/')] = None
        df['RMK'] = None
        df['departure_time'] = None
        df['arrival_time'] = None
        df['flight_level'] = None
        df['flight_zone'] = None
        df['flight_zone_radius'] = None

        time_pattern_icao = r'(?<![/])([A-Z]{4})(\d{4})\b'
        time_pattern_hms = r'(\d{2}:\d{2}(?::\d{2})?)'
        shr_pattern = r'\(SHR-(.*?)\)'
        flight_level_pattern = r'M\d{4}/M\d{4}'
        flight_zone_pattern = r'\/ZONA\s+([^\/]+)'

        for idx, row in df.iterrows():
            departure_time = None
            arrival_time = None
            shr_cell = None

            for col in df.columns:
                try:
                    cell = str(row[col])
                    shr_match = re.search(shr_pattern, cell, re.DOTALL)
                    if shr_match and not shr_cell:
                        shr_cell = shr_match.group(1)
                except Exception:
                    continue

            if shr_cell:
                for prefix in prefixes:
                    if prefix in shr_cell:
                        value = shr_cell.split(prefix)[1].split(' ')[0].split(')')[0].split('/')[0]
                        value = value.replace(',', '')
                        df.at[idx, prefix.strip('/')] = value

                        if prefix == 'OPR/':
                            next_prefix_positions = []
                            for p in prefixes:
                                if p in shr_cell and shr_cell.find(p) > shr_cell.find(prefix):
                                    next_prefix_positions.append(shr_cell.find(p))
                            if next_prefix_positions:
                                min_pos = min(next_prefix_positions)
                                value = shr_cell[shr_cell.find(prefix)+len(prefix):min_pos].strip()
                            else:
                                value = shr_cell.split(prefix)[1].strip()
                            df.at[idx, prefix.strip('/')] = value

                flight_level_match = re.search(flight_level_pattern, shr_cell)
                if flight_level_match:
                    df.at[idx, 'flight_level'] = flight_level_match.group(0)

                flight_zone_match = re.search(flight_zone_pattern, shr_cell)
                if flight_zone_match:
                    zone_info = flight_zone_match.group(1).strip()
                    df.at[idx, 'flight_zone'] = zone_info
                    radius_match = re.search(r'R[\d,]+', zone_info)
                    if radius_match:
                        df.at[idx, 'flight_zone_radius'] = radius_match.group(0)
                        df.at[idx, 'flight_zone'] = zone_info.replace(radius_match.group(0), '').strip()

                if 'RMK/' in shr_cell:
                    rmk_value = shr_cell.split('RMK/')[1].split(')')[0].strip()
                    rmk_value = rmk_value.replace(',', '')
                    df.at[idx, 'RMK'] = rmk_value

            idep_found = False
            iarr_found = False
            for col in df.columns:
                try:
                    cell = str(row[col])
                    if not idep_found and ('IDEP' in str(col) or 'IDEP' in cell):
                        idep_found = True
                        atd_match = re.search(r'-ATD\s+(\d{4})', cell)
                        if atd_match:
                            time_str = atd_match.group(1)
                            departure_time = f"{time_str[:2]}:{time_str[2:]}"

                    if not iarr_found and ('IARR' in str(col) or 'IARR' in cell):
                        iarr_found = True
                        ata_match = re.search(r'-ATA\s+(\d{4})', cell)
                        if ata_match:
                            time_str = ata_match.group(1)
                            arrival_time = f"{time_str[:2]}:{time_str[2:]}"
                except Exception:
                    continue

            if not departure_time and shr_cell:
                shr_times = []
                for match in re.finditer(time_pattern_hms, shr_cell):
                    current_time = match.group(1)
                    if current_time not in ["00:00", "00:00:00"]:
                        shr_times.append(current_time)
                for match in re.finditer(time_pattern_icao, shr_cell):
                    time_str = match.group(1)
                    current_time = f"{time_str[:2]}:{time_str[2:]}"
                    if current_time != "00:00":
                        shr_times.append(current_time)
                if shr_times:
                    departure_time = shr_times[0] if len(shr_times) > 0 else None

            if departure_time:
                if departure_time == '24:00' or departure_time == '24:00:00':
                    departure_time = '00:00:00'
                if len(departure_time.split(':')) == 2:
                    departure_time += ':00'

            if arrival_time:
                if arrival_time == '24:00' or arrival_time == '24:00:00':
                    arrival_time = '00:00:00'
                if len(arrival_time.split(':')) == 2:
                    arrival_time += ':00'

            if departure_time and arrival_time:
                try:
                    departure_dt = datetime.strptime(departure_time, "%H:%M:%S")
                    arrival_dt = datetime.strptime(arrival_time, "%H:%M:%S")
                    if arrival_dt < departure_dt:
                        departure_time, arrival_time = arrival_time, departure_time
                except ValueError:
                    pass

            df.at[idx, 'departure_time'] = departure_time
            df.at[idx, 'arrival_time'] = arrival_time

        return df

    def save_to_table_with_id(self, df, table_name="excel_data_result_1"):
        """
        Загрузка данных в таблицу excel_data_result_1 с добавлением уникального ID
        """
        try:
            self.logger.info(f"Начало сохранения {len(df)} строк в таблицу {table_name}")

            # Очищаем DataFrame перед сохранением
            df_cleaned = DataProcessor.clean_dataframe(df)
            if df_cleaned.empty:
                self.logger.info("Нет данных для сохранения")
                return {"added": 0, "total": 0}

            # Используем существующее подключение или создаем новое
            if self.db:
                # Используем существующую сессию
                connection = self.db.connection()
            else:
                # Создаем новое подключение
                connection = self.engine.connect()

            try:
                # Проверяем существование таблицы
                inspector = inspect(self.engine)
                table_exists = inspector.has_table(table_name)

                if table_exists:
                    self.logger.info(f"Таблица {table_name} существует, пересоздаем...")
                    
                    # УДАЛЯЕМ существующую таблицу и создаем новую с правильной структурой
                    connection.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
                    connection.commit()
                
                # Создаем таблицу с данными
                dtypes = DataProcessor.map_pandas_to_postgres_types(df_cleaned)
                
                # Создаем таблицу
                df_cleaned.to_sql(
                    table_name,
                    self.engine,
                    if_exists='replace',  # replace - пересоздает таблицу
                    index=False,
                    dtype=dtypes,
                    chunksize=1000
                )
                
                # Добавляем автоинкрементный ID
                columns_after = [col['name'] for col in inspector.get_columns(table_name)]
                if 'id' not in columns_after:
                    connection.execute(text(f"""
                        ALTER TABLE {table_name} ADD COLUMN id SERIAL PRIMARY KEY;
                    """))
                    self.logger.info("Добавлена колонка id SERIAL PRIMARY KEY")
                
                connection.commit()
                
                added_count = len(df_cleaned)
                self.logger.info(f"Создана новая таблица {table_name} с {added_count} записями")

                return {
                    "added": added_count,
                    "total": added_count
                }

            finally:
                # Закрываем подключение только если мы его создавали
                if not self.db:
                    connection.close()
                    
        except SQLAlchemyError as e:
            self.logger.error(f"Ошибка при сохранении в PostgreSQL: {e}")
            raise
        except Exception as e:
            self.logger.error(f"Неожиданная ошибка при сохранении данных: {e}")
            raise