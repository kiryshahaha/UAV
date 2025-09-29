from dataclasses import dataclass, fields
from typing import Dict, List, Optional, Any, Union
from datetime import datetime
import pandas as pd
import re


@dataclass
class FlightPlanTemplate:
    """Шаблон плана полета FPL"""
    message_type: str = "FPL"
    aircraft_id: Optional[str] = None
    flight_rules: Optional[str] = None
    aircraft_type: Optional[str] = None
    wake_turbulence: Optional[str] = None
    equipment: Optional[str] = None
    departure_aerodrome: Optional[str] = None
    departure_time: Optional[str] = None
    cruising_speed: Optional[str] = None
    cruising_level: Optional[str] = None
    route: Optional[str] = None
    destination_aerodrome: Optional[str] = None
    total_eet: Optional[str] = None
    alternate_aerodromes: Optional[str] = None
    other_info: Optional[str] = None

@dataclass
class DepartureTemplate:
    """Шаблон сообщения о вылете DEP"""
    message_type: str = "DEP"
    aircraft_id: Optional[str] = None
    departure_aerodrome: Optional[str] = None
    departure_time: Optional[str] = None
    destination_aerodrome: Optional[str] = None

@dataclass
class ArrivalTemplate:
    """Шаблон сообщения о прибытии ARR"""
    message_type: str = "ARR"
    aircraft_id: Optional[str] = None
    departure_aerodrome: Optional[str] = None
    arrival_aerodrome: Optional[str] = None
    arrival_time: Optional[str] = None

@dataclass
class DelayTemplate:
    """Шаблон сообщения о задержке DLA"""
    message_type: str = "DLA"
    aircraft_id: Optional[str] = None
    departure_aerodrome: Optional[str] = None
    original_departure_time: Optional[str] = None
    revised_departure_time: Optional[str] = None
    delay_reason: Optional[str] = None

@dataclass
class ChangeTemplate:
    """Шаблон сообщения об изменении CHG"""
    message_type: str = "CHG"
    aircraft_id: Optional[str] = None
    departure_aerodrome: Optional[str] = None
    departure_time: Optional[str] = None
    field_to_change: Optional[str] = None
    new_information: Optional[str] = None

@dataclass
class CancelTemplate:
    """Шаблон сообщения об отмене CNL"""
    message_type: str = "CNL"
    aircraft_id: Optional[str] = None
    departure_aerodrome: Optional[str] = None
    departure_time: Optional[str] = None
    reason: Optional[str] = None

@dataclass
class AlertTemplate:
    """Шаблон аварийного оповещения ALR"""
    message_type: str = "ALR"
    aircraft_id: Optional[str] = None
    alert_phase: Optional[str] = None
    position: Optional[str] = None
    time: Optional[str] = None
    description: Optional[str] = None

class AviationTemplateProcessor:
    """Процессор для обработки авиационных данных по шаблонам"""
    
    def __init__(self):
        self.templates = {
            'FPL': FlightPlanTemplate,
            'DEP': DepartureTemplate,
            'ARR': ArrivalTemplate,
            'DLA': DelayTemplate,
            'CHG': ChangeTemplate,
            'CNL': CancelTemplate,
            'ALR': AlertTemplate
        }
        
        # Словари для стандартизации данных
        self.aircraft_types = {
            'A320': 'A320', 'A321': 'A321', 'A319': 'A319',
            'B737': 'B737', 'B738': 'B738', 'B739': 'B739',
            'SU95': 'SU95', 'AN24': 'AN24', 'TU154': 'TU154',
            'IL76': 'IL76', 'IL96': 'IL96'
        }
        
        self.flight_rules = {
            'I': 'IFR',
            'V': 'VFR',
            'Y': 'IFR/VFR',
            'Z': 'VFR/IFR'
        }
        
        # Российские аэропорты ICAO коды
        self.airports = {
            'SVO': 'UUEE',  # Шереметьево
            'DME': 'UUDD',  # Домодедово
            'VKO': 'UUWW',  # Внуково
            'LED': 'ULLI',  # Пулково СПб
            'KZN': 'UWKD',  # Казань
            'ROV': 'URRR',  # Ростов
            'KRR': 'URKA',  # Краснодар
            'AER': 'URSS',  # Сочи
            'SVX': 'USSS',  # Екатеринбург
            'OVB': 'UNNT',  # Новосибирск
        }

    def detect_message_type(self, df: pd.DataFrame) -> str:
        """Автоматическое определение типа авиационного сообщения по содержимому"""
        columns = [col.lower() for col in df.columns]
        
        # Анализируем колонки для определения типа сообщения
        if any(word in ' '.join(columns) for word in ['plan', 'flight', 'route', 'destination']):
            return 'FPL'
        elif any(word in ' '.join(columns) for word in ['departure', 'takeoff', 'depart']):
            return 'DEP'
        elif any(word in ' '.join(columns) for word in ['arrival', 'landing', 'arrive']):
            return 'ARR'
        elif any(word in ' '.join(columns) for word in ['delay', 'postpone']):
            return 'DLA'
        elif any(word in ' '.join(columns) for word in ['change', 'modify', 'update']):
            return 'CHG'
        elif any(word in ' '.join(columns) for word in ['cancel', 'abort']):
            return 'CNL'
        elif any(word in ' '.join(columns) for word in ['alert', 'emergency', 'distress']):
            return 'ALR'
        
        return 'FPL'

    def standardize_aircraft_id(self, aircraft_id: Any) -> Optional[str]:
        """Стандартизация позывного ВС"""
        if pd.isna(aircraft_id) or aircraft_id is None:
            return None
            
        aircraft_id = str(aircraft_id).strip().upper()
        
        # Убираем лишние символы
        aircraft_id = re.sub(r'[^A-Z0-9]', '', aircraft_id)
        
        return aircraft_id if aircraft_id else None

    def standardize_airport_code(self, airport_code: Any) -> Optional[str]:
        """Стандартизация кода аэропорта"""
        if pd.isna(airport_code) or airport_code is None:
            return None
            
        airport_code = str(airport_code).strip().upper()
        
        # Если это IATA код, преобразуем в ICAO
        if len(airport_code) == 3 and airport_code in self.airports:
            return self.airports[airport_code]
        
        return airport_code if airport_code else None

    def standardize_time(self, time_value: Any) -> Optional[str]:
        """Стандартизация времени в формат HHMM UTC"""
        if pd.isna(time_value) or time_value is None:
            return None
            
        time_str = str(time_value).strip()
        
        # Если уже в формате HHMM
        if re.match(r'^\d{4}$', time_str):
            return time_str
            
        # Пытаемся парсить различные форматы времени
        try:
            if ':' in time_str:
                # Формат HH:MM
                time_parts = time_str.split(':')
                if len(time_parts) == 2:
                    hours = int(time_parts[0])
                    minutes = int(time_parts[1])
                    return f"{hours:02d}{minutes:02d}"
            
            # Формат Excel datetime
            if isinstance(time_value, (datetime, pd.Timestamp)):
                return f"{time_value.hour:02d}{time_value.minute:02d}"
                
        except (ValueError, IndexError, AttributeError):
            pass
            
        return time_str if time_str else None

    def apply_template(self, df: pd.DataFrame, message_type: Optional[str] = None) -> pd.DataFrame:
        """Применение шаблона к данным"""
        if message_type is None:
            message_type = self.detect_message_type(df)
        
        if message_type not in self.templates:
            raise ValueError(f"Неподдерживаемый тип сообщения: {message_type}")
        
        template_class = self.templates[message_type]
        processed_data = []
        
        # Создаем маппинг колонок на основе их содержимого
        column_mapping = self._create_column_mapping(list(df.columns), message_type)
        
        for _, row in df.iterrows():
            template_data = {}
            
            # Заполняем поля шаблона
            template_fields = [f.name for f in fields(template_class)]
            for template_field in template_fields:
                if template_field in column_mapping:
                    source_column = column_mapping[template_field]
                    value = row[source_column] if source_column in row.index else None
                    
                    # Применяем стандартизацию
                    if template_field == 'aircraft_id':
                        value = self.standardize_aircraft_id(value)
                    elif template_field in ['departure_aerodrome', 'destination_aerodrome', 'arrival_aerodrome']:
                        value = self.standardize_airport_code(value)
                    elif 'time' in template_field:
                        value = self.standardize_time(value)
                    
                    template_data[template_field] = value
                else:
                    # Устанавливаем значение по умолчанию
                    if template_field == 'message_type':
                        template_data[template_field] = message_type
                    else:
                        template_data[template_field] = None
            
            processed_data.append(template_data)
        
        # Создаем новый DataFrame с стандартизированными данными
        result_df = pd.DataFrame(processed_data)
        
        return result_df

    def _create_column_mapping(self, columns: List[str], message_type: str) -> Dict[str, str]:
        """Создание маппинга между колонками исходных данных и полями шаблона"""
        mapping = {}
        columns_lower = [col.lower() for col in columns]
        
        # Общие маппинги для всех типов сообщений
        for i, col in enumerate(columns_lower):
            if any(keyword in col for keyword in ['aircraft', 'callsign', 'flight']):
                mapping['aircraft_id'] = columns[i]
            elif any(keyword in col for keyword in ['departure', 'from', 'depart']):
                if 'time' in col:
                    mapping['departure_time'] = columns[i]
                else:
                    mapping['departure_aerodrome'] = columns[i]
            elif any(keyword in col for keyword in ['arrival', 'to', 'dest']):
                if 'time' in col:
                    mapping['arrival_time'] = columns[i]
                else:
                    mapping['destination_aerodrome'] = columns[i]
        
        # Специфичные маппинги для типа FPL
        if message_type == 'FPL':
            for i, col in enumerate(columns_lower):
                if any(keyword in col for keyword in ['type', 'aircraft_type']):
                    mapping['aircraft_type'] = columns[i]
                elif any(keyword in col for keyword in ['route', 'path']):
                    mapping['route'] = columns[i]
                elif any(keyword in col for keyword in ['speed', 'velocity']):
                    mapping['cruising_speed'] = columns[i]
                elif any(keyword in col for keyword in ['level', 'altitude', 'flight_level']):
                    mapping['cruising_level'] = columns[i]
                elif any(keyword in col for keyword in ['alternate']):
                    mapping['alternate_aerodromes'] = columns[i]
        
        return mapping

    def validate_data(self, df: pd.DataFrame, message_type: str) -> List[Dict[str, Any]]:
        """Валидация данных согласно авиационным стандартам"""
        errors = []
        
        for index, row in df.iterrows():
            row_errors = []
            
            # Проверяем обязательные поля
            if message_type in ['FPL', 'DEP', 'ARR', 'DLA', 'CHG', 'CNL']:
                if pd.isna(row.get('aircraft_id')) or not row.get('aircraft_id'):
                    row_errors.append("Отсутствует позывной ВС")
                
                if pd.isna(row.get('departure_aerodrome')) or not row.get('departure_aerodrome'):
                    row_errors.append("Отсутствует аэродром вылета")
            
            # Валидация времени
            time_fields = ['departure_time', 'arrival_time', 'original_departure_time', 'revised_departure_time']
            for field in time_fields:
                field_value = row.get(field)
                if not pd.isna(field_value) and field_value is not None:
                    time_value = str(field_value)
                    if not re.match(r'^\d{4}$', time_value):
                        row_errors.append(f"Некорректный формат времени в поле {field}: {time_value}")
            
            # Валидация кодов аэропортов
            airport_fields = ['departure_aerodrome', 'destination_aerodrome', 'arrival_aerodrome']
            for field in airport_fields:
                field_value = row.get(field)
                if not pd.isna(field_value) and field_value is not None:
                    airport_code = str(field_value)
                    if not re.match(r'^[A-Z]{3,4}$', airport_code):
                        row_errors.append(f"Некорректный код аэропорта в поле {field}: {airport_code}")
            
            if row_errors:
                errors.append({
                    'row': index,
                    'errors': row_errors
                })
        
        return errors

    def generate_summary_report(self, df: pd.DataFrame, message_type: str) -> Dict[str, Any]:
        """Генерация отчета о данных"""
        total_records = len(df)
        validation_errors = self.validate_data(df, message_type)
        
        # Статистика по типам ВС
        aircraft_stats = {}
        if 'aircraft_type' in df.columns:
            aircraft_stats = df['aircraft_type'].value_counts().to_dict()
        
        # Статистика по аэропортам
        departure_stats = {}
        if 'departure_aerodrome' in df.columns:
            departure_stats = df['departure_aerodrome'].value_counts().to_dict()
        
        return {
            'message_type': message_type,
            'total_records': total_records,
            'valid_records': total_records - len(validation_errors),
            'error_records': len(validation_errors),
            'validation_errors': validation_errors,
            'aircraft_statistics': aircraft_stats,
            'departure_statistics': departure_stats,
            'processing_timestamp': datetime.now().isoformat()
        }


# Функции-утилиты для интеграции с основной системой
def create_aviation_table_name(message_type: str, original_table_name: str) -> str:
    """Создание имени таблицы для авиационных данных"""
    return f"{original_table_name}_{message_type.lower()}_aviation"

def get_aviation_table_schema(message_type: str) -> Dict[str, str]:
    """Получение схемы таблицы для авиационных данных"""
    processor = AviationTemplateProcessor()
    
    if message_type not in processor.templates:
        raise ValueError(f"Неподдерживаемый тип сообщения: {message_type}")
    
    template_class = processor.templates[message_type]
    schema = {}
    
    template_fields = [f.name for f in fields(template_class)]
    for field_name in template_fields:
        schema[field_name] = 'TEXT'
    
    return schema


# Пример использования
if __name__ == "__main__":
    # Создаем тестовые данные
    test_data = pd.DataFrame({
        'Flight_Number': ['SU1234', 'UT5678', 'A6789'],
        'Aircraft_Type': ['A320', 'B737', 'SU95'],
        'Departure_Airport': ['SVO', 'LED', 'KZN'],
        'Destination_Airport': ['AER', 'ROV', 'SVX'],
        'Departure_Time': ['08:30', '14:45', '16:20'],
        'Route': ['SVO DCT AER', 'LED L607 ROV', 'KZN R22 SVX']
    })
    
    processor = AviationTemplateProcessor()
    
    # Обрабатываем данные
    processed_data = processor.apply_template(test_data, 'FPL')
    print("Обработанные данные:")
    print(processed_data.to_string())
    
    # Генерируем отчет
    report = processor.generate_summary_report(processed_data, 'FPL')
    print("\nОтчет о валидации:")
    print(f"Всего записей: {report['total_records']}")
    print(f"Валидных записей: {report['valid_records']}")
    print(f"Записей с ошибками: {report['error_records']}")