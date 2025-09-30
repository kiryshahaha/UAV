
import os
import pandas as pd
from typing import Dict, List, Optional, Any
from dotenv import load_dotenv
import json
from config.database import DatabaseConfig
from back.app.excel_parser import ExcelParser
from back.app.data_processor import DataProcessor
from back.app.postgres_loader import PostgresLoader
from templates.aviation_templates import AviationTemplateProcessor, create_aviation_table_name, get_aviation_table_schema

load_dotenv()


class TemplateEnabledLoader:
    """Загрузчик с поддержкой авиационных шаблонов"""
    
    def __init__(self):
        self.excel_parser = ExcelParser()
        self.data_processor = DataProcessor()
        self.postgres_loader = PostgresLoader()
        self.aviation_processor = AviationTemplateProcessor()
        
        # Параметры из .env
        self.base_table_name = os.getenv('TABLE_NAME', 'excel_data')
        self.use_aviation_templates = os.getenv('USE_AVIATION_TEMPLATES', 'true').lower() == 'true'
        self.auto_detect_message_type = os.getenv('AUTO_DETECT_MESSAGE_TYPE', 'true').lower() == 'true'
        self.generate_reports = os.getenv('GENERATE_REPORTS', 'true').lower() == 'true'
        
    def process_sheet_with_template(self, df: pd.DataFrame, sheet_name: str, message_type: Optional[str] = None) -> Dict[str, Any]:
        """Обработка листа Excel с применением авиационного шаблона"""
        print(f"\n  Применение авиационного шаблона к листу '{sheet_name}'...")
        
        # Определяем тип сообщения
        if message_type is None and self.auto_detect_message_type:
            message_type = self.aviation_processor.detect_message_type(df)
            print(f"    Автоматически определен тип сообщения: {message_type}")
        elif message_type is None:
            message_type = 'FPL'
            print(f"    Используется тип сообщения по умолчанию: {message_type}")
        
        # Применяем стандартный процессинг (очистка колонок и данных)
        df_cleaned = self.data_processor.clean_dataframe(df.copy())
        
        if df_cleaned.empty:
            return {
                'success': False,
                'message': f"Лист '{sheet_name}' пустой после стандартной очистки",
                'data': None
            }
        
        try:
            # Применяем авиационный шаблон
            df_aviation = self.aviation_processor.apply_template(df_cleaned, message_type)
            
            # Валидируем данные
            validation_errors = self.aviation_processor.validate_data(df_aviation, message_type)
            
            # Генерируем отчет если требуется
            report = None
            if self.generate_reports:
                report = self.aviation_processor.generate_summary_report(df_aviation, message_type)
            
            print(f"    Шаблон применен успешно")
            print(f"    Обработано записей: {len(df_aviation)}")
            if validation_errors:
                print(f"    Найдено ошибок валидации: {len(validation_errors)}")
            
            return {
                'success': True,
                'message_type': message_type,
                'data': df_aviation,
                'validation_errors': validation_errors,
                'report': report,
                'processed_records': len(df_aviation)
            }
            
        except Exception as e:
            return {
                'success': False,
                'message': f"Ошибка применения шаблона: {str(e)}",
                'data': None
            }
    
    def load_aviation_data(self, df_aviation: pd.DataFrame, message_type: str, sheet_name: str, if_exists: str = 'replace') -> bool:
        """Загрузка авиационных данных в PostgreSQL"""
        # Создаем имя таблицы
        clean_sheet_name = self.data_processor.clean_sheet_name_for_table(sheet_name)
        table_name = create_aviation_table_name(message_type, f"{self.base_table_name}_{clean_sheet_name}")
        
        try:
            # Получаем схему таблицы для авиационных данных
            aviation_schema = get_aviation_table_schema(message_type)
            
            print(f"    Загрузка в таблицу: {table_name}")
            print(f"    Схема таблицы: {list(aviation_schema.keys())}")
            
            # Загружаем данные
            self.postgres_loader.load_data(df_aviation, table_name, if_exists=if_exists)
            
            return True
            
        except Exception as e:
            print(f"    Ошибка загрузки авиационных данных: {str(e)}")
            return False
    
    def save_processing_report(self, reports: Dict[str, Any], output_dir: str = "reports"):
        """Сохранение отчетов о процессинге"""
        if not reports:
            return
        
        try:
            # Создаем директорию если не существует
            os.makedirs(output_dir, exist_ok=True)
            
            # Сохраняем общий отчет
            timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
            report_filename = os.path.join(output_dir, f"aviation_processing_report_{timestamp}.json")
            
            with open(report_filename, 'w', encoding='utf-8') as f:
                json.dump(reports, f, ensure_ascii=False, indent=2, default=str)
            
            print(f"\nОтчет сохранен: {report_filename}")
            
            # Сохраняем краткую статистику
            summary_filename = os.path.join(output_dir, f"processing_summary_{timestamp}.txt")
            with open(summary_filename, 'w', encoding='utf-8') as f:
                f.write("=== ОТЧЕТ О ПРОЦЕССИНГЕ АВИАЦИОННЫХ ДАННЫХ ===\n\n")
                
                total_sheets = len(reports.get('sheets', {}))
                successful_sheets = sum(1 for sheet_data in reports.get('sheets', {}).values() 
                                      if sheet_data.get('success', False))
                total_records = sum(sheet_data.get('processed_records', 0) 
                                  for sheet_data in reports.get('sheets', {}).values())
                total_errors = sum(len(sheet_data.get('validation_errors', [])) 
                                 for sheet_data in reports.get('sheets', {}).values())
                
                f.write(f"Обработано листов: {successful_sheets}/{total_sheets}\n")
                f.write(f"Всего записей: {total_records}\n")
                f.write(f"Ошибок валидации: {total_errors}\n\n")
                
                f.write("Детализация по листам:\n")
                for sheet_name, sheet_data in reports.get('sheets', {}).items():
                    status = "✓" if sheet_data.get('success', False) else "✗"
                    message_type = sheet_data.get('message_type', 'N/A')
                    records = sheet_data.get('processed_records', 0)
                    errors = len(sheet_data.get('validation_errors', []))
                    
                    f.write(f"{status} {sheet_name}: {message_type}, {records} записей, {errors} ошибок\n")
            
            print(f"Краткая статистика сохранена: {summary_filename}")
            
        except Exception as e:
            print(f"Ошибка сохранения отчетов: {str(e)}")
    
    def process_all_sheets(self):
        """Основной метод для обработки всех листов с применением шаблонов"""
        print("=" * 70)
        print("ПРОЦЕССИНГ АВИАЦИОННЫХ ДАННЫХ ИЗ EXCEL")
        print("=" * 70)
        
        processing_reports = {
            'processing_timestamp': pd.Timestamp.now().isoformat(),
            'settings': {
                'use_aviation_templates': self.use_aviation_templates,
                'auto_detect_message_type': self.auto_detect_message_type,
                'generate_reports': self.generate_reports,
                'base_table_name': self.base_table_name
            },
            'sheets': {}
        }
        
        try:
            # Получаем список листов
            sheet_names = self.excel_parser.get_sheet_names()
            print(f"\nНайдено листов: {len(sheet_names)}")
            print(f"Список листов: {sheet_names}")
            
            if not self.use_aviation_templates:
                print("\nАвиационные шаблоны отключены - используется стандартная обработка")
                self._process_standard_way()
                return
            
            # Читаем все листы
            print(f"\nЧтение всех листов Excel...")
            all_sheets_data = self.excel_parser.read_all_excel_sheets()
            
            successful_loads = 0
            total_records = 0
            
            for sheet_name, df in all_sheets_data.items():
                print(f"\n{'='*50}")
                print(f"Обработка листа: '{sheet_name}'")
                print(f"Исходные данные: {df.shape[0]} строк, {df.shape[1]} колонок")
                
                if df.empty:
                    print(f"    Лист '{sheet_name}' пустой, пропускаем")
                    processing_reports['sheets'][sheet_name] = {
                        'success': False,
                        'message': 'Пустой лист',
                        'processed_records': 0
                    }
                    continue
                
                # Применяем авиационный шаблон
                template_result = self.process_sheet_with_template(df, sheet_name)
                
                if not template_result['success']:
                    print(f"    {template_result['message']}")
                    processing_reports['sheets'][sheet_name] = template_result
                    continue
                
                # Загружаем данные в базу
                load_success = self.load_aviation_data(
                    template_result['data'], 
                    template_result['message_type'], 
                    sheet_name
                )
                
                if load_success:
                    successful_loads += 1
                    total_records += template_result['processed_records']
                    print(f"    Успешно загружено: {template_result['processed_records']} записей")
                else:
                    print(f"    Ошибка загрузки данных")
                
                # Сохраняем результат в отчет
                template_result['load_success'] = load_success
                processing_reports['sheets'][sheet_name] = template_result
            
            # Общая статистика
            print(f"\n{'='*70}")
            print(f"ИТОГОВАЯ СТАТИСТИКА:")
            print(f"   Успешно обработано листов: {successful_loads}/{len(all_sheets_data)}")
            print(f"   Общее количество загруженных записей: {total_records}")
            
            # Проверяем загруженные данные
            if successful_loads > 0:
                print(f"\nПроверка загруженных данных:")
                self._verify_loaded_data(processing_reports)
            
            # Сохраняем отчеты
            if self.generate_reports:
                self.save_processing_report(processing_reports)
            
            print(f"\n{'='*70}")
            print("ПРОЦЕССИНГ АВИАЦИОННЫХ ДАННЫХ ЗАВЕРШЕН!")
            print(f"{'='*70}")
            
        except Exception as e:
            print(f"\nКРИТИЧЕСКАЯ ОШИБКА: {e}")
            import traceback
            traceback.print_exc()
    
    def _process_standard_way(self):
        """Стандартная обработка без шаблонов (резервный режим)"""
        print("Переключение на стандартную обработку...")
        
        # Импортируем и запускаем стандартный main
        try:
            from main import main_standard
            main_standard()
        except ImportError:
            print("Ошибка импорта main_standard")
    
    def _verify_loaded_data(self, processing_reports: Dict[str, Any]):
        """Проверка загруженных данных в базе"""
        try:
            for sheet_name, sheet_data in processing_reports['sheets'].items():
                if not sheet_data.get('success') or not sheet_data.get('load_success'):
                    continue
                
                # Формируем имя таблицы
                clean_sheet_name = self.data_processor.clean_sheet_name_for_table(sheet_name)
                message_type = sheet_data.get('message_type', 'FPL')
                table_name = create_aviation_table_name(message_type, f"{self.base_table_name}_{clean_sheet_name}")
                
                try:
                    # Проверяем количество записей
                    count_query = f"SELECT COUNT(*) as count FROM {table_name}"
                    count_result = pd.read_sql_query(count_query, self.postgres_loader.engine)
                    actual_count = count_result['count'][0]
                    expected_count = sheet_data.get('processed_records', 0)
                    
                    status = "✓" if actual_count == expected_count else "⚠"
                    print(f"   {status} Таблица '{table_name}': {actual_count} записей")
                    
                    if actual_count != expected_count:
                        print(f"      Ожидалось: {expected_count}, найдено: {actual_count}")
                    
                    # Обновляем отчет
                    processing_reports['sheets'][sheet_name]['verification'] = {
                        'expected_count': expected_count,
                        'actual_count': actual_count,
                        'verified': actual_count == expected_count
                    }
                    
                except Exception as e:
                    print(f"   Ошибка проверки таблицы '{table_name}': {e}")
                    processing_reports['sheets'][sheet_name]['verification'] = {
                        'error': str(e)
                    }
                    
        except Exception as e:
            print(f"Ошибка при верификации данных: {e}")
    
    def get_available_templates(self) -> List[str]:
        """Получение списка доступных шаблонов"""
        return list(self.aviation_processor.templates.keys())
    
    def preview_template_mapping(self, sheet_name: str, message_type: Optional[str] = None) -> Dict[str, Any]:
        """Предварительный просмотр маппинга шаблона"""
        try:
            # Читаем только первые несколько строк для анализа
            df_sample = pd.read_excel(
                self.excel_parser.excel_file_path,
                sheet_name=sheet_name,
                nrows=5
            )
            
            # Определяем тип сообщения
            if message_type is None:
                message_type = self.aviation_processor.detect_message_type(df_sample)
            
            # Создаем маппинг
            column_mapping = self.aviation_processor._create_column_mapping(
                df_sample.columns.tolist(), 
                message_type
            )
            
            return {
                'sheet_name': sheet_name,
                'detected_message_type': message_type,
                'original_columns': df_sample.columns.tolist(),
                'column_mapping': column_mapping,
                'sample_data': df_sample.head(3).to_dict('records')
            }
            
        except Exception as e:
            return {
                'error': str(e)
            }


class ConfigManager:
    """Менеджер конфигурации для авиационных шаблонов"""
    
    @staticmethod
    def update_env_file(updates: Dict[str, str], env_file: str = '.env'):
        """Обновление файла .env с новыми настройками"""
        try:
            # Читаем существующий файл
            env_lines = []
            if os.path.exists(env_file):
                with open(env_file, 'r', encoding='utf-8') as f:
                    env_lines = f.readlines()
            
            # Создаем словарь существующих настроек
            existing_vars = {}
            for line in env_lines:
                if '=' in line and not line.strip().startswith('#'):
                    key, value = line.strip().split('=', 1)
                    existing_vars[key] = value
            
            # Обновляем значения
            existing_vars.update(updates)
            
            # Записываем обновленный файл
            with open(env_file, 'w', encoding='utf-8') as f:
                f.write("# Настройки базы данных\n")
                for key in ['DB_HOST', 'DB_PORT', 'DB_NAME', 'DB_USER', 'DB_PASSWORD', 'DB_SCHEMA']:
                    if key in existing_vars:
                        f.write(f"{key}={existing_vars[key]}\n")
                
                f.write("\n# Настройки файлов\n")
                for key in ['EXCEL_FILE_PATH', 'SHEET_NAME', 'TABLE_NAME', 'CHUNK_SIZE']:
                    if key in existing_vars:
                        f.write(f"{key}={existing_vars[key]}\n")
                
                f.write("\n# Настройки авиационных шаблонов\n")
                aviation_keys = ['USE_AVIATION_TEMPLATES', 'AUTO_DETECT_MESSAGE_TYPE', 'GENERATE_REPORTS', 'MERGE_SHEETS']
                for key in aviation_keys:
                    if key in existing_vars:
                        f.write(f"{key}={existing_vars[key]}\n")
            
            print(f"Файл {env_file} успешно обновлен")
            return True
            
        except Exception as e:
            print(f"Ошибка обновления {env_file}: {e}")
            return False
    
    @staticmethod
    def create_aviation_config_template(filename: str = ".env.aviation.template"):
        """Создание шаблона конфигурации для авиационных данных"""
        template_content = """# Настройки базы данных
DB_HOST=localhost
DB_PORT=5432
DB_NAME=postgres
DB_USER=postgres
DB_PASSWORD=postgres
DB_SCHEMA=public

# Настройки файлов
EXCEL_FILE_PATH=C:\\Users\\USER\\Downloads\\2024.xlsx
SHEET_NAME=Москва
TABLE_NAME=excel_data
CHUNK_SIZE=10000

# Настройки авиационных шаблонов
USE_AVIATION_TEMPLATES=true
AUTO_DETECT_MESSAGE_TYPE=true
GENERATE_REPORTS=true
MERGE_SHEETS=false

# Дополнительные настройки
REPORTS_DIRECTORY=reports
VALIDATION_LEVEL=normal
PRESERVE_ORIGINAL_DATA=false
"""
        
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                f.write(template_content)
            print(f"Шаблон конфигурации создан: {filename}")
            return True
        except Exception as e:
            print(f"Ошибка создания шаблона: {e}")
            return False


def main_with_templates():
    """Главная функция для запуска обработки с авиационными шаблонами"""
    try:
        # Загружаем переменные окружения
        load_dotenv()
        
        # Создаем загрузчик с поддержкой шаблонов
        loader = TemplateEnabledLoader()
        
        # Проверяем настройки
        print("Текущие настройки:")
        print(f"   - Авиационные шаблоны: {'включены' if loader.use_aviation_templates else 'отключены'}")
        print(f"   - Автоопределение типа: {'включено' if loader.auto_detect_message_type else 'отключено'}")
        print(f"   - Генерация отчетов: {'включена' if loader.generate_reports else 'отключена'}")
        print(f"   - Базовое имя таблиц: {loader.base_table_name}")
        
        # Показываем доступные шаблоны
        available_templates = loader.get_available_templates()
        print(f"   - Доступные шаблоны: {', '.join(available_templates)}")
        
        # Запускаем обработку
        loader.process_all_sheets()
        
    except Exception as e:
        print(f"Критическая ошибка: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    if not os.path.exists('.env'):
        ConfigManager.create_aviation_config_template()
        print("Отредактируйте .env файл согласно вашим настройкам и запустите снова")
    else:
        main_with_templates()