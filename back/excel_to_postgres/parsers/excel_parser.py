import pandas as pd
import os
from dotenv import load_dotenv
import logging

load_dotenv()

class ExcelParser:
    """Парсер Excel файлов"""
    
    def __init__(self):
        self.excel_file_path = os.getenv('EXCEL_FILE_PATH', 'data.xlsx')
        # Убираем sheet_name по умолчанию, так как теперь будем работать со всеми страницами
    
    def get_sheet_names(self):
        """Получить список всех страниц в Excel файле"""
        try:
            excel_file = pd.ExcelFile(self.excel_file_path)
            return excel_file.sheet_names
        except Exception as e:
            raise Exception(f"Ошибка при получении списка страниц: {e}")
    
    def read_excel_sheet(self, sheet_name):
        """Чтение конкретной страницы Excel файла"""
        try:
            return pd.read_excel(
                io=self.excel_file_path,
                sheet_name=sheet_name
            )
        except Exception as e:
            raise Exception(f"Ошибка при чтении страницы '{sheet_name}': {e}")
    
    def read_all_excel_sheets(self):
        """Чтение всех страниц Excel файла с ограничением первыми 10 строками для дебага"""
        try:
            # Используем pandas.read_excel с sheet_name=None для чтения всех страниц
            all_sheets = pd.read_excel(
                io=self.excel_file_path,
                sheet_name=None  # Это вернет словарь {sheet_name: DataFrame}
            )

            # Ограничиваем только первыми 10 строками для каждого листа
            for sheet_name in all_sheets:
                all_sheets[sheet_name] = all_sheets[sheet_name].head(10)

            return all_sheets
        except Exception as e:
            raise Exception(f"Ошибка при чтении всех страниц Excel файла: {e}")

    
    def get_sheet_columns_info(self, sheet_name):
        """Получить информацию о колонках конкретной страницы"""
        try:
            df = pd.read_excel(
                io=self.excel_file_path,
                sheet_name=sheet_name,
                nrows=1
            )
            return {
                'columns': list(df.columns),
                'dtypes': {col: str(df[col].dtype) for col in df.columns}
            }
        except Exception as e:
            raise Exception(f"Ошибка при получении информации о колонках страницы '{sheet_name}': {e}")
    
    def get_all_sheets_info(self):
        """Получить информацию о всех страницах и их колонках"""
        try:
            sheets_info = {}
            sheet_names = self.get_sheet_names()
            
            for sheet_name in sheet_names:
                sheets_info[sheet_name] = self.get_sheet_columns_info(sheet_name)
            
            return sheets_info
        except Exception as e:
            raise Exception(f"Ошибка при получении информации о всех страницах: {e}")
    
    # Оставляем старые методы для совместимости
    def read_excel(self, sheet_name=None):
        """Чтение Excel файла (обратная совместимость)"""
        if sheet_name:
            return self.read_excel_sheet(sheet_name)
        else:
            # Если sheet_name не указан, читаем первую страницу
            sheet_names = self.get_sheet_names()
            if sheet_names:
                return self.read_excel_sheet(sheet_names[0])
            else:
                raise Exception("В Excel файле нет страниц")
    
    def get_columns_info(self, sheet_name=None):
        """Получить информацию о колонках (обратная совместимость)"""
        if sheet_name:
            return self.get_sheet_columns_info(sheet_name)
        else:
            # Если sheet_name не указан, возвращаем информацию о первой странице
            sheet_names = self.get_sheet_names()
            if sheet_names:
                return self.get_sheet_columns_info(sheet_names[0])
            else:
                raise Exception("В Excel файле нет страниц")