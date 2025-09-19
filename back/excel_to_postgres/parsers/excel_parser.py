import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()

class ExcelParser:
    """Парсер Excel файлов"""
    
    def __init__(self):
        self.excel_file_path = os.getenv('EXCEL_FILE_PATH', 'data.xlsx')
        self.sheet_name = os.getenv('SHEET_NAME', 'Sheet1')
    
    def read_excel(self):
        """Чтение Excel файла"""
        try:
            return pd.read_excel(
                io=self.excel_file_path,
                sheet_name=self.sheet_name
            )
        except Exception as e:
            raise Exception(f"Ошибка при чтении Excel файла: {e}")
    
    def get_columns_info(self):
        """Получить информацию о колонках Excel файла"""
        try:
            df = pd.read_excel(
                io=self.excel_file_path,
                sheet_name=self.sheet_name,
                nrows=1
            )
            return {
                'columns': list(df.columns),
                'dtypes': {col: str(df[col].dtype) for col in df.columns}
            }
        except Exception as e:
            raise Exception(f"Ошибка при получении информации о колонках: {e}")