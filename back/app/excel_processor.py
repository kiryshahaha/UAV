import os
import sys
import subprocess
import logging

logger = logging.getLogger(__name__)

def process_excel_with_external_parser(file_path: str):
    """Запускает внешний парсер excel_to_postgres как subprocess"""
    try:
        # Проверяем что файл существует
        if not os.path.exists(file_path):
            logger.error(f"❌ Файл не найден: {file_path}")
            return False
            
        # Получаем абсолютный путь к парсеру
        current_dir = os.path.dirname(os.path.abspath(__file__))
        back_dir = os.path.dirname(current_dir)
        parser_dir = os.path.join(back_dir, 'excel_to_postgres')
        parser_main = os.path.join(parser_dir, 'main.py')
        
        logger.info(f"🔍 Поиск парсера в: {parser_dir}")
        
        if not os.path.exists(parser_main):
            raise FileNotFoundError(f"❌ Парсер не найден: {parser_main}")
        
        logger.info(f"✅ Парсер найден: {parser_main}")
        logger.info(f"📁 Обрабатываемый файл: {file_path}")
        logger.info(f"📂 Рабочая директория: {parser_dir}")
        
        # Временно меняем переменную окружения с АБСОЛЮТНЫМ путем
        env = os.environ.copy()
        env['EXCEL_FILE_PATH'] = os.path.abspath(file_path)
        
        logger.info(f"🚀 Запуск парсера с файлом: {env['EXCEL_FILE_PATH']}")
        
        # Запускаем парсер
        result = subprocess.run(
            [sys.executable, parser_main],
            cwd=parser_dir,
            env=env,
            capture_output=True,
            text=True,
            timeout=300
        )
        
        if result.returncode == 0:
            logger.info("✅ Парсинг завершен успешно!")
            # Ищем ключевые фразы в выводе
            if "Успешно загружено строк" in result.stdout:
                for line in result.stdout.split('\n'):
                    if "Успешно загружено строк" in line:
                        logger.info(f"📊 {line.strip()}")
            return True
        else:
            logger.error(f"❌ Ошибка парсера (код {result.returncode})")
            if result.stderr:
                # Выводим только последние строки ошибки
                error_lines = result.stderr.strip().split('\n')[-5:]
                for line in error_lines:
                    logger.error(f"  {line}")
            return False
            
    except subprocess.TimeoutExpired:
        logger.error("❌ Таймаут при парсинге файла (5 минут)")
        return False
    except Exception as e:
        logger.error(f"❌ Ошибка запуска парсера: {e}")
        return False