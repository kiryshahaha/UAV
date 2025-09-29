import os
from dotenv import load_dotenv

load_dotenv()

class DatabaseConfig:
    """Конфигурация подключения к PostgreSQL"""
    
    def __init__(self):
        self.host = os.getenv('DB_HOST', 'localhost')
        self.port = os.getenv('DB_PORT', '5432')
        self.database = os.getenv('DB_NAME', 'postgres')
        self.user = os.getenv('DB_USER', 'postgres')
        self.password = os.getenv('DB_PASSWORD', '')
        self.schema = os.getenv('DB_SCHEMA', 'public')
    
    def get_connection_string(self):
        """Возвращает строку подключения для SQLAlchemy"""
        # Обработка пустого пароля
        password_part = f":{self.password}" if self.password else ""
        return f"postgresql+psycopg2://{self.user}{password_part}@{self.host}:{self.port}/{self.database}"
    
    def get_psycopg2_params(self):
        """Возвращает параметры для psycopg2"""
        return {
            'host': self.host,
            'port': self.port,
            'database': self.database,
            'user': self.user,
            'password': self.password
        }

# Для быстрой проверки подключения
if __name__ == "__main__":
    config = DatabaseConfig()
    print("Connection string:", config.get_connection_string())