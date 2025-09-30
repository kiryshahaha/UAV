# Используем официальный Python
FROM python:3.12-slim

# Устанавливаем рабочую директорию
WORKDIR /app

# Копируем файлы бэка
COPY back/ /app/

# Устанавливаем зависимости
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

# Открываем порт для FastAPI
EXPOSE 8000

# Команда запуска
CMD ["python", "app/run_api.py"]
