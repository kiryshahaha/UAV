import uvicorn
import sys
import os

# Добавляем текущую папку в путь
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

if __name__ == "__main__":
    print("🚀 Запуск БВС API сервера...")
    print("📍 API будет доступно по адресу: http://localhost:8000")
    print("📚 Документация: http://localhost:8000/docs")
    print("⏹️  Для остановки нажмите Ctrl+C")
    print("\n🔧 Автоматическая настройка данных при запуске...")
    
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )