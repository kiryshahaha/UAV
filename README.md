# Сервис для анализа полетов гражданских БПЛА в регионах РФ

Веб-приложение для анализа количества и длительности полетов гражданских беспилотных воздушных судов в регионах Российской Федерации на основе данных Росавиации.

## Структура проекта

### Корневая структура
```
├── back/                     # Backend приложение (FastAPI)
├── src/                      # Frontend приложение (Next.js)
├── public/                   # Статические файлы фронтенда
├── reports/                  # Отчеты (БД)
├── docker-compose.yml        # Docker компоновка
├── Dockerfile.frontend       # Docker образ фронтенда
└── README.md                # Документация
```

### Backend структура (`/back`)
```
back/
├── app/                     # Основное приложение FastAPI
│   ├── parsers/            # Парсеры данных
│   ├── auto_setup.py       # Автонастройка
│   ├── data_integrator.py  # Интегратор данных
│   ├── data_processor.py   # Обработчик данных
│   ├── database.py         # Настройки БД
│   ├── excel_parser.py     # Парсер Excel файлов
│   ├── flight_parsers.py   # Парсеры полетов
│   ├── main.py             # Основной файл FastAPI
│   ├── models.py           # Модели данных
│   ├── postgres_loader.py  # Загрузчик в PostgreSQL
│   └── run_api.py          # Точка входа сервера
├── excel_to_postgres/      # Утилиты конвертации Excel в PostgreSQL
│   ├── config/             # Конфигурации
│   ├── templates/          # Шаблоны
│   └── main.py
├── .env                    # Переменные окружения бэкенда
├── Dockerfile              # Docker образ бэкенда
└── requirements.txt        # Зависимости Python
```

### Frontend структура (`/src`)
```
src/
├── app/                    # Next.js App Router
│   ├── api/               # API routes
│   ├── auth/              # Аутентификация
│   ├── loading/           # Компоненты загрузки
│   ├── globals.css        # Глобальные стили
│   ├── layout.js          # Корневой layout
│   └── page.jsx           # Главная страница
├── components/            # React компоненты
├── lib/                   # Вспомогательные библиотеки
└── test/                  # Тесты фронтенда
```

## Функциональность

### Для обычных пользователей
- **Поиск по регионам** - интеллектуальный поиск
- **Визуализация на карте** - отображение дронов с точными координатами запуска
- **Статистика по регионам** - сравнение количества запусков и времени полетов
- **Месячная статистика** - анализ активности по месяцам для каждого региона
- **Смена темы** - переключение между светлой и темной темой

### Для администраторов
- **Загрузка файлов** - импорт Excel таблиц с данными полетов
- **Автоматическое отображение** - карта обновляется в зависимости от загруженных данных
- **Управление регионами** - добавление новых регионов в систему

## Технический стек

### Frontend
- **Next.js** - фреймворк React
- **TypeScript** - типизация
- **Tailwind CSS** - стилизация
- **React-Leaflet** - карты и визуализация

### Backend
- **FastAPI** - Python веб-фреймворк
- **PostgreSQL** - база данных
- **Pandas** - обработка Excel файлов
- **SQLAlchemy** - ORM

##  API Endpoints

### Основные эндпоинты

| Метод | Эндпоинт | Описание |
|-------|----------|----------|
| `GET` | `/` | Главная страница с данными полетов |
| `GET` | `/statistics` | Полная статистика с пагинацией |
| `GET` | `/city/{city_name}` | Данные по конкретному городу |
| `GET` | `/cities` | Список городов для автодополнения |
| `GET` | `/stats/regions` | Статистика по всем регионам |
| `GET` | `/stats/region/{region_name}` | Детальная статистика региона |
| `GET` | `/flights/points` | Точки взлета на карте |
| `GET` | `/flights/{flight_id}` | Данные о зоне полета |
| `GET` | `/stats/regions/monthly` | Статистика по месяцам |
| `POST` | `/api/upload` | Загрузка Excel файлов |
| `POST` | `/admin/regions` | Добавление нового региона |
| `GET` | `/health` | Проверка здоровья API |

### Примеры ответов API

**Главная страница:**
```json
{
  "data": [
    {
      "reg": "бортовой номер",
      "opr": "оператор",
      "typ": "тип ВС",
      "dep": "аэропорт вылета",
      "dest": "аэропорт назначения",
      "flight_zone_radius": "радиус зоны полета",
      "flight_level": "эшелон/уровень",
      "departure_time": "время вылета",
      "arrival_time": "время прибытия"
    }
  ],
  "count": 76902,
  "columns": ["reg", "opr", "typ", "dep", "dest", "flight_zone_radius", "flight_level", "departure_time", "arrival_time"]
}
```

**Статистика по регионам:**
```json
[
  {
    "region": "Красноярский",
    "num_flights": 1500,
    "avg_flight_duration": 45.5
  }
]
```

## Запуск приложения

### Способ 1: Локальный запуск

#### Backend
```bash
# Переходим в папку бэкенда
cd back

# Запуск FastAPI сервера
python app/run_api.py
```

#### Frontend
```bash
# Установка зависимостей
npm install

# Запуск в режиме разработки
npm run dev
```

### Способ 2: Запуск через Docker

#### Docker Compose
```yaml
version: '3.8'
services:
  backend:
    build: ./back
    ports:
      - "8000:8000"
    environment:
      - DB_HOST=aws-1-eu-north-1.pooler.supabase.com
      - DB_PORT=6543
      - DB_NAME=postgres
      - DB_USER=postgres.adrxmxwncvtbvrmqiihb
      - DB_PASSWORD=Trening0811!
      - DB_SCHEMA=public
      - EXCEL_FILE_PATH=/data/2025.xlsx
      - SHEET_NAME=Москва
      - TABLE_NAME=excel_data
      - CHUNK_SIZE=10000
      - USE_AVIATION_TEMPLATES=false
      - NEXT_PUBLIC_BACKEND_URL=http://uav-backend:8000

  frontend:
    build: 
      context: .
      dockerfile: Dockerfile.frontend
    ports:
      - "3000:3000"
    depends_on:
      - backend
```

#### Dockerfile для Frontend
```dockerfile
# Используем легкий Node.js образ
FROM node:20-alpine

# Рабочая директория
WORKDIR /app

# Копируем package.json и package-lock.json
COPY package*.json ./

# Устанавливаем зависимости
RUN npm install

# Копируем весь фронтенд-код
COPY . .

# Указываем порт для Next.js
EXPOSE 3000

# Команда запуска
CMD ["npm", "run", "dev"]
```

### Переменные окружения

```env
DB_HOST=aws-1-eu-north-1.pooler.supabase.com
DB_PORT=6543
DB_NAME=postgres
DB_USER=postgres.adrxmxwncvtbvrmqiihb
DB_PASSWORD=Trening0811!
DB_SCHEMA=public
EXCEL_FILE_PATH=C:\Users\USER\Downloads\2025.xlsx
SHEET_NAME=Москва
TABLE_NAME=excel_data
CHUNK_SIZE=10000
USE_AVIATION_TEMPLATES=false
NEXT_PUBLIC_BACKEND_URL=http://uav-backend:8000
```

## Особенности обработки данных

### Парсинг Excel файлов
- Автоматическое определение полей данных
- Приведение таблиц к единому формату
- Обработка больших объемов данных (70.000+ данных)
- Поддержка авиационных шаблонов данных

### Основные поля данных
- **Регистрационные данные**: `reg`, `typ`, `opr`
- **Маршрут**: `dep`, `dest`, `dep_1` (координаты)
- **Время**: `departure_time`, `arrival_time`, `dof` (дата)
- **Зона полета**: `flight_zone`, `flight_zone_radius`, `flight_level`
- **Регион**: `tsentr_es_orvd` (центр ЕС ОРВД)

## Разработка

### Ключевые файлы приложения

**Backend:**
- `back/app/run_api.py` - Точка входа сервера
- `back/app/main.py` - Основное FastAPI приложение
- `back/app/excel_parser.py` - Парсер Excel файлов
- `back/app/models.py` - Модели данных SQLAlchemy

**Frontend:**
- `src/app/page.jsx` - Главная страница
- `src/app/layout.js` - Корневой layout
- `src/components/` - React компоненты

## Возможности анализа

- **Количественный анализ** - подсчет количества полетов по регионам
- **Временной анализ** - длительность полетов, сезонные колебания
- **Географический анализ** - распределение активности по регионам
- **Сравнительный анализ** - сравнение показателей между регионами

---

*Разработано в рамках хакатона по теме анализа полетной активности гражданских БПЛА в РФ*
