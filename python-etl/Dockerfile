# Базовый образ
FROM python:3.11-slim

# Рабочая директория внутри контейнера
WORKDIR /app

# Копируем зависимости и устанавливаем
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копируем все скрипты
COPY . .

# Команда по умолчанию (можно позже переопределять)
#CMD ["python", "etl_scripts/first_pipeline.py"]
CMD ["sleep", "infinity"]
