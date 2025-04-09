# Dockerfile

FROM python:3.10-slim

# Создаем рабочую директорию
WORKDIR /app

# Копируем requirements.txt и устанавливаем зависимости
COPY app/requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

# Копируем все остальные файлы из папки app
COPY app/ .

# По умолчанию запускаем main.py (создай его, если ещё не сделал)
CMD ["python", "main.py"]
