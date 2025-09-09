FROM python:3.11-slim

WORKDIR /app

# 루트에서 빌드하므로 경로 조정
COPY data-collection/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 루트 파일들 복사
COPY config.py .
COPY scheduler_handler.py .
COPY monitoring.py .
COPY rate_limiter.py .
COPY logger_config.py .

# data-collection 폴더 복사
COPY data-collection/ ./data-collection/

# Cloud Run용 포트 설정
EXPOSE 8080

# Flask 앱 실행 (Cloud Run용)
CMD ["python", "scheduler_handler.py"]
