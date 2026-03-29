FROM python:3.14-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY app ./app
COPY pyproject.toml ./

RUN mkdir -p /data

EXPOSE 6379

ENTRYPOINT ["python", "-m", "app.main"]
CMD ["--host", "0.0.0.0", "--port", "6379", "--dir", "/data"]
