import os
import logging

from dotenv import load_dotenv


load_dotenv()
logging.basicConfig(level=logging.INFO)

POSTGRES_DSN = {
    'dbname': os.environ.get('DB_NAME'),
    'user': os.environ.get('DB_USER'),
    'password': os.environ.get('DB_PASSWORD'),
    'host': os.environ.get('DB_HOST', '127.0.0.1'),
    'port': os.environ.get('DB_PORT', 5432)
}
"""Настройки подключения к Postgres."""

ROWS_LIMIT = int(os.environ.get('ROWS_LIMIT', 100))
"""Кол-во строк для извлечения за раз."""

ES = {
    'HOST': os.environ.get('ES_HOST', '127.0.0.1'),
    'PORT': os.environ.get('ES_PORT', 9200),
    'MOVIES_INDEX': os.environ.get('MOVIES_INDEX', 'movies'),
    'GENRE_INDEX': os.environ.get('GENRE_INDEX', 'genres')
}
"""Настройки подключения к Elasticsearch."""

REDIS_DSN = {
    'host': os.environ.get('REDIS_HOST', '127.0.0.1'),
    'port': os.environ.get('REDIS_PORT', 6379),
    'db': int(os.environ.get('REDIS_DB', 0))
}
"""Настройки подключения к Redis."""

SLEEP_SECONDS = float(os.environ.get('SLEEP_SECONDS', 1.0))
"""Через сколько секунд заново опрашивать Postgres."""

BACKOFF_MAX_TIME = float(os.environ.get('BACKOFF_MAX_TIME', 10.0))
