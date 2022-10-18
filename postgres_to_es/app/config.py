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
    'INDEX': os.environ.get('ES_INDEX')
}
"""Настройки подключения к Elasticsearch."""

STORAGE_PATH = os.environ.get('STORAGE_PATH')
"""Путь для хранилища состояния."""

SLEEP_SECONDS = float(os.environ.get('SLEEP_SECONDS', 1.0))
"""Через сколько секунд заново опрашивать Postgres."""
