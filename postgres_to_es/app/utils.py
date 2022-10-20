import psycopg2
from psycopg2.extras import DictCursor
from psycopg2 import OperationalError
from redis import Redis
import backoff

from app import config


@backoff.on_exception(backoff.expo, OperationalError, max_time=10)
def psql_connect():
    """Подключиться к Postgres."""
    return psycopg2.connect(**config.POSTGRES_DSN, cursor_factory=DictCursor)


def redis_init():
    """Инициализировать Redis."""
    return Redis(**config.REDIS_DSN, decode_responses=True)
