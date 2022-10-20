from elasticsearch import Elasticsearch
import psycopg2
from psycopg2.extras import DictCursor
from psycopg2 import OperationalError
from redis import Redis
import backoff

from app import config


def es_init():
    """Инициализировать Elasticsearch."""
    host = config.ES['HOST']
    port = config.ES['PORT']
    return Elasticsearch(f"http://{host}:{port}")


@backoff.on_exception(backoff.expo, OperationalError, max_time=10)
def psql_connect():
    """Подключиться к Postgres."""
    return psycopg2.connect(**config.POSTGRES_DSN, cursor_factory=DictCursor)


def redis_init():
    """Инициализировать Redis."""
    return Redis(host=config.REDIS['HOST'], port=config.REDIS['PORT'],
                 db=0, decode_responses=True)
