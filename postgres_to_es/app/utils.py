from dataclasses import dataclass
from functools import wraps
import logging
import time
from typing import Any

from elasticsearch import Elasticsearch
from elastic_transport import ConnectionError
import psycopg2
from psycopg2.extras import DictCursor
from psycopg2 import OperationalError
from redis import Redis

from app import config
from app.models import Filmwork


def es_init():
    """Инициализировать Elasticsearch."""
    host = config.ES['HOST']
    port = config.ES['PORT']
    return Elasticsearch(f"http://{host}:{port}")


def backoff(start_sleep_time=0.1, factor=2, border_sleep_time=10,
            classes=None):
    """
    Функция для повторного выполнения функции через некоторое время,
    если возникла ошибка.
    Использует наивный экспоненциальный рост времени повтора (factor)
    до граничного времени ожидания (border_sleep_time)

    Args:
        start_sleep_time: начальное время повтора
        factor: во сколько раз нужно увеличить время ожидания
        border_sleep_time: граничное время ожидания
        classes: Классы исключений для перехвата

    Returns:
        Wrapped function
    """
    def func_wrapper(func):
        @wraps(func)
        def inner(*args, **kwargs):
            result = None
            next_time = time.time()
            retries = 0

            while True:
                try:
                    if time.time() >= next_time:
                        result = func(*args, **kwargs)
                        if retries:
                            logging.info('Соединение восстановлено.')
                        break
                except classes as e:
                    logging.error(e)
                    t = start_sleep_time * factor**(retries)
                    if t >= border_sleep_time:
                        t = border_sleep_time
                    next_time = time.time() + t
                    retries += 1
                time.sleep(0.1)

            return result
        return inner
    return func_wrapper


@backoff(classes=(OperationalError,))
def psql_connect():
    """Подключиться к Postgres."""
    return psycopg2.connect(**config.POSTGRES_DSN, cursor_factory=DictCursor)


def redis_init():
    """Инициализировать Redis."""
    return Redis(host=config.REDIS['HOST'], port=config.REDIS['PORT'],
                 db=0, decode_responses=True)


class EtlTransformer():
    """Класс трансформации Кинопроизведений из формата Postgres в ES."""

    @classmethod
    def transform(cls, rows: list[Any]) -> list[Filmwork]:
        """трансформировать Кинопроизведения из формата Postgres в ES.

        Args:
            rows: Данные из Postgres

        """
        return [Filmwork(**row) for row in rows]


@dataclass
class EtlLoader():
    """Класс загрузки Кинопроизведений в ElasticSearch."""

    @classmethod
    @backoff(classes=(ConnectionError,))
    def load(cls, rows: list[Filmwork]):
        """Загрузить Кинопроизведения в ElasticSearch.

        Args:
            rows: Кинопроизведения в подготовленном формате

        """
        with es_init() as es:
            body = []
            for row in rows:
                meta = {'index': {'_index': config.ES['INDEX'],
                                  '_id': row.id}}
                body.append(meta)
                body.append(row.dict())

            if body:
                es.bulk(body=body)
                logging.info(f'Было обновлено {len(rows)} Кинопроизведений')
