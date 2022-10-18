from dataclasses import dataclass
from functools import wraps
import logging
import time
from typing import Any
import uuid

from pydantic import BaseModel
from elasticsearch import Elasticsearch
from elastic_transport import ConnectionError
import psycopg2
from psycopg2.extras import DictCursor
from psycopg2 import OperationalError

from app import config


class Person(BaseModel):
    id: uuid.UUID
    name: str


class Filmwork(BaseModel):
    id: uuid.UUID
    imdb_rating: float = None
    genre: list[str]
    title: str
    description: str = None
    director: list[str]
    actors_names: list[str]
    writers_names: list[str]
    actors: list[Person]
    writers: list[Person]

    def __hash__(self):
        return hash(self.id)


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
