from functools import wraps
import logging
import time
import uuid

from pydantic import BaseModel
from elasticsearch import Elasticsearch

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


def get_columns_for_select() -> str:
    """Получить список столбцов для Кинопроизведения в формате SQL.

    Returns:
        str: Столбцы в формате SQL

    """
    return """
        fw.id,
        fw.rating as imdb_rating,
        COALESCE(array_agg(DISTINCT g.name), '{}') as genre,
        fw.title,
        fw.description,
        COALESCE(
            array_agg(DISTINCT p.full_name)
                FILTER (WHERE pfw.role = 'director'),
            '{}'
        ) as director,
        COALESCE(
            array_agg(DISTINCT p.full_name)
                FILTER (WHERE pfw.role = 'actor'),
            '{}'
        ) as actors_names,
        COALESCE(
            array_agg(DISTINCT p.full_name)
                FILTER (WHERE pfw.role = 'writer'),
            '{}'
        ) as writers_names,
        COALESCE(
            json_agg(
                DISTINCT jsonb_build_object(
                    'id', p.id,
                    'name', p.full_name
                )
            ) FILTER (WHERE pfw.role = 'actor'),
            '[]'
        ) as actors,
        COALESCE(
            json_agg(
                DISTINCT jsonb_build_object(
                    'id', p.id,
                    'name', p.full_name
                )
            ) FILTER (WHERE pfw.role = 'writer'),
            '[]'
        ) as writers
    """
