from dataclasses import dataclass
import logging
from typing import Any

from elasticsearch import Elasticsearch
from elastic_transport import ConnectionError
import psycopg2
from psycopg2.extras import DictCursor
from psycopg2 import OperationalError
from redis import Redis
import backoff

from app import config
from app.models import Filmwork


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
    @backoff.on_exception(backoff.expo,
                          ConnectionError,
                          max_time=config.BACKOFF_MAX_TIME)
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
