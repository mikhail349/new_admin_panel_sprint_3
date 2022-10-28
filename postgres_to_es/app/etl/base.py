from abc import abstractmethod
from dataclasses import dataclass
import logging
import datetime

from pydantic import BaseModel
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from elastic_transport import ConnectionError
from psycopg2._psycopg import connection
import backoff

from app import config
from app.state import State


def transform(rows: list, target_model: BaseModel) -> list[BaseModel]:
    """Трансформировать данные из формата Postgres в ES.

    Args:
        rows: Данные из Postgres

    Returns:
        list[BaseModel]: Данные для ES

    """
    return [target_model(**row) for row in rows]


@backoff.on_exception(backoff.expo,
                      ConnectionError,
                      max_time=config.BACKOFF_MAX_TIME)
def load(rows: list[BaseModel], index_name: str):
    """Загрузить данные в ElasticSearch.

    Args:
        rows: Данные в подготовленном формате
        index_name: Название индекса

    """
    def es_init():
        """Инициализировать Elasticsearch."""
        host = config.ES['HOST']
        port = config.ES['PORT']
        return Elasticsearch(f"http://{host}:{port}")

    with es_init() as es:
        body = [{'_index': index_name,
                 '_id': row.id,
                 '_source': row.dict()}
                for row in rows]
        if body:
            bulk(es, body)
            logging.info(f'Было обновлено {len(rows)} записей')


@dataclass
class Extractor():
    """Класс извлечения данных из Postgres

    Args:
        connection: Соединение с БД Postgres
        rows_limit: Кол-во строк для обработки
        state: Класс для хранения состояния

    """
    connection: connection
    rows_limit: int
    state: State

    def execute_sql(self, sql: str) -> tuple[list, datetime.datetime]:
        """Получить данные из Postgres и дату-время последнего изменения.

        Args:
            sql: SQL-запрос

        """
        with self.connection.cursor() as curs:
            curs.execute(sql)
            data = curs.fetchall()

            last_updated_at = data[-1]['updated_at'] if data else None
            return (data, last_updated_at)

    @abstractmethod
    def get(self) -> tuple[list, dict]:
        """Получить данные, измененные за период.

        Returns:
            tuple[list[Any], dict]: Список кинопроизведений и словарь
                                    с датой-временем последнего изменения
                                    каждой таблицы
        """
        pass


@dataclass
class Etl():
    """Основной ETL-класс.

    Args:
        connection: Соединение с БД Postgres
        rows_limit: Кол-во строк для обработки
        state: Класс для хранения состояния
        model: Модель для сохранения данных
        index_name: Название индекса

    """
    connection: connection
    rows_limit: int
    state: State
    extractor_class: Extractor
    model: BaseModel
    index_name: str

    def etl(self):
        """Извлечь, трансформировать и загрузить Жанры."""
        extractor = self.extractor_class(connection=self.connection,
                                         rows_limit=self.rows_limit,
                                         state=self.state)

        rows, state = extractor.get()
        rows = transform(rows, self.model)
        load(rows, self.index_name)
        self.save_state(state)

    def save_state(self, state: dict):
        pass
