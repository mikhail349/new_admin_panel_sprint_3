from dataclasses import dataclass
from typing import Any
import datetime

from psycopg2._psycopg import connection as Connection
from elastic_transport import ConnectionError

from app.utils import backoff, Filmwork, es_init, get_columns_for_select
from app import config
from app.state import State


@dataclass
class FilmworkExtractor():
    """Класс извлечения Кинопроизведений из Postgres.

    Args:
        connection: Соединение с БД Postgres
        rows_limit: Кол-во строк для обработки
        state: Класс для хранения состояния

    """
    connection: Connection
    rows_limit: int
    state: State

    def get_where(self) -> str:
        """Получить инструкцию WHERE для запроса."""

        value = self.state.get_state('film_work')
        if value:
            return f"WHERE fw.updated_at > '{value}'"
        return ''

    def get(self) -> tuple[list[Any], datetime.datetime]:
        """Получить сырые данные по измененным кинопроизведениям и
        дату-время последнего изменения."""

        with self.connection.cursor() as curs:
            columns = get_columns_for_select()
            where = self.get_where()
            sql = f"""
                SELECT
                    {columns}
                    ,fw.updated_at
                FROM
                    content.film_work fw
                    LEFT JOIN content.person_film_work pfw
                        ON pfw.film_work_id = fw.id
                    LEFT JOIN content.person p
                        ON p.id = pfw.person_id
                    LEFT JOIN content.genre_film_work gfw
                        ON gfw.film_work_id = fw.id
                    LEFT JOIN content.genre g
                        ON g.id = gfw.genre_id
                {where}
                GROUP BY
                    fw.id
                ORDER BY
                    fw.updated_at
                LIMIT {self.rows_limit};
            """
            curs.execute(sql)
            data = curs.fetchall()

            last_updated_at = None
            if data:
                last_updated_at = data[-1]['updated_at']
            return (data, last_updated_at)


class FilmworkTransformer():
    """Класс трансформации Кинопроизведений из формата Postgres в ES."""

    @classmethod
    def transform(cls, rows: list[Any]) -> list[Filmwork]:
        """трансформировать Кинопроизведения из формата Postgres в ES.

        Args:
            rows: Данные из Postgres

        """
        return [Filmwork(**row) for row in rows]


@dataclass
class FilmworkLoader():
    """Класс загрузки Кинопроизведений в ElasticSearch.

    Args:
        state: Класс для сохранения состояния

    """
    state: State

    @backoff(classes=(ConnectionError,))
    def load(self, rows: list[Filmwork], last_updated_at):
        """Загрузить Кинопроизведения в ElasticSearch.

        Args:
            rows: Кинопроизведения в подготовленном формате
            last_updated_at: Дата-время последнего изменения

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
                if last_updated_at:
                    self.state.set_state('film_work', last_updated_at)


@dataclass
class FilmworkEtl():
    """ETL-класс для Кинопроизведений.

    Args:
        connection: Соединение с БД Postgres
        rows_limit: Кол-во строк для обработки
        state: Класс для хранения состояния

    """
    connection: Connection
    rows_limit: int
    state: State

    def etl(self):
        """Извлечь, трансформировать и загрузить Кинопроизведения."""
        extractor = FilmworkExtractor(connection=self.connection,
                                      rows_limit=self.rows_limit,
                                      state=self.state)
        rows, last_updated_at = extractor.get()
        rows = FilmworkTransformer.transform(rows)
        loader = FilmworkLoader(state=self.state)
        loader.load(rows, last_updated_at)
