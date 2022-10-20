from dataclasses import dataclass
from typing import Any, ClassVar
import datetime
import logging

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from elastic_transport import ConnectionError
from psycopg2._psycopg import connection
import backoff

from app.state import State
from app.models import Filmwork
from app import config


def transform(rows: list[Any]) -> list[Filmwork]:
    """Трансформировать Кинопроизведения из формата Postgres в ES.

    Args:
        rows: Данные из Postgres

    """
    return [Filmwork(**row) for row in rows]


@backoff.on_exception(backoff.expo,
                      ConnectionError,
                      max_time=config.BACKOFF_MAX_TIME)
def load(rows: list[Filmwork]):
    """Загрузить Кинопроизведения в ElasticSearch.

    Args:
        rows: Кинопроизведения в подготовленном формате

    """
    def es_init():
        """Инициализировать Elasticsearch."""
        host = config.ES['HOST']
        port = config.ES['PORT']
        return Elasticsearch(f"http://{host}:{port}")

    with es_init() as es:
        body = [{'_index': config.ES['INDEX'],
                 '_id': row.id,
                 '_source': row.dict()}
                for row in rows]
        if body:
            bulk(es, body)
            logging.info(f'Было обновлено {len(rows)} Кинопроизведений')


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

    SELECT_COLUMNS: ClassVar[str] = (
        """
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
    )
    """Столбцы Кинопроизведения в формате SQL для SELECT"""

    def execute_sql(self, sql):
        with self.connection.cursor() as curs:
            curs.execute(sql)
            data = curs.fetchall()

            last_updated_at = data[-1]['updated_at'] if data else None
            return (data, last_updated_at)

    def get_by_filmworks(self) -> tuple[list[Any], datetime.datetime]:
        """Получить сырые данные по измененным Кинопроизведениям и
        дату-время последнего изменения."""

        def get_where() -> str:
            """Получить инструкцию WHERE для запроса."""
            value = self.state.get_state('film_work')
            if value:
                return f"WHERE fw.updated_at > '{value}'"
            return ''

        where = get_where()
        sql = f"""
            SELECT
                {self.SELECT_COLUMNS}
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
        return self.execute_sql(sql)

    def get_by_genres(self) -> tuple[list[Any], datetime.datetime]:
        """Получить сырые данные по измененным жанрам и
        дату-время последнего изменения."""

        def get_having() -> str:
            """Получить инструкцию HAVING для запроса."""
            value = self.state.get_state('genre')
            if value:
                return f"HAVING MAX(g.updated_at) > '{value}'"
            return ''

        having = get_having()
        sql = f"""
            SELECT
                {self.SELECT_COLUMNS}
                ,MAX(g.updated_at) as updated_at
            FROM
                content.film_work fw
                JOIN content.genre_film_work gfw
                    ON gfw.film_work_id = fw.id
                JOIN content.genre g
                    ON g.id = gfw.genre_id
                LEFT JOIN content.person_film_work pfw
                    ON pfw.film_work_id = fw.id
                LEFT JOIN content.person p
                    ON p.id = pfw.person_id
            GROUP BY
                fw.id
            {having}
            ORDER BY
                updated_at
            LIMIT {self.rows_limit};
        """
        return self.execute_sql(sql)

    def get_by_persons(self) -> tuple[list[Any], datetime.datetime]:
        """Получить сырые данные по измененным персоналиям и
        дату-время последнего изменения."""

        def get_having() -> str:
            """Получить инструкцию HAVING для запроса."""
            value = self.state.get_state('person')
            if value:
                return f"HAVING MAX(p.updated_at) > '{value}'"
            return ''

        having = get_having()
        sql = f"""
            SELECT
                {self.SELECT_COLUMNS}
                ,MAX(p.updated_at) as updated_at
            FROM
                content.film_work fw
                JOIN content.person_film_work pfw
                    ON pfw.film_work_id = fw.id
                JOIN content.person p
                    ON p.id = pfw.person_id
                LEFT JOIN content.genre_film_work gfw
                    ON gfw.film_work_id = fw.id
                LEFT JOIN content.genre g
                    ON g.id = gfw.genre_id
            GROUP BY
                fw.id
            {having}
            ORDER BY
                updated_at
            LIMIT {self.rows_limit};
        """
        return self.execute_sql(sql)

    def get(self) -> tuple[list[Any], dict]:
        """Получить кинопроизведения, измененные по
        Кинопроизведениям, Жанрам и Персоналиям.

        Returns:
            tuple[list[Any], dict]: Список кинопроизведений и словарь
                                    с датой-временем последнего изменения
                                    каждой таблицы

        """
        rows_by_filmworks, last_updated_filmwork = self.get_by_filmworks()
        rows_by_genres, last_updated_genre = self.get_by_genres()
        rows_by_persons, last_updated_person = self.get_by_persons()
        rows = rows_by_filmworks + rows_by_genres + rows_by_persons

        state = {
            'film_work': last_updated_filmwork,
            'genre': last_updated_genre,
            'person': last_updated_person
        }

        return (rows, state)


@dataclass
class Etl():
    """Основной ETL-класс.

    Args:
        connection: Соединение с БД Postgres
        rows_limit: Кол-во строк для обработки
        state: Класс для хранения состояния

    """
    connection: connection
    rows_limit: int
    state: State

    def etl(self):
        """Извлечь, трансформировать и загрузить Кинопроизведения."""
        extractor = Extractor(connection=self.connection,
                              rows_limit=self.rows_limit,
                              state=self.state)

        rows, state = extractor.get()
        rows = transform(rows)
        load(rows)

        if state['film_work']:
            self.state.set_state('film_work', str(state['film_work']))
        if state['genre']:
            self.state.set_state('genre', str(state['genre']))
        if state['person']:
            self.state.set_state('person', str(state['person']))
