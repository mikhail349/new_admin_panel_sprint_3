from dataclasses import dataclass
from typing import Any
import datetime

from psycopg2._psycopg import connection
from app.state import State
from app.utils import EtlTransformer, EtlLoader


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

    def get_columns_for_select(self) -> str:
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

    def get_by_filmworks(self) -> tuple[list[Any], datetime.datetime]:
        """Получить сырые данные по измененным Кинопроизведениям и
        дату-время последнего изменения."""

        def get_where() -> str:
            """Получить инструкцию WHERE для запроса."""

            value = self.state.get_state('film_work')
            if value:
                return f"WHERE fw.updated_at > '{value}'"
            return ''

        with self.connection.cursor() as curs:
            columns = self.get_columns_for_select()
            where = get_where()
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

    def get_by_genres(self) -> tuple[list[Any], datetime.datetime]:
        """Получить сырые данные по измененным жанрам и
        дату-время последнего изменения."""

        def get_having() -> str:
            """Получить инструкцию HAVING для запроса."""

            value = self.state.get_state('genre')
            if value:
                return f"HAVING MAX(g.updated_at) > '{value}'"
            return ''

        with self.connection.cursor() as curs:
            columns = self.get_columns_for_select()
            having = get_having()
            sql = f"""
                SELECT
                    {columns}
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
            curs.execute(sql)
            data = curs.fetchall()

            last_updated_at = None
            if data:
                last_updated_at = data[-1]['updated_at']
            return (data, last_updated_at)

    def get_by_persons(self) -> tuple[list[Any], datetime.datetime]:
        """Получить сырые данные по измененным персоналиям и
        дату-время последнего изменения."""

        def get_having() -> str:
            """Получить инструкцию HAVING для запроса."""

            value = self.state.get_state('person')
            if value:
                return f"HAVING MAX(p.updated_at) > '{value}'"
            return ''

        with self.connection.cursor() as curs:
            columns = self.get_columns_for_select()
            having = get_having()
            sql = f"""
                SELECT
                    {columns}
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
            curs.execute(sql)
            data = curs.fetchall()

            last_updated_at = None
            if data:
                last_updated_at = data[-1]['updated_at']
            return (data, last_updated_at)

    def get(self) -> tuple[list[Any], dict]:
        """Получить кинопроизведения, измененные по Кинопроизведениям,
        Жанрам, Персоналиям.

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
        rows = EtlTransformer.transform(rows)
        rows = list(set(rows))
        EtlLoader.load(rows)

        if state['film_work']:
            self.state.set_state('film_work', state['film_work'])
        if state['genre']:
            self.state.set_state('genre', state['genre'])
        if state['person']:
            self.state.set_state('person', state['person'])
