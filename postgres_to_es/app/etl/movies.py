from dataclasses import dataclass
from typing import ClassVar
import datetime

from app import config
from app.models.filmwork import Filmwork
from app.etl.base import Extractor, Etl


@dataclass
class FilmworkExtractor(Extractor):
    """Класс извлечения данных из Postgres."""

    SELECT_COLUMNS: ClassVar[str] = (
        """
            fw.id,
            fw.rating as imdb_rating,
            fw.title,
            fw.description,
            fw.creation_date,
            fw.file_path as file_url,

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
            ) as writers,
            COALESCE(
                json_agg(
                    DISTINCT jsonb_build_object(
                        'id', p.id,
                        'name', p.full_name
                    )
                ) FILTER (WHERE pfw.role = 'director'),
                '[]'
            ) as directors,
            COALESCE(
                json_agg(
                    DISTINCT jsonb_build_object(
                        'id', g.id,
                        'name', g.name
                    )
                ),
                '[]'
            ) as genres
        """
    )
    """Столбцы Кинопроизведения в формате SQL для SELECT"""

    def get_by_filmworks(self) -> tuple[list, datetime.datetime]:
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

    def get_by_genres(self) -> tuple[list, datetime.datetime]:
        """Получить сырые данные по измененным жанрам и
        дату-время последнего изменения."""

        def get_having() -> str:
            """Получить инструкцию HAVING для запроса."""
            value = self.state.get_state('film_work_genre')
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

    def get_by_persons(self) -> tuple[list, datetime.datetime]:
        """Получить сырые данные по измененным персоналиям и
        дату-время последнего изменения."""

        def get_having() -> str:
            """Получить инструкцию HAVING для запроса."""
            value = self.state.get_state('film_work_person')
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

    def get(self) -> tuple[list, dict]:
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
            'film_work_genre': last_updated_genre,
            'film_work_person': last_updated_person
        }

        return (rows, state)


class EtlFilmwork(Etl):
    """Основной ETL-класс для Кинопроизведений."""

    def save_state(self, state: dict):
        if state['film_work']:
            self.state.set_state('film_work',
                                 str(state['film_work']))
        if state['film_work_genre']:
            self.state.set_state('film_work_genre',
                                 str(state['film_work_genre']))
        if state['film_work_person']:
            self.state.set_state('film_work_person',
                                 str(state['film_work_person']))
        return super().save_state(state)


def create_filmwork_etl(psql_conn, state):
    """Создать ETL-класс для Кинопроизведений.

    Args:
        psql_conn: соединение с Postgres
        state: Класс для хранения состояния

    """
    return EtlFilmwork(connection=psql_conn,
                       rows_limit=config.ROWS_LIMIT,
                       state=state,
                       extractor_class=FilmworkExtractor,
                       model=Filmwork,
                       index_name='movies')
