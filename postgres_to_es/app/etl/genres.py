from dataclasses import dataclass

from app import config
from app.models.genre import Genre
from app.etl.base import Extractor, Etl


@dataclass
class GenreExtractor(Extractor):
    """Класс извлечения данных из Postgres."""

    def get(self) -> tuple[list, dict]:
        """Получить измененные жанры.

        Returns:
            tuple[list[Any], dict]: Список жанров и словарь
                                    с датой-временем последнего изменения
                                    таблицы
        """
        def get_where() -> str:
            """Получить инструкцию WHERE для запроса."""
            value = self.state.get_state('genre')
            if value:
                return f"WHERE g.updated_at > '{value}'"
            return ''

        where = get_where()
        sql = f"""
            SELECT
                g.id,
                g.name,
                g.description,
                g.updated_at
            FROM
                content.genre g
                JOIN content.genre_film_work gfw ON g.id = gfw.genre_id
            {where}
            GROUP BY
                g.id
            ORDER BY
                g.updated_at
            LIMIT {self.rows_limit};
        """
        rows, last_updated = self.execute_sql(sql)
        state = {
            'genre': last_updated
        }

        return (rows, state)


class EtlGenre(Etl):
    """Основной ETL-класс для Жанров."""

    def save_state(self, state: dict):
        if state['genre']:
            self.state.set_state('genre', str(state['genre']))
        return super().save_state(state)


def create_genre_etl(psql_conn, state):
    """Создать ETL-класс для Жанров.

    Args:
        psql_conn: соединение с Postgres
        state: Класс для хранения состояния

    """
    return EtlGenre(connection=psql_conn,
                    rows_limit=config.ROWS_LIMIT,
                    state=state,
                    extractor_class=GenreExtractor,
                    model=Genre,
                    index_name='genres')
