from dataclasses import dataclass

from app import config
from app.models.person import Person
from app.etl.base import Extractor, Etl


@dataclass
class PersonExtractor(Extractor):
    """Класс извлечения данных из Postgres."""

    def get(self) -> tuple[list, dict]:
        """Получить измененные персоналии.

        Returns:
            tuple[list[Any], dict]: Список персоналий и словарь
                                    с датой-временем последнего изменения
                                    таблицы
        """
        def get_where() -> str:
            """Получить инструкцию WHERE для запроса."""
            value = self.state.get_state('person')
            if value:
                return f"WHERE p.updated_at > '{value}'"
            return ''

        where = get_where()
        sql = f"""
            SELECT
                p.id,
                p.full_name,
                p.updated_at,
                ARRAY_AGG(DISTINCT pfw.role) AS roles,
                ARRAY_AGG(DISTINCT pfw.film_work_id)::text[] AS film_ids
            FROM
                content.person p
                JOIN content.person_film_work pfw ON p.id = pfw.person_id
            {where}
            GROUP BY
                p.id
            ORDER BY
                p.updated_at
            LIMIT {self.rows_limit};
        """
        rows, last_updated = self.execute_sql(sql)
        state = {
            'person': last_updated
        }

        return (rows, state)


class EtlGenre(Etl):
    """Основной ETL-класс для Персоналий."""

    def save_state(self, state: dict):
        if state['person']:
            self.state.set_state('person', str(state['person']))
        return super().save_state(state)


def create_person_etl(psql_conn, state):
    """Создать ETL-класс для Персоналий.

    Args:
        psql_conn: соединение с Postgres
        state: Класс для хранения состояния

    """
    return EtlGenre(connection=psql_conn,
                    rows_limit=config.ROWS_LIMIT,
                    state=state,
                    extractor_class=PersonExtractor,
                    model=Person,
                    index_name='persons')
