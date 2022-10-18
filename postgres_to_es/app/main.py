import time
import logging

import psycopg2
from psycopg2.extras import DictCursor
from psycopg2 import OperationalError, InterfaceError

from app import config
from app.utils import backoff
from app.filmwork_etl import FilmworkEtl
from app.state import State, JsonFileStorage


@backoff(classes=(OperationalError,))
def connect():
    """Подключиться к Postgres."""
    return psycopg2.connect(**config.POSTGRES_DSN, cursor_factory=DictCursor)


def main():
    logging.info(f'Сервис запущен.')
    while True:
        try:
            with connect() as psql_conn:
                logging.info(
                    f'Соединение с БД установлено. ' \
                    f'Частота опроса БД: {config.SLEEP_SECONDS} сек.' \
                    f'Кол-во строк за один запрос: {config.ROWS_LIMIT}')
                storage = JsonFileStorage(config.STORAGE_PATH)
                state = State(storage)
                filmwork_etl = FilmworkEtl(connection=psql_conn,
                                           rows_limit=config.ROWS_LIMIT,
                                           state=state)
                while True:
                    filmwork_etl.etl()
                    time.sleep(config.SLEEP_SECONDS)
        except (OperationalError, InterfaceError) as e:
            logging.error(e)
            continue


if __name__ == '__main__':
    main()
