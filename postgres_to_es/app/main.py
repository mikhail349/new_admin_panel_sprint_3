import time
import logging

from psycopg2 import OperationalError, InterfaceError

from app import config
from app.etl import Etl
from app.state import State, JsonFileStorage
from app.utils import psql_connect


def main():
    logging.info('Сервис запущен.')
    while True:
        try:
            with psql_connect() as psql_conn:
                logging.info(
                    'Соединение с БД установлено. '
                    'Частота опроса БД: %d сек. '
                    'Кол-во строк за один запрос: %d' % (config.SLEEP_SECONDS,
                                                         config.ROWS_LIMIT)
                )
                storage = JsonFileStorage(config.STORAGE_PATH)
                state = State(storage)

                etl = Etl(connection=psql_conn,
                          rows_limit=config.ROWS_LIMIT,
                          state=state)
                
                while True:
                    etl.etl()
                    time.sleep(config.SLEEP_SECONDS)
        except (OperationalError, InterfaceError) as e:
            logging.error(e)
            continue


if __name__ == '__main__':
    main()
