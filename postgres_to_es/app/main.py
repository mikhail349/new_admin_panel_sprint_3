import time
import logging

from psycopg2 import OperationalError, InterfaceError

from app import config
from app.etl import Etl
from app.state import State, RedisStorage
from app.utils import psql_connect, redis_init


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
                redis = redis_init()
                storage = RedisStorage(redis)
                state = State(storage)

                etl = Etl(connection=psql_conn,
                          rows_limit=config.ROWS_LIMIT,
                          state=state)

                while True:
                    etl.etl()
                    time.sleep(config.SLEEP_SECONDS)
        except Exception as e:
            logging.error(e, exc_info=True)
            continue


if __name__ == '__main__':
    main()
