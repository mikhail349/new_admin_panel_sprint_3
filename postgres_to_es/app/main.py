import time
import logging

from app import config
from app.etl.movies import create_filmwork_etl
from app.etl.genres import create_genre_etl
from app.etl.persons import create_person_etl
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

                etl_filmwork = create_filmwork_etl(psql_conn=psql_conn,
                                                   state=state)
                etl_genre = create_genre_etl(psql_conn=psql_conn,
                                             state=state)
                etl_person = create_person_etl(psql_conn=psql_conn,
                                               state=state)

                while True:
                    etl_filmwork.etl()
                    etl_genre.etl()
                    etl_person.etl()
                    time.sleep(config.SLEEP_SECONDS)
        except Exception as e:
            logging.error(e, exc_info=True)
            time.sleep(0.1)
            continue


if __name__ == '__main__':
    main()
