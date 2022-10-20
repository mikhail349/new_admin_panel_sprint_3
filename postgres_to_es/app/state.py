from dataclasses import dataclass
from typing import Any

from redis import Redis
from redis.exceptions import ConnectionError
import backoff

from app import config


@dataclass
class RedisStorage():
    """Класс для хранения состояния в Redis.

    Args:
        redis: Драйвер Redis

    """
    redis: Redis

    @backoff.on_exception(backoff.expo,
                          ConnectionError,
                          max_time=config.BACKOFF_MAX_TIME)
    def save_state(self, state: dict) -> None:
        """Сохранить состояние в БД.

        Args:
            state: Словарь-состояние

        """
        self.redis.mset(state)

    @backoff.on_exception(backoff.expo,
                          ConnectionError,
                          max_time=config.BACKOFF_MAX_TIME)
    def retrieve_state(self) -> dict:
        """Загрузить состояние из БД.

        Returns:
            dict: Словарь-состояние

        """
        r = self.redis
        return {key: r.get(key) for key in r.keys()}


@dataclass
class State:
    """Класс для хранения состояния при работе с данными,
    чтобы постоянно не перечитывать данные с начала.

    Args:
        storage: Хранилище для постоянного хранения состояния

    """
    storage: RedisStorage

    def __post_init__(self):
        """Инициализировать состояние."""
        self.state = self.storage.retrieve_state()

    def set_state(self, key: str, value: Any) -> None:
        """Сохранить состояние.

        Args:
            key: Ключ
            value: Значение

        """
        self.state[key] = value
        self.storage.save_state(self.state)

    def get_state(self, key: str) -> Any:
        """Получить состояние по ключу.

        Args:
            key: Ключ

        Returns:
            Any: Значение

        """
        return self.state.get(key)
