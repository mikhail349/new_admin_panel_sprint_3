from dataclasses import dataclass
from typing import Any
import json

from redis import Redis
from redis.exceptions import ConnectionError

from app.utils import backoff


@dataclass
class RedisStorage():
    redis: Redis

    @backoff(classes=(ConnectionError,))
    def save_state(self, state: dict) -> None:
        self.redis.mset(state)

    @backoff(classes=(ConnectionError,))
    def retrieve_state(self) -> dict:
        r = self.redis
        return {key: r.get(key) for key in r.keys()}


@dataclass
class JsonFileStorage():
    """Класс для постоянного хранения состояния в формате JSON.

    Args:
        file_path: Путь файла json

    """
    file_path: str

    def save_state(self, state: dict) -> None:
        """Сохранить состояние в файл.

        Args:
            state: Словарь-состояние

        """
        with open(self.file_path, 'w') as f:
            json.dump(state, f, default=str)

    def retrieve_state(self) -> dict:
        """Загрузить состояние из файла.
        Если файла нет, будет возвращен пустой словарь.

        Returns:
            dict: Словарь-состояние

        """
        try:
            with open(self.file_path, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            return {}


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
