import datetime

from pydantic import FileUrl

from models.base import Base


class Person(Base):
    """Модель персоны в кинопроизведении."""
    name: str


class Genre(Base):
    """Модель жанра в кинопроизведении."""
    name: str


class Filmwork(Base):
    """Модель кинопроизведения."""
    type: str
    title: str
    description: str = None
    imdb_rating: float = None
    creation_date: datetime.date = None
    file_url: FileUrl = None
    actors_names: list[str]
    writers_names: list[str]
    actors: list[Person]
    writers: list[Person]
    directors: list[Person]
    genres: list[Genre]
