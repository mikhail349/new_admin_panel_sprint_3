import datetime

from pydantic import FileUrl

from models.base import Base


class Person(Base):
    """Класс персоны в кинопроизведении."""
    name: str


class Genre(Base):
    """Класс жанра в кинопроизведении."""
    name: str


class Filmwork(Base):
    """Класс кинопроизведения."""
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
