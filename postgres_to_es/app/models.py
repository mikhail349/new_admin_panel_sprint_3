import uuid

from pydantic import BaseModel


class UUIDMixin(BaseModel):
    id: uuid.UUID


class FilmworkPerson(UUIDMixin):
    name: str


class Filmwork(UUIDMixin):
    imdb_rating: float = None
    genre: list[str]
    title: str
    description: str = None
    director: list[str]
    actors_names: list[str]
    writers_names: list[str]
    actors: list[FilmworkPerson]
    writers: list[FilmworkPerson]


class Genre(UUIDMixin):
    name: str
    description: str = None


class Person(UUIDMixin):
    full_name: str
