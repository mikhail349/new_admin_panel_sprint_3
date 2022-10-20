import uuid

from pydantic import BaseModel


class Person(BaseModel):
    id: uuid.UUID
    name: str


class Filmwork(BaseModel):
    id: uuid.UUID
    imdb_rating: float = None
    genre: list[str]
    title: str
    description: str = None
    director: list[str]
    actors_names: list[str]
    writers_names: list[str]
    actors: list[Person]
    writers: list[Person]
