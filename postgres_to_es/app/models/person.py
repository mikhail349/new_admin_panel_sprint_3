from models.base import Base


class Person(Base):
    """Модель персоны."""
    full_name: str
    roles: list
    film_ids: list
