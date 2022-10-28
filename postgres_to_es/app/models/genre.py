from models.base import Base


class Genre(Base):
    """Класс жанра."""
    name: str
    description: str = None
