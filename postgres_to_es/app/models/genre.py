from models.base import Base


class Genre(Base):
    """Модель жанра."""
    name: str
    description: str = None
