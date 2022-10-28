from models.base import UUIDMixin


class Genre(UUIDMixin):
    name: str
    description: str = None
