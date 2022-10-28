from models.base import UUIDMixin


class Person(UUIDMixin):
    full_name: str
