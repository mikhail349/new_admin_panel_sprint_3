import uuid

from pydantic import BaseModel


class Base(BaseModel):
    """Базовый класс с UUID."""
    id: uuid.UUID
