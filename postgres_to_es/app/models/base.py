import uuid

from pydantic import BaseModel


class Base(BaseModel):
    """Базовый модель с UUID."""
    id: uuid.UUID
