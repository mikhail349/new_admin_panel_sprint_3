import uuid

from pydantic import BaseModel


class UUIDMixin(BaseModel):
    """Миксин с UUID."""
    id: uuid.UUID
