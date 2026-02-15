from collections import namedtuple
from datetime import datetime, timezone
from uuid import UUID as PyUUID
import os
from typing import List, Optional
from uuid6 import uuid7
from sqlmodel import Field, Relationship, SQLModel
import sqlalchemy as sa

PRODUCT_ENV = os.getenv("PRODUCT_ENV", "dev")

class StoredEntity(SQLModel):
    id: PyUUID = Field(
        default_factory=uuid7,
        primary_key=True,
        index=True,
        nullable=False,
        sa_type=sa.UUID(as_uuid=True),
    )
    created_at: datetime
    def get_domain(self) -> str:
        raise NotImplementedError
    def get_user_id(self) -> int:
        raise NotImplementedError
    def get_storage(self) -> "ObjectStorage":
        raise NotImplementedError

class StoredObject(StoredEntity):
    upload_validated_at: datetime = None
    mime_type: Optional[str] = None
    def get_full_key(self) -> str:
        raise NotImplementedError

class PendingDynamicObject(StoredObject, table=True):
    relative_key: str
    purpose: str #service-specific label to understand purpose of the object
    group_id: Optional[PyUUID] = Field(default=None, foreign_key="dynamicobjectgroup.id")
    group: Optional["DynamicObjectGroup"] = Relationship(back_populates="pending_objcts")
    def get_full_key(self) -> str:
        return f"{PRODUCT_ENV}/{self.group.common_prefix}/{self.relative_key}"
    def get_domain(self) -> str:
        return self.group.domain
    def get_user_id(self) -> str:
        return self.group.user_id
    def get_storage(self) -> "ObjectStorage":
        return self.group.storage


class DynamicObjectGroup(StoredEntity, table=True):
    storage_id: Optional[int] = Field(default=None, foreign_key="objectstorage.id")
    storage: Optional["ObjectStorage"] = Relationship(back_populates="dyamic_object_groups")
    finalized_at: datetime  = None
    common_prefix: str
    domain: str = None
    user_id: str = None
    project_id: str = None
    pending_objcts: List[PendingDynamicObject] = Relationship(back_populates="group")
    def get_ref(self) -> namedtuple:
        return self.domain
    def get_user_id(self) -> str:
        return self.user_id
    def get_storage(self) -> "ObjectStorage":
        return self.storage

class ObjectStorage(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    storage_secret: str = "nosecret"
    name: str
    url: str = os.environ.get("S3_ENDPOINT_URL")
    type: str = "s3"
    region: str = "us-east-1"
    bucket: str = "adev"
    dyamic_object_groups: List[DynamicObjectGroup] = Relationship(back_populates="storage")