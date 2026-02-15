from datetime import datetime, timezone
import os
from services.common import get_static_object_key_from_ref
from models.models import ObjectStorage, StoredObject, PendingDynamicObject, DynamicObjectGroup
from sqlmodel import SQLModel, Session, create_engine, select
from agpyutils.storage import StaticObjectRef, DynamicObjectRef, NewDynamicObjectGroupRequest



PRODUCT_ENV = os.getenv("PRODUCT_ENV", "dev")
SQL_TYPE = os.getenv("SQL_TYPE")
SQL_USER = os.getenv("SQL_USER")
SQL_PASSWORD = os.getenv("SQL_PASSWORD")
SQL_HOST = os.getenv("SQL_HOST")
SQL_PORT = os.getenv("SQL_PORT")
SQL_DB = os.getenv("SQL_DB")
sql_url = f"{SQL_TYPE}://{SQL_USER}:{SQL_PASSWORD}@{SQL_HOST}:{SQL_PORT}/{SQL_DB}"
engine = create_engine(sql_url)
SQLModel.metadata.create_all(engine)
main_storage: ObjectStorage = None

def initialize_db():
     with Session(engine) as session:
        statement = select(ObjectStorage).where(ObjectStorage.name == "main")
        main_storage = session.exec(statement).first()
        if main_storage is None:
            new_main_storage = ObjectStorage(
                name="main",
                bucket="agdev",
            )
            session.add(new_main_storage)
            session.commit()
            session.refresh(new_main_storage)
            main_storage = new_main_storage
initialize_db()
with Session(engine) as session:
    statement = select(ObjectStorage).where(ObjectStorage.name == "main")
    main_storage = session.exec(statement).first()
def find_storage_by_name(storage_name: str) -> ObjectStorage:
    with Session(engine) as session:
        return session.exec(
            select(ObjectStorage).where(ObjectStorage.name == storage_name)
        ).first()

def add_static_object(resource: StoredObject) -> StoredObject:
    with Session(engine) as session:
        session.add(resource)
        session.commit()
        session.refresh(resource)
        return resource

def get_dynamic_object(ref: DynamicObjectRef) -> PendingDynamicObject:
    with Session(engine) as session:
        resource = session.get(PendingDynamicObject, id)
        return resource


def new_dynamic_object_group(
        request: NewDynamicObjectGroupRequest
    ):
    new_group = DynamicObjectGroup(
        domain=request.domain,
        user_id=request.user_id,
        created_at=datetime.now(timezone.utc),
        storage_id=main_storage.id_#Todo: change on demand
    )
    with Session(engine) as session:
        session.add(new_group)
        session.flush()
        new_group.common_prefix = f"dynamic/env={PRODUCT_ENV}/project_id={request.project_id}/{new_group.created_at.strftime('years=%YYYY/months=%MM/days=%DD')}/domain={new_group.domain}/category={new_group.category}/id={new_group.id}"
        session.commit()
        return new_group

def get_dynamic_object_group(id: str) -> DynamicObjectGroup:
    with Session(engine) as session:
        resource = session.get(DynamicObjectGroup, id)
        return resource
    
def new_dynamic_object(group: DynamicObjectGroup, ref: DynamicObjectRef):
    new_object = PendingDynamicObject(
        relative_key=ref.key,
        purpose=ref.purpose,
        group_id=group.id,
        created_at=datetime.now(timezone.utc),
        storage_id=main_storage.id
    )
    with Session(engine) as session:
        session.add(new_object)
        session.commit()
        session.refresh(new_object)
        return new_object
    




