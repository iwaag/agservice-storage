from dataclasses import dataclass
from datetime import datetime, timezone
import os

from fastapi import HTTPException
from agpyutils.storage import (
    StaticObjectRef,
    DynamicObjectRef,
    PresignDownloadOption,
    PresignUploadOption,
    CopyObjectRequest
)

PRODUCT_ENV = os.getenv("PRODUCT_ENV", "dev")

@dataclass(frozen=True)
class DataDomain:
    domain_folder: str

domain_settings: dict[str, DataDomain] = {
    "agcore": DataDomain(domain_folder="agcore"),
    "agvideo": DataDomain(domain_folder="agvideo"),
    "agimage": DataDomain(domain_folder="agimage"),
}

def get_static_object_key_from_ref(ref: StaticObjectRef) ->str:
    full_path = f"static/env={PRODUCT_ENV}"
    if ref.project_id is not None:
        full_path += f"/project_id={ref.project_id}"
    if ref.user_id is not None:
        full_path += f"/user_id={ref.user_id}"
    full_path += f"/domain={ref.domain}/{ref.relative_key}"
    return full_path


def _check_write_access(
    domain_name: str,
    client_id: str
) -> DataDomain:
    domain = domain_settings.get(domain_name)
    if domain is None:
        raise HTTPException(status_code=403, detail=f"Unknown domain: {domain_name}")
    else:
        if domain.domain_folder != client_id:
            raise HTTPException(
                status_code=403,
                detail=f"Client not allowed to write to this domain. client {client_id}, domain {domain_name}",
            )
    return domain

def _check_read_access(domain_name: str) -> DataDomain:
    domain = domain_settings.get(domain_name)
    if domain is None:
        raise HTTPException(status_code=403, detail=f"Unknown domain: {domain_name}")
    return domain