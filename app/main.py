import json
import logging
import sys
from typing import Annotated, Optional, Tuple
from dataclasses import dataclass

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    stream=sys.stderr,
)

from fastapi import Depends, FastAPI, HTTPException, Header, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel

from agpyutils.auth import get_auth_info, AuthInfo
#from tempauth import get_auth_info, AuthInfo
from agpyutils.storage import (
    StaticObjectRef, 
    DynamicObjectRef,
    PresignDownloadOption,
    PresignUploadOption,
    NewDynamicObjectGroupRequest
)
from models.models import StoredObject
import db.database
from services.common import DataDomain, domain_settings, _check_write_access, _check_read_access, get_static_object_key_from_ref
from sqlmodel import SQLModel, create_engine
from services.s3 import (
    create_presigned_download_url,
    create_presigned_upload_url
)
import services.s3
import os

tags_metadata = [
    {
        "name": "Static Object",
        "description": "Endpoints for generating presigned URLs to upload/download static objects.",
    },
    {
        "name": "Dynamic Object",
        "description": "Endpoints for creating dynamic object groups and generating presigned URLs to upload/download dynamic objects.",
    },
    {
        "name": "Webhook",
        "description": "Endpoints for receiving webhooks from external services.",
    },
]

app = FastAPI(
    title="agservice-storage",
    description="Storage service for agdev. Manages presigned URL generation and object lifecycle on S3-compatible storage.",
    version="0.1.1",
    openapi_tags=tags_metadata,
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    logging.error(f"caught exception: {exc.detail}", exc_info=True)
    return exc

@app.post(
    "/static_object/upload",
    tags=["Static Object"],
    summary="Get presigned upload URL for a static object",
    response_class=PlainTextResponse,
    responses={
        200: {"description": "Presigned upload URL", "content": {"text/plain": {"schema": {"type": "string"}}}},
        403: {"description": "Write access denied for the domain"},
    },
)
def get_static_object_upload_url(
     ref: StaticObjectRef,
     option: PresignUploadOption,
     auth: AuthInfo = Depends(get_auth_info),
):
    user_id, client_id = auth.user_id, auth.client_id
    _check_write_access(ref.domain, client_id)
    key = get_static_object_key_from_ref(ref)
    return PlainTextResponse( create_presigned_upload_url(key, db.database.main_storage, option) )

@app.post(
    "/static_object/download",
    tags=["Static Object"],
    summary="Get presigned download URL for a static object",
    description="Generate a presigned GET URL for downloading a static object from S3. "
    "The caller must have read access to the specified domain.",
    response_class=PlainTextResponse,
    responses={
        200: {"description": "Presigned download URL", "content": {"text/plain": {"schema": {"type": "string"}}}},
        403: {"description": "Read access denied for the domain"},
    },
)
def get_static_object_download_url(
    ref: StaticObjectRef,
    option: PresignDownloadOption,
    auth: AuthInfo = Depends(get_auth_info),
):
    user_id, client_id = auth.user_id, auth.client_id
    _check_read_access(ref.domain)
    key = get_static_object_key_from_ref(ref)
    return  PlainTextResponse( create_presigned_download_url(key, db.database.main_storage, option) )
    
@app.post(
    "/dynamic_object/new_group",
    tags=["Dynamic Object"],
    summary="Create a new dynamic object group",
    responses={
        200: {"description": "UUID of the created group", "content": {"application/json": {"schema": {"type": "string", "format": "uuid"}}}},
    },
)
def get_static_resource(
    request: NewDynamicObjectGroupRequest,
    auth: AuthInfo = Depends(get_auth_info),
) :
    new_group = db.database.new_dynamic_object_group(request)
    group_meta_data = new_group.model_dump_json()
    services.s3.direct_upload(key = f"{new_group.common_prefix}/manifest.json", data = group_meta_data.encode("utf-8"), storage = new_group.storage)
    return new_group.id

@app.post(
    "/dynamic_object/upload",
    tags=["Dynamic Object"],
    summary="Get presigned upload URL for a dynamic object",
    response_class=PlainTextResponse,
    responses={
        200: {"description": "Presigned upload URL", "content": {"text/plain": {"schema": {"type": "string"}}}},
    },
)
def get_dynamic_object_upload_url(
    ref: DynamicObjectRef,
    option: PresignUploadOption,
    auth: AuthInfo = Depends(get_auth_info),
) :
    group = db.database.get_dynamic_object_group(ref)
    return  PlainTextResponse( create_presigned_upload_url(ref.relative_key, group.storage, option) )

@app.post(
    "/dynamic_object/download",
    tags=["Dynamic Object"],
    summary="Get presigned download URL for a dynamic object",
    response_class=PlainTextResponse,
    responses={
        200: {"description": "Presigned download URL", "content": {"text/plain": {"schema": {"type": "string"}}}},
    },
)
async def get_dynamic_object_download_url(
    ref: DynamicObjectRef,
    option: PresignDownloadOption,
    auth: AuthInfo = Depends(get_auth_info),
) :
    group = db.database.get_dynamic_object_group(ref)
    return  PlainTextResponse( create_presigned_download_url(ref.relative_key, group.storage, option) )

@app.post(
    "/webhook/minio",
    tags=["Webhook"],
    summary="Receive MinIO event notification"
)
async def minio_webhook(request: Request):
    raw = await request.body()
    try:
        payload = json.loads(raw.decode("utf-8"))
    except Exception:
        print("Invalid JSON payload")
        return {"ok": False, "reason": "invalid json"}

    print("MinIO webhook payload: %s", payload)
    return {"ok": True}

