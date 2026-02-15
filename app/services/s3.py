"""S3 helper functions for presigned URLs and listing keys."""

from __future__ import annotations

import logging
import os

import httpx
from models.models import ObjectStorage
from typing import Iterable, List, Optional

import boto3
from botocore.client import Config
from agpyutils.storage import (
    StaticObjectRef, 
    PresignDownloadOption,
    PresignUploadOption,
    CopyObjectRequest
)
from models.models import ObjectStorage, StoredObject



_DEFAULT_EXPIRES_IN = 3600
_logger = logging.getLogger(__name__)


def _get_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"Missing required env var: {name}")
    return value

def get_s3_client(storage: ObjectStorage):
    """Create a configured S3 client using docker-compose env vars."""
    endpoint_url = storage.url
    access_key = _get_env("S3_ACCESS_KEY")
    secret_key = _get_env("S3_SECRET_KEY")
    region = storage.region
    _logger.debug(
        "Creating S3 client (endpoint=%s, region=%s, access_key_set=%s)",
        endpoint_url,
        region,
        bool(access_key),
    )
    return boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region,
        config=Config(signature_version="s3v4"),
    )

def create_presigned_upload_url(
    key: str,
    storage: ObjectStorage,
    option: PresignUploadOption,
) -> str:
    """Create a presigned PUT URL for uploading to a specific key."""
    client = get_s3_client(storage)
    params = {"Bucket": storage.bucket, "Key": key}
    if option.content_type:
        params["ContentType"] = option.content_type

    _logger.debug(
        "Generating presigned upload URL (key=%s, expires_in=%s, content_type=%s)",
        key,
        option.expires_in,
        option.content_type,
    )
    url = client.generate_presigned_url(
        "put_object",
        Params=params,
        ExpiresIn=option.expires_in,
    )
    return url


def direct_upload(
    key: str,
    data: bytes,
    storage: ObjectStorage
):
    client = get_s3_client(storage)
    client.put_object(
        Body=data,
        Bucket=storage.bucket,
        Key=key,
    )

def check_object_exists(key: str, storage: ObjectStorage) -> bool:
    client = get_s3_client(storage)
    try:
        head = client.head_object(Bucket=storage.bucket, Key=key)
        if head:
            _logger.debug("Object exists: ", head.get("ETag"), head.get("LastModified"), head.get("ContentLength"), head.get("ContentType"))
        return True
    except client.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "404":
            return False
        raise

def create_presigned_download_url(
    key: str,
    storage: ObjectStorage,
    option: PresignDownloadOption,
) -> str:
    """Create a presigned GET URL for downloading a specific key."""
    client = get_s3_client(storage)
    params = {"Bucket": storage.bucket, "Key": key}
    if option.response_content_type:
        params["ResponseContentType"] = option.response_content_type
    if option.response_content_disposition:
        params["ResponseContentDisposition"] = option.response_content_disposition

    _logger.debug(
        "Generating presigned download URL (key=%s, expires_in=%s, response_content_type=%s, response_content_disposition=%s)",
        key,
        option.expires_in,
        option.response_content_type,
        option.response_content_disposition,
    )
    is_exist = check_object_exists(key, storage)
    if not is_exist:
        raise Exception("object not found")
    url = client.generate_presigned_url(
        "get_object",
        Params=params,
        ExpiresIn=option.expires_in,
    )
    with httpx.Client() as client:
        response = client.get(url)
        _logger.debug("AAAAAAAAAAAAAAAA", response.status_code, len(response.content))
        response.raise_for_status()

    return url



# def list_files_under_path(
#     prefix: str,
#     bucket_name: str,
#     *,
#     max_keys: int = 1000,
# ) -> List[str]:
#     client = get_s3_client()
#     prefix = prefix
#     if prefix and not prefix.endswith("/"):
#         prefix = prefix + "/"

#     _logger.debug(
#         "Listing objects (prefix=%s, max_keys=%s)",
#         prefix,
#         max_keys,
#     )
#     response = client.list_objects_v2(
#         Bucket=bucket_name,
#         Prefix=prefix,
#         Delimiter="/",
#         MaxKeys=max_keys,
#     )

#     contents: Iterable[dict] = response.get("Contents", [])
#     keys = []
#     for obj in contents:
#         key = obj.get("Key")
#         if not key:
#             continue
#         # Exclude the prefix "directory" placeholder if present
#         if prefix and key == prefix:
#             continue
#         keys.append(key)

#     _logger.debug("List result count=%s", len(keys))
#     return keys


# def delete_object(
#     source: StoredObject
# ) -> None:
#     storage = source.storage
#     client = get_s3_client(storage)
#     client.delete_object(Bucket=storage.bucket, Key=source.key)


# def copy_object(
#     source: StoredObject,
#     destination: StoredObject,
# ) -> None:
#     storage = source.storage
#     client = get_s3_client(storage)
#     copy_source = {"Bucket": source.storage.bucket, "Key": source.key}
#     client.copy_object(
#         CopySource=copy_source,
#         Bucket=destination.storage.bucket,
#         Key=destination.key,
#     )


# def move_object(
#     source: StoredObject,
#     destination: StoredObject,
# ) -> None:
#     copy_object(source, destination)
#     delete_object(source)
