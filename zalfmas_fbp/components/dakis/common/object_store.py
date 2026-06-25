from __future__ import annotations

from pathlib import PurePosixPath
from typing import Any, cast, TYPE_CHECKING

from boto3.session import Session
from botocore.response import StreamingBody

if TYPE_CHECKING:
    from types_boto3_s3.client import S3Client


def object_store_bucket_and_key(*, bucket: str, path: str, filename: str) -> tuple[str, str]:
    bucket = bucket.strip("/")
    path = path.strip("/")
    filename = filename.strip("/")

    if bucket:
        return bucket, object_key(path, filename)

    parts = PurePosixPath(object_key(path, filename)).parts
    if len(parts) < 2:
        msg = "Object store bucket is required when path does not include a bucket segment."
        raise ValueError(msg)

    return parts[0], str(PurePosixPath(*parts[1:]))


def object_key(path: str, filename: str) -> str:
    path = path.strip("/")
    filename = filename.strip("/")
    if not path:
        return filename
    if not filename:
        return path
    return str(PurePosixPath(path) / filename)


def get_object_body(
        *,
        endpoint_url: str,
        access_key: str,
        secret_key: str,
        bucket: str,
        key: str,
) -> StreamingBody:
    s3_client = _object_store_client(
        endpoint_url=endpoint_url,
        access_key=access_key,
        secret_key=secret_key,
    )
    response = s3_client.get_object(Bucket=bucket, Key=key)
    return response["Body"]


def put_object(
        *,
        endpoint_url: str,
        access_key: str,
        secret_key: str,
        bucket: str,
        key: str,
        body: Any,
        content_type: str,
) -> None:
    s3_client = _object_store_client(
        endpoint_url=endpoint_url,
        access_key=access_key,
        secret_key=secret_key,
    )
    s3_client.put_object(Bucket=bucket, Key=key, Body=body, ContentType=content_type)


def _object_store_client(
        *,
        endpoint_url: str,
        access_key: str,
        secret_key: str,
) -> S3Client:
    session = Session(
        aws_access_key_id=access_key or None,
        aws_secret_access_key=secret_key or None,
    )
    return cast("S3Client", session.client("s3", endpoint_url=endpoint_url))
