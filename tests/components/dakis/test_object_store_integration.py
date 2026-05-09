from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from uuid import uuid4

import boto3
import pytest
from boto3.session import Session
from botocore.exceptions import ClientError
from mas.schema.common import common_capnp
from types_boto3_s3.client import S3Client
from zalfmas_common import common

from tests.component_harness import PortMessage, PortValue, done_message, run_process_component
from zalfmas_fbp.components.dakis.common.file_payload import object_key, prepared_file_chunk_ips
from zalfmas_fbp.components.dakis.read_file_from_object_store import (
    METADATA as read_file_from_object_store_metadata,
)
from zalfmas_fbp.components.dakis.read_file_from_object_store import ReadFileFromObjectStore
from zalfmas_fbp.components.dakis.write_file_to_object_store import (
    METADATA as write_file_to_object_store_metadata,
)
from zalfmas_fbp.components.dakis.write_file_to_object_store import WriteFileToObjectStore

REPO_ROOT = Path(__file__).resolve().parents[3]
DOTENV_PATH = REPO_ROOT / ".env"
DEFAULT_OBJECT_STORE_URL = "https://objects.dakispro.de"
DEFAULT_BUCKET = "misc"


@dataclass(frozen=True)
class ObjectStoreSmokeConfig:
    endpoint_url: str
    access_key: str
    secret_key: str
    bucket: str


def test_object_store_components_round_trip_against_real_bucket() -> None:
    config = _object_store_smoke_config()
    unique_id = uuid4().hex
    path = f"copilot-smoke-tests/{unique_id}"
    filename = "object-store-smoke.bin"
    key = object_key(path, filename)
    content_type = "application/octet-stream"
    payload = f"copilot-object-store-smoke:{unique_id}".encode()
    s3_client = _s3_client(config)

    try:
        writer = WriteFileToObjectStore(write_file_to_object_store_metadata)
        writer.apply_config_values(
            {
                "object_store_url": config.endpoint_url,
                "access_key": config.access_key,
                "secret_key": config.secret_key,
                "bucket": config.bucket,
            }
        )
        file_ips = list(
            prepared_file_chunk_ips(
                payload,
                path=path,
                filename=filename,
                content_type=content_type,
            )
        )

        run_process_component(
            writer,
            inputs={"in": [_ip_reader_message(ip) for ip in file_ips] + [done_message()]},
            outputs=(),
        )

        reader = ReadFileFromObjectStore(read_file_from_object_store_metadata)
        reader.apply_config_values(
            {
                "object_store_url": config.endpoint_url,
                "access_key": config.access_key,
                "secret_key": config.secret_key,
                "bucket": config.bucket,
                "path": path,
                "filename": filename,
                "content_type": content_type,
            }
        )

        result = run_process_component(reader, inputs={}, outputs=("out",))
        messages = result.output().values
        assert messages[0].type == "openBracket"
        assert messages[-1].type == "closeBracket"
        assert _attr_text(messages[0], "path") == path
        assert _attr_text(messages[0], "filename") == filename
        assert _attr_text(messages[0], "content_type") == content_type
        assert _prepared_file_bytes(messages) == payload

    finally:
        s3_client.delete_object(Bucket=config.bucket, Key=key)

    with pytest.raises(ClientError) as exc_info:
        s3_client.head_object(Bucket=config.bucket, Key=key)

    error_code = str(exc_info.value.response["Error"]["Code"])
    assert error_code in {"404", "NoSuchKey", "NotFound"}


def _object_store_smoke_config() -> ObjectStoreSmokeConfig:
    env_file_values = _read_env_file(DOTENV_PATH)
    env = {**env_file_values, **os.environ}
    run_requested = env.get("RUN_OBJECT_STORE_SMOKE_TEST", "").strip().lower()
    if run_requested not in {"1", "true", "yes", "on"}:
        pytest.skip("Set RUN_OBJECT_STORE_SMOKE_TEST=1 to run the real object-store smoke test.")

    access_key = env.get("ACCESS_KEY", "").strip()
    secret_key = env.get("SECRET_ACCESS_KEY", "").strip()
    if not access_key or not secret_key:
        pytest.skip("Set ACCESS_KEY and SECRET_ACCESS_KEY in the environment or .env to run this smoke test.")

    endpoint_url = env.get("OBJECT_STORE_URL", DEFAULT_OBJECT_STORE_URL).strip() or DEFAULT_OBJECT_STORE_URL
    bucket = env.get("OBJECT_STORE_BUCKET", DEFAULT_BUCKET).strip() or DEFAULT_BUCKET
    return ObjectStoreSmokeConfig(
        endpoint_url=endpoint_url,
        access_key=access_key,
        secret_key=secret_key,
        bucket=bucket,
    )


def _read_env_file(path: Path) -> dict[str, str]:
    if not path.is_file():
        return {}

    values: dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line.removeprefix("export ").strip()

        key, separator, value = line.partition("=")
        if not separator:
            continue
        values[key.strip()] = _strip_quotes(value.strip())

    return values


def _strip_quotes(value: str) -> str:
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "'"}:
        return value[1:-1]
    return value


def _s3_client(config: ObjectStoreSmokeConfig) -> S3Client:
    session: boto3.session.Session = Session(
        aws_access_key_id=config.access_key or None,
        aws_secret_access_key=config.secret_key or None,
    )
    return session.client("s3", endpoint_url=config.endpoint_url)


def _ip_reader_message(ip):
    return PortMessage(PortValue(ip))


def _prepared_file_bytes(messages) -> bytes:
    return b"".join(bytes(ip.content.as_struct(common_capnp.Value).d) for ip in messages[1:-1])


def _attr_text(ip, key: str) -> str:
    attr = common.get_fbp_attr(ip, key)
    return attr.as_text() if attr is not None else ""
