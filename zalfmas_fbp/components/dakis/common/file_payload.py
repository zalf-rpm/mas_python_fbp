from __future__ import annotations

from collections.abc import Iterable
from pathlib import PurePosixPath
from typing import TYPE_CHECKING, Literal

from mas.schema.common import common_capnp
from mas.schema.fbp import fbp_capnp
from zalfmas_common import common

from zalfmas_fbp.run import process

if TYPE_CHECKING:
    from mas.schema.fbp.fbp_capnp.types.builders import IPBuilder
    from mas.schema.fbp.fbp_capnp.types.readers import IPReader

PATH_ATTR = "path"
FILENAME_ATTR = "filename"
CONTENT_TYPE_ATTR = "content_type"


def prepared_file_ip(
    data: bytes,
    *,
    path: str,
    filename: str,
    content_type: str,
) -> IPBuilder:
    out_ip = fbp_capnp.IP.new_message(content=common_capnp.Value.new_message(d=data))
    out_ip.attributes = [
        {"key": PATH_ATTR, "value": path},
        {"key": FILENAME_ATTR, "value": filename},
        {"key": CONTENT_TYPE_ATTR, "value": content_type},
    ]
    return out_ip


def prepared_file_bracket_ip(
    *,
    bracket_type: Literal["openBracket", "closeBracket"],
    path: str,
    filename: str,
    content_type: str,
) -> IPBuilder:
    out_ip = fbp_capnp.IP.new_message(type=bracket_type)
    out_ip.attributes = [
        {"key": PATH_ATTR, "value": path},
        {"key": FILENAME_ATTR, "value": filename},
        {"key": CONTENT_TYPE_ATTR, "value": content_type},
    ]
    return out_ip


def prepared_file_chunk_ips(
    data: bytes,
    *,
    path: str,
    filename: str,
    content_type: str,
) -> Iterable[IPBuilder]:
    yield prepared_file_bracket_ip(
        bracket_type="openBracket",
        path=path,
        filename=filename,
        content_type=content_type,
    )
    for offset in range(0, len(data), process.DEFAULT_BRACKETED_CHUNK_SIZE):
        yield prepared_file_ip(
            data[offset : offset + process.DEFAULT_BRACKETED_CHUNK_SIZE],
            path=path,
            filename=filename,
            content_type=content_type,
        )
    yield prepared_file_bracket_ip(
        bracket_type="closeBracket",
        path=path,
        filename=filename,
        content_type=content_type,
    )


def read_prepared_file(
    ip: IPReader,
    *,
    default_path: str,
    default_filename: str,
    default_content_type: str = "application/octet-stream",
) -> tuple[bytes, str, str, str]:
    data = bytes(ip.content.as_struct(common_capnp.Value).d)
    path = _attr_text(ip, PATH_ATTR) or default_path
    filename = _attr_text(ip, FILENAME_ATTR) or default_filename
    content_type = _attr_text(ip, CONTENT_TYPE_ATTR) or default_content_type
    return data, path, filename, content_type


def read_prepared_file_metadata(
    ip: IPReader,
    *,
    default_path: str,
    default_filename: str,
    default_content_type: str = "application/octet-stream",
) -> tuple[str, str, str]:
    path = _attr_text(ip, PATH_ATTR) or default_path
    filename = _attr_text(ip, FILENAME_ATTR) or default_filename
    content_type = _attr_text(ip, CONTENT_TYPE_ATTR) or default_content_type
    return path, filename, content_type


def read_prepared_file_chunk(ip: IPReader) -> bytes:
    return bytes(ip.content.as_struct(common_capnp.Value).d)


def object_key(path: str, filename: str) -> str:
    path = path.strip("/")
    filename = filename.strip("/")
    if not path:
        return filename
    if not filename:
        return path
    return str(PurePosixPath(path) / filename)


def _attr_text(ip: IPReader, key: str) -> str | None:
    attr = common.get_fbp_attr(ip, key)
    return attr.as_text() if attr is not None else None
