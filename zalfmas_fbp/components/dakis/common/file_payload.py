from __future__ import annotations

from collections.abc import Iterable
from typing import TYPE_CHECKING, Literal, cast

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
DEFAULT_CONTENT_TYPE = common_capnp.MimeTypes.applicationOctetStream
GEOPARQUET_CONTENT_TYPE = common_capnp.MimeTypes.applicationVndApacheParquet
GEOTIFF_CONTENT_TYPE = common_capnp.MimeTypes.imageTiffApplicationGeotiff
COG_CONTENT_TYPE = common_capnp.MimeTypes.imageTiffApplicationGeotiffCloudOptimized
CSV_CONTENT_TYPE = common_capnp.MimeTypes.textCsv
BLOB_CONTENT_TYPE = "common.capnp:Blob"


def blob_content_type(content_type: str) -> str:
    return f"{BLOB_CONTENT_TYPE}[{content_type}]"


def prepared_file_ip(
    data: bytes,
    *,
    path: str,
    filename: str,
    content_type: str,
) -> IPBuilder:
    out_ip = process.blob_ip(data, content_type=content_type)
    out_ip.attributes = _prepared_file_attributes(path=path, filename=filename, content_type=content_type)
    return out_ip


def prepared_file_bracket_ip(
    *,
    bracket_type: Literal["openBracket", "closeBracket"],
    path: str,
    filename: str,
    content_type: str,
) -> IPBuilder:
    out_ip = fbp_capnp.IP.new_message(
        type=bracket_type,
        sysAttributes={"contentType": content_type, "bracketType": {"chunkedContent": {"chunkCount": 0}}},
    )
    if bracket_type == "openBracket":
        out_ip.attributes = _prepared_file_attributes(path=path, filename=filename, content_type=content_type)
    return out_ip


def prepared_file_chunk_ips(
    data: bytes,
    *,
    path: str,
    filename: str,
    content_type: str,
) -> Iterable[IPBuilder]:
    return iter_prepared_file_chunk_ips(
        process.iter_bytes_in_chunks(data),
        path=path,
        filename=filename,
        content_type=content_type,
    )


def iter_prepared_file_chunk_ips(
    chunks: Iterable[bytes],
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
    for chunk in chunks:
        yield _prepared_file_chunk_ip(chunk, content_type=content_type)
    yield prepared_file_bracket_ip(
        bracket_type="closeBracket",
        path=path,
        filename=filename,
        content_type=content_type,
    )


def read_prepared_file_metadata(
    ip: IPReader,
    *,
    default_path: str,
    default_filename: str,
    default_content_type: str = DEFAULT_CONTENT_TYPE,
) -> tuple[str, str, str]:
    path = _attr_text(ip, PATH_ATTR) or default_path
    filename = _attr_text(ip, FILENAME_ATTR) or default_filename
    content_type = process.ip_content_type(ip) or _attr_text(ip, CONTENT_TYPE_ATTR) or default_content_type
    return path, filename, content_type


def _attr_text(ip: IPReader | IPBuilder, key: str) -> str | None:
    attr = common.get_fbp_attr(cast("IPReader", ip), key)
    return attr.as_text() if attr is not None else None


def _prepared_file_attributes(*, path: str, filename: str, content_type: str) -> list[dict[str, str]]:
    return [
        {"key": PATH_ATTR, "value": path},
        {"key": FILENAME_ATTR, "value": filename},
        {"key": CONTENT_TYPE_ATTR, "value": content_type},
    ]


def _prepared_file_chunk_ip(data: bytes, *, content_type: str) -> IPBuilder:
    return process.blob_ip(data, content_type=content_type)
