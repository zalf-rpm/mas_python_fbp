from __future__ import annotations

from pathlib import Path
from tempfile import NamedTemporaryFile

from rasterio.io import MemoryFile


def prepare_raster_bytes(
    raster_bytes: bytes,
    *,
    compression: str,
    write_as_cog: bool = False,
) -> bytes:
    return _maybe_rewrite(raster_bytes, compression=compression, write_as_cog=write_as_cog)


def write_raster_bytes(
    raster_bytes: bytes,
    *,
    output_path: str | Path,
    compression: str,
    write_as_cog: bool = False,
) -> Path:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    data = prepare_raster_bytes(raster_bytes, compression=compression, write_as_cog=write_as_cog)

    with NamedTemporaryFile("wb", dir=path.parent, prefix=f".{path.name}.", suffix=".tmp", delete=False) as tmp_file:
        tmp_path = Path(tmp_file.name)
        tmp_file.write(data)

    try:
        tmp_path.replace(path)
    except Exception:
        tmp_path.unlink(missing_ok=True)
        raise

    return path


def _maybe_rewrite(raster_bytes: bytes, *, compression: str, write_as_cog: bool) -> bytes:
    normalized = compression.strip().lower()
    if normalized in ("", "preserve") and not write_as_cog:
        return raster_bytes

    with MemoryFile(raster_bytes) as source_memory_file, source_memory_file.open() as source:
        profile = source.profile.copy()
        if write_as_cog:
            profile["driver"] = "COG"
            source_compression = source.compression.value.lower() if source.compression is not None else "none"
        else:
            source_compression = None

        if normalized in ("", "preserve"):
            profile["compress"] = source_compression
        elif normalized == "none":
            if write_as_cog:
                profile["compress"] = "none"
            else:
                profile.pop("compress", None)
        else:
            profile["compress"] = normalized

        with MemoryFile() as output_memory_file:
            with output_memory_file.open(**profile) as output:
                output.write(source.read())
            return bytes(output_memory_file.read())
