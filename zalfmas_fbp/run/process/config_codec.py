from __future__ import annotations

import json
import tomllib
from typing import TYPE_CHECKING, Any, TypeGuard

import capnp
from mas.schema.common import common_capnp

from .errors import ProcessConfigError
from .types import ConfigScalar, ConfigValue

if TYPE_CHECKING:
    from mas.schema.common.common_capnp.types.builders import PairBuilder, ValueBuilder
    from mas.schema.common.common_capnp.types.enums import StructuredTextTypeEnum
    from mas.schema.common.common_capnp.types.readers import ValueReader
    from mas.schema.fbp.fbp_capnp.types.readers import IPReader


def is_config_list[T: ConfigScalar](
    value: list[ConfigValue],
    item_type: type[T],
    *,
    exact: bool = False,
) -> TypeGuard[list[T]]:
    if exact:
        return all(type(item) is item_type for item in value)
    return all(isinstance(item, item_type) for item in value)


def is_config_value_list(value: object) -> TypeGuard[list[ConfigValue]]:
    return isinstance(value, list)


def is_object_dict(value: object) -> TypeGuard[dict[object, object]]:
    return isinstance(value, dict)


def is_str_key_dict(value: dict[object, object]) -> TypeGuard[dict[str, object]]:
    return all(isinstance(key, str) for key in value)


def config_value_from_python(value: object) -> ValueBuilder:
    if isinstance(value, str):
        return common_capnp.Value.new_message(t=value)
    if isinstance(value, bool):
        return common_capnp.Value.new_message(b=value)
    if isinstance(value, int):
        return common_capnp.Value.new_message(i64=value)
    if isinstance(value, float):
        return common_capnp.Value.new_message(f64=value)
    if is_config_value_list(value):
        if not value:
            return common_capnp.Value.new_message(lv=[])
        if is_config_list(value, int, exact=True):
            return common_capnp.Value.new_message(li64=value)
        if is_config_list(value, float):
            return common_capnp.Value.new_message(lf64=value)
        if is_config_list(value, bool):
            return common_capnp.Value.new_message(lb=value)
        if is_config_list(value, str):
            return common_capnp.Value.new_message(lt=value)
        values = [config_value_from_python(item) for item in value]
        return common_capnp.Value.new_message(lv=values)
    if is_object_dict(value):
        if not is_str_key_dict(value):
            raise TypeError("Config dict keys must be strings")
        pairs: list[PairBuilder] = []
        for key, item in value.items():
            pairs.append(
                common_capnp.Pair.new_message(
                    fst=key,
                    snd=config_value_from_python(item),
                ),
            )
        return common_capnp.Value.new_message(lpair=pairs)

    raise TypeError(f"Unsupported config value type: {type(value).__name__}")


def python_value_from_capnp_value(value: ValueReader) -> ConfigValue:
    value_type = value.which()
    if value_type == "i64":
        return value.i64
    if value_type == "f64":
        return value.f64
    if value_type == "b":
        return value.b
    if value_type == "t":
        return value.t
    if value_type == "li64":
        return list(value.li64)
    if value_type == "lf64":
        return list(value.lf64)
    if value_type == "lb":
        return list(value.lb)
    if value_type == "lt":
        return list(value.lt)
    if value_type == "lv":
        return [python_value_from_capnp_value(v) for v in value.lv]
    if value_type == "lpair":
        try:
            d: dict[str, ConfigValue] = {}
            for pair in value.lpair:
                if not pair._has("fst") or not pair._has("snd"):
                    raise TypeError("Pair entries must have both 'fst' and 'snd'")
                d[pair.fst.as_text()] = python_value_from_capnp_value(
                    pair.snd.as_struct(common_capnp.Value),
                )
            return d
        except (AttributeError, capnp.KjException, TypeError) as e:
            raise TypeError(f"Error unpacking dict (list of pairs): {e}") from e
    raise TypeError(f"Unsupported config value type: {value_type}")


def load_config_text(text: str, config_type: StructuredTextTypeEnum) -> dict[str, Any]:
    normalized_type = config_type or "toml"
    if isinstance(normalized_type, str):
        normalized_type = normalized_type.lower()
    if normalized_type == "toml":
        return dict(tomllib.loads(text))
    if normalized_type == "json":
        config = json.loads(text)
        if type(config) is not dict:
            raise TypeError("JSON config must decode to an object")
        return config
    raise ValueError(f"Unsupported config text type: {config_type}")


def load_unstructured_config_text(text: str) -> dict[str, Any]:
    try:
        return load_config_text(text, "toml")
    except tomllib.TOMLDecodeError as toml_error:
        try:
            return load_config_text(text, "json")
        except (json.JSONDecodeError, TypeError) as json_error:
            raise ValueError(f"Config text is neither valid TOML nor JSON: {toml_error}; {json_error}") from json_error


def config_from_ip(in_msg: IPReader) -> dict[str, ConfigValue]:
    try:
        try:
            structured_text = in_msg.content.as_struct(common_capnp.StructuredText)
        except capnp.KjException:
            return load_unstructured_config_text(in_msg.content.as_text())
        return load_config_text(structured_text.value, structured_text.type)
    except (capnp.KjException, json.JSONDecodeError, tomllib.TOMLDecodeError, TypeError, ValueError) as e:
        raise ProcessConfigError(str(e)) from e
