from collections import OrderedDict
from typing import Any


def _json_transform(obj: Any, from_pattern: str, to_pattern: str) -> Any:
    if isinstance(obj, (dict, OrderedDict)):
        if len(obj.keys()) == 1:
            key: str = list(obj.keys())[0]
            value = obj[key]
            if key.startswith(from_pattern):
                new_key = key.replace(from_pattern, to_pattern, 1)
                return {new_key: _json_transform(value, from_pattern, to_pattern)}

        if "fieldDiscriminator" in obj:
            # Field discriminators are used for unions between primitive types.
            field = obj["fieldDiscriminator"]
            return {field: _json_transform(obj[field], from_pattern, to_pattern)}

        new_obj: Any = {}
        for key, value in obj.items():
            if value is not None:
                new_obj[key] = _json_transform(value, from_pattern, to_pattern)
        return new_obj
    elif isinstance(obj, list):
        new_obj = [_json_transform(item, from_pattern, to_pattern) for item in obj]
        return new_obj
    elif isinstance(obj, bytes):
        return obj.decode()
    return obj


def pre_json_transform(obj: Any) -> Any:
    """Usually called before sending avro-serialized json over to the rest.li server"""
    return _json_transform(
        obj, from_pattern="com.linkedin.pegasus2avro.", to_pattern="com.linkedin."
    )


def post_json_transform(obj: Any) -> Any:
    """Usually called after receiving restli-serialized json before instantiating into avro-generated Python classes"""
    return _json_transform(
        obj, from_pattern="com.linkedin.", to_pattern="com.linkedin.pegasus2avro."
    )
