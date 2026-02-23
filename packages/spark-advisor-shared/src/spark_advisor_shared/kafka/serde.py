from __future__ import annotations

import orjson
from pydantic import BaseModel


def serialize_message(model: BaseModel) -> bytes:
    return orjson.dumps(model.model_dump(mode="json"))


def deserialize_message[T: BaseModel](data: bytes, model_class: type[T]) -> T:
    parsed = orjson.loads(data)
    return model_class.model_validate(parsed)
