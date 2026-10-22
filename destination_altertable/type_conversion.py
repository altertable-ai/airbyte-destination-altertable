"""
Type conversion utilities for converting Airbyte types to PyArrow types.

See: https://docs.airbyte.com/platform/understanding-airbyte/supported-data-types
"""

import json
from datetime import date, datetime, time
from typing import Any

import pyarrow as pa

# float64 can't represent every integer beyond this magnitude exactly.
MAX_EXACT_FLOAT_INTEGER = 2**53
INT64_RANGE = range(-(2**63), 2**63)


def convert_column_to_arrow(
    values: list[Any], property_schema: dict
) -> tuple[pa.DataType, list[Any]]:
    """Convert the values of a column to a PyArrow type and values.

    `number` columns are float64, except when all their values are integers and some
    can't be represented exactly as float64 (e.g. IDs): those columns are int64.

    Args:
        values: The column values, one per record (None for missing values).
        property_schema: The Airbyte JSON schema for the property.

    Returns:
        A tuple of (PyArrow type, converted values).
    """
    if is_number_column(property_schema):
        return convert_number_column(values)

    sample_value = next((v for v in values if v is not None), None)
    pa_type, _ = convert_to_arrow(sample_value, property_schema)
    return pa_type, [convert_to_arrow(v, property_schema)[1] for v in values]


def is_number_column(property_schema: dict) -> bool:
    """Whether the column type depends on its values (see `convert_number_column`)."""
    return (
        get_json_type(property_schema) == "number"
        and property_schema.get("airbyte_type") != "integer"
    )


def unify_number_columns(batches: list[pa.RecordBatch]) -> list[pa.RecordBatch]:
    """Give each column the same type in all batches.

    Number columns are typed on the values of each batch, so a column can be int64 in a
    batch and float64 in another. It is int64 when all its values are integers within
    the int64 range, as if the batches had been converted together, and float64 otherwise.
    """
    if len({batch.schema for batch in batches}) <= 1:
        return batches

    columns = []
    for i in range(batches[0].num_columns):
        arrays = [batch.column(i) for batch in batches]
        if len({array.type for array in arrays}) > 1:
            try:
                arrays = [array.cast(pa.int64()) for array in arrays]
            except pa.ArrowInvalid:
                arrays = [array.cast(pa.float64(), safe=False) for array in arrays]
        columns.append(arrays)

    names = batches[0].schema.names
    return [
        pa.RecordBatch.from_arrays([arrays[j] for arrays in columns], names=names)
        for j in range(len(batches))
    ]


def convert_number_column(values: list[Any]) -> tuple[pa.DataType, list[Any]]:
    present = [v for v in values if v is not None]
    if all(is_integral(v) and int(v) in INT64_RANGE for v in present) and any(
        type(v) is int and abs(v) > MAX_EXACT_FLOAT_INTEGER for v in present
    ):
        return pa.int64(), [None if v is None else int(v) for v in values]

    # PyArrow rejects integers that float64 can't represent exactly instead of rounding them.
    return pa.float64(), [float(v) if type(v) is int else v for v in values]


def is_integral(value: Any) -> bool:
    return type(value) is int or (isinstance(value, float) and value.is_integer())


def get_json_type(property_schema: dict) -> Any:
    json_type = property_schema.get("type")
    # Handle nullable types (e.g., ["null", "string"])
    if isinstance(json_type, list):
        json_type = next((t for t in json_type if t != "null"), "string")
    return json_type


def convert_to_arrow(value: Any, property_schema: dict) -> tuple[pa.DataType, Any]:
    """Convert an Airbyte value to PyArrow type and value.

    Args:
        value: The value to convert.
        property_schema: The Airbyte JSON schema for the property.

    Returns:
        A tuple of (PyArrow type, converted value).
    """
    json_type = get_json_type(property_schema)
    format_ = property_schema.get("format")
    airbyte_type = property_schema.get("airbyte_type")

    # Date: {"type": "string", "format": "date"}
    if json_type == "string" and format_ == "date":
        converted = parse_date(value)
        return pa.date32(), converted

    # Timestamp: {"type": "string", "format": "date-time"}
    if json_type == "string" and format_ == "date-time":
        converted = parse_timestamp(value)
        if airbyte_type == "timestamp_without_timezone":
            return pa.timestamp("us"), converted
        return pa.timestamp("us", tz="UTC"), converted

    # Time: {"type": "string", "format": "time"}
    if json_type == "string" and format_ == "time":
        converted = parse_time(value)
        return pa.time64("us"), converted

    # String
    if json_type == "string":
        return pa.string(), value

    # Integer
    if json_type == "integer":
        return pa.int64(), value

    # Number
    if json_type == "number":
        if airbyte_type == "integer":
            return pa.int64(), value
        return pa.float64(), value

    # Boolean
    if json_type == "boolean":
        return pa.bool_(), value

    # Default fallback - serialize complex types to JSON
    if isinstance(value, (dict, list)):
        return pa.string(), json.dumps(value)

    return pa.string(), value


def parse_date(value: Any) -> date | None:
    """Parse a date value from Airbyte format.

    Args:
        value: The value to parse. Can be None, a date, datetime, or string.

    Returns:
        A date object or None.
    """
    if value is None:
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    # String format: "2021-01-23"
    return datetime.fromisoformat(str(value).replace(" BC", "")).date()


def parse_timestamp(value: Any) -> datetime | None:
    """Parse a timestamp value from Airbyte format.

    Args:
        value: The value to parse. Can be None, a datetime, or string.

    Returns:
        A datetime object or None.
    """
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    # String format: "2022-11-22T01:23:45.123456+05:00" or "2022-11-22T01:23:45Z"
    cleaned = str(value).replace(" BC", "")
    if cleaned.endswith("Z"):
        cleaned = cleaned[:-1] + "+00:00"
    return datetime.fromisoformat(cleaned)


def parse_time(value: Any) -> time | None:
    """Parse a time value from Airbyte format.

    Args:
        value: The value to parse. Can be None, a time, or string.

    Returns:
        A time object or None.
    """
    if value is None:
        return None
    if isinstance(value, time):
        return value
    # String format: "01:23:45.123456" or "01:23:45.123456+05:00"
    time_str = str(value).split("+")[0].split("Z")[0]
    # Handle negative timezone offset
    if "-" in time_str and time_str.count("-") > 0 and ":" in time_str.split("-")[-1]:
        time_str = "-".join(time_str.split("-")[:-1])
    fmt = "%H:%M:%S.%f" if "." in time_str else "%H:%M:%S"
    return datetime.strptime(time_str, fmt).time()
