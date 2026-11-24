"""
Type conversion utilities for converting Airbyte types to PyArrow types.

See: https://docs.airbyte.com/platform/understanding-airbyte/supported-data-types
"""

import json
import math
from datetime import date, datetime, time
from decimal import Decimal
from typing import Any

import pyarrow as pa

# Integers beyond this magnitude are not all exactly representable as float64.
MAX_EXACT_FLOAT_INTEGER = 2**53
INT64_MIN = -(2**63)
INT64_MAX = 2**63 - 1
DECIMAL128_MAX_PRECISION = 38


def convert_column_to_arrow(
    values: list[Any], property_schema: dict
) -> tuple[pa.DataType, list[Any]]:
    """Convert all the values of a column to a PyArrow type and values.

    Numeric columns are typed from their values so that no value loses precision:
    `number` stays float64 unless it holds integers float64 can't represent exactly,
    and integers that don't fit int64 are widened to decimal128 then string.

    Args:
        values: The column values, one per record (None for missing values).
        property_schema: The Airbyte JSON schema for the property.

    Returns:
        A tuple of (PyArrow type, converted values).
    """
    json_type = _json_type(property_schema)
    airbyte_type = property_schema.get("airbyte_type")

    if json_type == "integer" or (json_type == "number" and airbyte_type == "integer"):
        return _convert_numeric_column(values, integer=True)
    if json_type == "number":
        return _convert_numeric_column(values, integer=False)

    sample_value = next((v for v in values if v is not None), None)
    pa_type, _ = convert_to_arrow(sample_value, property_schema)
    return pa_type, [convert_to_arrow(v, property_schema)[1] for v in values]


def _json_type(property_schema: dict) -> Any:
    json_type = property_schema.get("type")
    # Handle nullable types (e.g., ["null", "string"])
    if isinstance(json_type, list):
        json_type = next((t for t in json_type if t != "null"), "string")
    return json_type


def _is_integer(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _convert_numeric_column(
    values: list[Any], integer: bool
) -> tuple[pa.DataType, list[Any]]:
    present = [v for v in values if v is not None]

    if not all(isinstance(v, (int, float)) for v in present):
        return _convert_to_string_column(values)

    has_fraction = any(isinstance(v, float) and not v.is_integer() for v in present)
    has_inexact_float_integer = any(
        _is_integer(v) and abs(v) > MAX_EXACT_FLOAT_INTEGER for v in present
    )

    if has_fraction:
        if not has_inexact_float_integer:
            return pa.float64(), values
        return _convert_to_decimal_column(values)

    if not integer and not has_inexact_float_integer:
        return pa.float64(), values

    integers = [None if v is None else int(v) for v in values]
    if all(INT64_MIN <= v <= INT64_MAX for v in integers if v is not None):
        return pa.int64(), integers
    return _convert_to_decimal_column(integers)


def _convert_to_decimal_column(values: list[Any]) -> tuple[pa.DataType, list[Any]]:
    """Convert numbers to a decimal128 column, or a string column if they don't fit."""
    if any(isinstance(v, float) and not math.isfinite(v) for v in values):
        return _convert_to_string_column(values)

    # repr() is the shortest string that round-trips, i.e. the number as the source wrote it.
    decimals = [
        None if v is None else Decimal(repr(v) if isinstance(v, float) else int(v))
        for v in values
    ]
    present = [d.as_tuple() for d in decimals if d is not None]
    scale = max((max(0, -t.exponent) for t in present), default=0)
    integer_digits = max(
        (max(0, len(t.digits) + t.exponent) for t in present), default=0
    )
    if integer_digits + scale > DECIMAL128_MAX_PRECISION:
        return _convert_to_string_column(values)
    return pa.decimal128(DECIMAL128_MAX_PRECISION, scale), decimals


def _convert_to_string_column(values: list[Any]) -> tuple[pa.DataType, list[Any]]:
    def to_string(value: Any) -> str | None:
        if value is None or isinstance(value, str):
            return value
        if isinstance(value, float):
            return repr(value)
        if _is_integer(value):
            return str(value)
        return json.dumps(value)

    return pa.string(), [to_string(v) for v in values]


def convert_to_arrow(value: Any, property_schema: dict) -> tuple[pa.DataType, Any]:
    """Convert an Airbyte value to PyArrow type and value.

    Args:
        value: The value to convert.
        property_schema: The Airbyte JSON schema for the property.

    Returns:
        A tuple of (PyArrow type, converted value).
    """
    json_type = property_schema.get("type")
    format_ = property_schema.get("format")
    airbyte_type = property_schema.get("airbyte_type")

    # Handle nullable types (e.g., ["null", "string"])
    if isinstance(json_type, list):
        json_type = next((t for t in json_type if t != "null"), "string")

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
