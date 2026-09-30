"""
Massive Ingestion — SDK Serialization.

Converts Massive SDK response models into plain dictionaries so they can be
written to Bronze as JSON. Used by every module in datasets/.
"""

from dataclasses import asdict, is_dataclass
from typing import Any


# ============================================================
# SERIALIZATION
# ============================================================

def serialize_record(record: Any) -> dict:
    """
    Convert a Massive SDK model object into a dictionary.
    """

    if isinstance(record, dict):
        return record

    if is_dataclass(record):
        return asdict(record)

    if hasattr(record, "__dict__"):
        return dict(record.__dict__)

    raise TypeError(
        f"Unsupported Massive API record type: {type(record)}"
    )


def serialize_records(records: list[Any]) -> list[dict]:
    """
    Convert Massive SDK response objects into dictionaries.
    """

    return [
        serialize_record(record)
        for record in records
    ]
