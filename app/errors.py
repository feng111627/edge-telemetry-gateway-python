"""统一的拒绝原因与异常类型。"""

from __future__ import annotations

from enum import Enum


class RejectReason(str, Enum):
    NOT_A_OBJECT = "not_an_object"
    MISSING_DEVICE_ID = "missing_device_id"
    UNKNOWN_FORMAT = "unknown_format"
    MALFORMED_JSON = "malformed_json"
    MISSING_FIELD = "missing_field"
    BAD_TYPE = "bad_type"
    BAD_TIMESTAMP = "bad_timestamp"
    FUTURE_TIMESTAMP = "future_timestamp"
    EXPIRED_TIMESTAMP = "expired_timestamp"


class IngestError(ValueError):
    """上报无法被归一化时抛出，携带可区分的拒绝原因。"""

    def __init__(self, reason: RejectReason, detail: str = "") -> None:
        self.reason = reason
        self.detail = detail
        super().__init__(f"{reason.value}: {detail}")
