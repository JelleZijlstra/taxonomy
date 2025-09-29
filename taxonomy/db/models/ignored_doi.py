"""DOIs that we do not care to download."""

import enum

from clirm import Field

from .base import BaseModel


class IgnoreReason(enum.IntEnum):
    not_interested = 1
    no_access = 2
    already_have = 3
    invalid = 4


class IgnoredDoi(BaseModel):
    call_sign = "IDOI"
    label_field = "doi"
    reason = Field[IgnoreReason | None](default=None)
    clirm_table_name = "ignored_doi"

    doi = Field[str]()
