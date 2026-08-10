"""Result types shared by model lint producers and consumers."""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .lint import LintFix


@dataclass(frozen=True, slots=True)
class LintIssue:
    """A lint message with an optional stable code and deterministic fix."""

    message: str
    code: str | None = None
    fix: LintFix | None = None

    def __str__(self) -> str:
        return self.message

    def with_code(self, code: str) -> LintIssue:
        if self.code is not None:
            return self
        return replace(self, code=code)

    def with_message(self, message: str) -> LintIssue:
        return replace(self, message=message)


LintResult = str | LintIssue
