"""Data-quality result types. Pure Python, so the CLI can import them without Spark."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from enum import StrEnum


class Severity(StrEnum):
    ERROR = "error"
    WARN = "warn"


@dataclass(frozen=True)
class CheckResult:
    table: str
    check: str
    severity: Severity
    passed: bool
    violations: int
    details: str


class DataQualityError(RuntimeError):
    def __init__(self, failures: Sequence[CheckResult]) -> None:
        self.failures = list(failures)
        names = ", ".join(f"{f.table}:{f.check} ({f.violations})" for f in self.failures)
        super().__init__(f"{len(self.failures)} data-quality check(s) failed: {names}")
