"""One check's decision, as stored in metrics.check_results."""
from dataclasses import dataclass, field


@dataclass
class Result:
    table: str
    check: str
    method: str
    severity: str          # block, quarantine, flag, warn or alert
    status: str            # pass or fail
    observed: float | None = None
    expected: str = ""
    partition: str = ""
    detail: dict = field(default_factory=dict)

    @property
    def blocks(self) -> bool:
        return self.status == "fail" and self.severity == "block"
