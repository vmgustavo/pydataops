from pathlib import Path
from dataclasses import dataclass


@dataclass
class DataPath:
    directory: str
    rows: int
    groups: int

    def primary(self):
        return Path(self.directory) / f"primary__rows_{self.rows}__groups_{self.groups}.csv"

    def secondary(self):
        return Path(self.directory) / f"secondary__rows_{self.rows}__groups_{self.groups}.csv"
