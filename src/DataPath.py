from pathlib import Path
from typing import Union
from dataclasses import dataclass


@dataclass
class DataPath:
    directory: str
    rows: int
    groups_num: int
    groups_arg: Union[int, float]

    def primary(self):
        filename = (
            f"primary__rows_{self.rows}"
            + f"__groups_num_{self.groups_num}"
            + f"__groups_arg_{self.format_groups(self.groups_arg)}.csv"
        )
        return Path(self.directory) / filename

    def secondary(self):
        filename = (
            f"secondary__rows_{self.rows}"
            + f"__groups_num_{self.groups_num}"
            + f"__groups_arg_{self.format_groups(self.groups_arg)}.csv"
        )
        return Path(self.directory) / filename

    @staticmethod
    def format_groups(groups: Union[int, float]):
        if float(groups).is_integer():
            return f"{groups:.0f}n"
        else:
            return f"{groups * 100:.0f}p"
