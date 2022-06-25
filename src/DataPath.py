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

    @classmethod
    def from_str(cls, s: str):
        import re

        pattern = re.compile(pattern=r"rows_(\d+)__groups_num_(\d+)__groups_arg_(\d+)(\w)")

        cls.directory = s.split("/")[0]
        cls.rows, cls.groups_num, cls.groups_arg, arg = re.search(pattern, s).groups()
        cls.rows = int(cls.rows)
        cls.groups_num = int(cls.groups_num)

        if arg == "p":
            cls.groups_arg = int(cls.groups_arg) / 100
        elif arg == "n":
            cls.groups_arg = int(cls.groups_arg)

        return cls
