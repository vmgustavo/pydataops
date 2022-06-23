import csv
import logging
from random import choice, random
from string import ascii_uppercase

from tqdm import tqdm
from faker import Faker

from .DataPath import DataPath


class CreateData:
    def __init__(self, rows: int, groups: int, datadir: str):
        self.logger = logging.getLogger(__name__)
        self.rows = rows
        self.groups = groups
        self.datadir = datadir
        self.fkr = Faker(["en_US"])

    @staticmethod
    def _dec2alpha(i: int, length: int = 10) -> str:
        """Convert a decimal number to its base alphabet representation
        Source: https://codereview.stackexchange.com/a/182757
        """
        a_upper = ord("A")
        nletters = len(ascii_uppercase)

        def _decompose(number):
            """Generate digits from `number` in base alphabet, least significants
            bits first.
            """

            while number:
                number, remainder = divmod(number, nletters)
                yield remainder

        res = "".join(chr(a_upper + part) for part in _decompose(i))[::-1]

        return "A" * (length - len(res)) + res

    def gen(self) -> None:
        filepaths = (
            DataPath(self.datadir, self.rows, self.groups).primary(),
            DataPath(self.datadir, self.rows, self.groups).secondary(),
        )

        with open(filepaths[0], "w") as f0, open(filepaths[1], "w") as f1:
            writer0 = csv.writer(f0, dialect="unix", delimiter=",")
            writer0.writerow(
                [
                    "index_int",
                    "index_str",
                    "index_float",
                    "group_int",
                    "group_str",
                    "group_float",
                    "value_bool_0",
                    "value_float_0",
                    "value_int_0",
                    "value_str_name",
                    "value_str_color",
                    "value_str_android",
                ]
            )
            writer1 = csv.writer(f1, dialect="unix", delimiter=",")
            writer1.writerow(
                [
                    "index_int",
                    "index_str",
                    "index_float",
                    "value_bool_1",
                    "value_float_1",
                    "value_int_1",
                ]
            )

            for i in tqdm(range(self.rows), desc="Generating Random Data"):
                indexes = [i, self._dec2alpha(i), float(i) + random()]
                i_group = choice(range(self.groups))
                groups = [i_group, self._dec2alpha(i_group), float(i_group) + random()]

                line0 = indexes.copy()
                line0.extend(groups)
                line0.extend(
                    [
                        self.fkr.boolean(),
                        self.fkr.pyfloat(right_digits=3, min_value=-100, max_value=100),
                        self.fkr.pyint(min_value=0, max_value=100, step=1),
                        self.fkr.name(),
                        self.fkr.safe_color_name(),
                        self.fkr.android_platform_token(),
                    ]
                )
                writer0.writerow(line0)

                line1 = indexes.copy()
                line1.extend(
                    [
                        self.fkr.boolean(),
                        self.fkr.pyfloat(right_digits=3, min_value=-100, max_value=100),
                        self.fkr.pyint(min_value=0, max_value=100, step=1),
                    ]
                )
                writer1.writerow(line1)
