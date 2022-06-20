import csv
from random import random
from typing import List, Tuple
from string import ascii_uppercase

from tqdm import tqdm
from faker import Faker


class CreateData:
    def __init__(self, rows: int, filepaths: Tuple[str, str]):
        self.rows = rows
        self.filepaths = filepaths
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
        # TODO: create columns specifying groups
        # TODO: save data based on the number of rows
        with open(self.filepaths[0], "w") as f0, open(self.filepaths[1], "w") as f1:
            writer0 = csv.writer(f0, dialect="unix", delimiter=",")
            writer0.writerow(
                [
                    "index_int",
                    "index_str",
                    "index_float",
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

            for i in tqdm(self.rows, ncols=80, desc="Generating Random Data"):
                indexes = [i, self._dec2alpha(i), float(i) + random()]

                line0 = indexes.copy()
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
