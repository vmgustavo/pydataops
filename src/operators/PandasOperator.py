from time import time
from typing import Tuple

import pandas as pd

from .BaseOperator import BaseOperator


class PandasOperator(BaseOperator):
    def __init__(self, paths: Tuple[str, str]):
        BaseOperator.__init__(self, paths=paths)

    @staticmethod
    def _loader(path: str):
        return pd.read_csv(path)

    def groupby(self, dtype: str):
        df0 = self._loader(self.paths[0])

    def join(self, dtype: str):
        df0 = self._loader(self.paths[0])
        df1 = self._loader(self.paths[1])

        st = time()
        df0.merge(df1, on=f"index_{dtype}", how="inner")
        en = time()

        return en - st

    def aggregate(self, dtype: str):
        df0 = self._loader(self.paths[0])

        st = time()
        df0[f"value_{dtype}_0"].aggregate("sum")
        en = time()

        return en - st
