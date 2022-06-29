import os
from time import time
from typing import Tuple

import modin.pandas as pd

from .BaseOperator import BaseOperator


class ModinDaskOperator(BaseOperator):
    def __init__(self, paths: Tuple[str, str]):
        BaseOperator.__init__(self, paths=paths)
        os.environ["MODIN_ENGINE"] = "dask"  # Modin will use Dask

    @staticmethod
    def _loader(path: str):
        return pd.read_csv(path)

    def groupby(self, dtype: str):
        df0 = self._loader(self.paths[0])

        st = time()
        res = df0.groupby(f"group_{dtype}").agg({"index_int": "count"})
        en = time()

        return en - st, res

    def join(self, dtype: str):
        df0 = self._loader(self.paths[0])
        df1 = self._loader(self.paths[1])

        st = time()
        res = df0.merge(df1, on=f"index_{dtype}", how="inner")
        en = time()

        return en - st, res

    def aggregate(self, dtype: str):
        df0 = self._loader(self.paths[0])

        st = time()
        res = df0[f"value_{dtype}_0"].aggregate("sum")
        en = time()

        return en - st, res
