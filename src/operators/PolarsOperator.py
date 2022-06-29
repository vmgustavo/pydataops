from time import time
from typing import Tuple

import polars as pl

from .BaseOperator import BaseOperator


class PolarsOperator(BaseOperator):
    def __init__(self, paths: Tuple[str, str]):
        BaseOperator.__init__(self, paths=paths)

    def _loader(self, path: str):
        return pl.read_csv(path)

    def groupby(self, dtype: str):
        df0 = self._loader(self.paths[0])

        st = time()
        res = df0.groupby(f"group_{dtype}").agg([pl.count("index_int")])
        en = time()

        return en - st, res

    def join(self, dtype: str):
        df0 = self._loader(self.paths[0])
        df1 = self._loader(self.paths[1])

        st = time()
        res = df0.join(df1, on=f"index_{dtype}")
        en = time()

        return en - st, res

    def aggregate(self, dtype: str):
        df0 = self._loader(self.paths[0])

        st = time()
        res = df0.select(f"value_{dtype}_0").sum()
        en = time()

        return en - st, res

    def res_to_csv(self, res, outpath: str):
        return res.astype("str").values.tolist()
