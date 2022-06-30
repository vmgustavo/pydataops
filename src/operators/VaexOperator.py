from time import time
from typing import Tuple

import vaex

from .BaseOperator import BaseOperator


class VaexOperator(BaseOperator):
    def __init__(self, paths: Tuple[str, str]):
        BaseOperator.__init__(self, paths=paths)

    def _loader(self, path: str):
        return vaex.from_csv(path)

    def groupby(self, dtype: str):
        df0 = self._loader(self.paths[0])

        st = time()
        res = df0.groupby(by=f"group_{dtype}").agg({"index_int": "count"})
        en = time()

        return en - st, res

    def join(self, dtype: str):
        df0 = self._loader(self.paths[0])
        df1 = self._loader(self.paths[1])

        st = time()
        res = df0.join(
            df1,
            on=f"index_{dtype}",
            lsuffix="_0",
            rsuffix="_1",
        )
        en = time()

        return en - st, res

    def aggregate(self, dtype: str):
        df0 = self._loader(self.paths[0])

        st = time()
        res = df0.sum(df0[f"value_{dtype}_0"])
        en = time()

        return en - st, res
