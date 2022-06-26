import os
import shutil
from time import time
from pathlib import Path
from typing import Tuple

import ray
import modin.pandas as pd

from .BaseOperator import BaseOperator


class ModinRayOperator(BaseOperator):
    def __init__(self, paths: Tuple[str, str]):
        BaseOperator.__init__(self, paths=paths)
        os.environ["MODIN_ENGINE"] = "ray"  # Modin will use Ray
        self.tmp_dir = f"{Path().home()}/.tmp-Ray"
        ray.shutdown()
        ray.init(_temp_dir=self.tmp_dir)

    @staticmethod
    def _loader(path: str):
        return pd.read_csv(path)

    def groupby(self, dtype: str):
        df0 = self._loader(self.paths[0])

        st = time()
        df0.groupby(f"group_{dtype}").agg({"index_int": "count"})
        en = time()

        return en - st

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

    def __del__(self):
        try:
            shutil.rmtree(self.tmp_dir)
        except FileNotFoundError:
            pass
