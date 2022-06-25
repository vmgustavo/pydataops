from time import time
from typing import Tuple

import pyspark.sql.functions as f
from pyspark.sql import SparkSession

from .BaseOperator import BaseOperator


class PySparkOperator(BaseOperator):
    def __init__(self, paths: Tuple[str, str]):
        BaseOperator.__init__(self, paths=paths)

    def _loader(self, path: str):
        spark = SparkSession.builder.getOrCreate()
        return spark.read.csv(path, header=True)

    def groupby(self, dtype: str):
        df0 = self._loader(self.paths[0])

        st = time()
        df0.groupby(f"group_{dtype}").agg({"index_int": "count"}).collect()
        en = time()

        return en - st

    def join(self, dtype: str):
        df0 = self._loader(self.paths[0])
        df1 = self._loader(self.paths[1])

        st = time()
        df0.join(df1, on=f"index_{dtype}", how="inner").collect()
        en = time()

        return en - st

    def aggregate(self, dtype: str):
        df0 = self._loader(self.paths[0])

        st = time()
        df0.select(f"value_{dtype}_0").agg(f.sum(f"value_{dtype}_0")).collect()
        en = time()

        return en - st
