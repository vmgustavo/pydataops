import logging
from time import time
from pathlib import Path
from typing import Tuple

from pyspark import SparkConf
import pyspark.sql.functions as f
from pyspark.sql import SparkSession

from .BaseOperator import BaseOperator


class PySparkOperator(BaseOperator):
    def __init__(self, paths: Tuple[str, str]):
        BaseOperator.__init__(self, paths=paths)
        self.tmp_dir = f"{Path().home()}/.tmp-PySpark"

        logging.getLogger("spark").setLevel(logging.ERROR)
        logging.getLogger("py4j").setLevel(logging.ERROR)

        conf = SparkConf()
        conf.set("spark.local.dir", self.tmp_dir)

        self.spark = SparkSession.builder.config(conf=conf).getOrCreate()
        self.spark.sparkContext.setLogLevel("ERROR")

    def _loader(self, path: str):
        return self.spark.read.csv(path, header=True)

    def groupby(self, dtype: str):
        df0 = self._loader(self.paths[0])

        st = time()
        res = df0.groupby(f"group_{dtype}").agg({"index_int": "count"}).collect()
        en = time()

        return en - st, res

    def join(self, dtype: str):
        df0 = self._loader(self.paths[0])
        df1 = self._loader(self.paths[1])

        st = time()
        res = df0.join(df1, on=f"index_{dtype}", how="inner").collect()
        en = time()

        return en - st, res

    def aggregate(self, dtype: str):
        df0 = self._loader(self.paths[0])

        st = time()
        res = df0.select(f"value_{dtype}_0").agg(f.sum(f"value_{dtype}_0")).collect()
        en = time()

        return en - st, res
