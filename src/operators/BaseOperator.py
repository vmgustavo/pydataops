from typing import Tuple
from abc import ABC, abstractmethod


class BaseOperator(ABC):
    def __init__(self, paths: Tuple[str, str]):
        self.paths = paths
        self.last_result = None

    @staticmethod
    @abstractmethod
    def _loader(path: str):
        raise NotImplementedError

    @abstractmethod
    def groupby(self, dtype: str):
        raise NotImplementedError

    @abstractmethod
    def join(self, dtype: str):
        raise NotImplementedError

    @abstractmethod
    def aggregate(self, dtype: str):
        raise NotImplementedError

    @abstractmethod
    def last_result_aslist(self):
        raise NotImplementedError
