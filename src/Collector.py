import os
import json
from typing import Optional
from datetime import datetime
from dataclasses import asdict, dataclass


@dataclass()
class EvalData:
    library: str
    operation: str
    col_dtype: str
    dataset_p: str
    dataset_s: Optional[str]
    time: Optional[float]
    exception: Optional[str]

    def filename(self):
        return "_".join([self.library, self.operation, self.col_dtype])


class Collector:
    def __init__(self, dirpath: str = os.path.join("data", "execs")):
        if not os.path.exists(dirpath):
            os.makedirs(dirpath, exist_ok=True)

        self.dirpath = dirpath

    def save(self, record: EvalData):
        time = datetime.now().isoformat()
        filepath = os.path.join(self.dirpath, f"{record.filename()}__{time}.json")
        with open(filepath, "w") as f:
            json.dump(asdict(record), f, indent=2)
