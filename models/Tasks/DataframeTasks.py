import logging
from luigi import Task
from luigi.parameter import BoolParameter
from luigi.task import ExternalTask
import luigi
from csci_utils.luigi.dask.target import CSVTarget
from csci_utils.luigi.dask.target import ParquetTarget
from csci_utils.luigi.task import Requirement
from csci_utils.luigi.task import Requires
from csci_utils.luigi.task import TargetOutput
from luigi.contrib.s3 import S3Target
import pandas as pd

class ChexpertDataframe(ExternalTask):

    s3_path = 's3://radio-star-csci-e-29/unzipped/'

    output = TargetOutput(
        file_pattern="",
        ext="train.csv",
        target_class=S3Target,
        path=s3_path
    )


class ProcessChexpertDf(Task):
    requires = Requires()
    chexpertdf = Requirement(ChexpertDataframe)

    output = TargetOutput(
        target_class=ParquetTarget,
        path="../data/processed/",
        ext="",
        flag=False,
        storage_options=dict(requester_pays=True),
    )

    def run(self):
        pathCSV = self.input()["chexpertdf"].path
        ddf = dd.read_csv(pathCSV)
        self.output().write_dask(ddf, compression="gzip")