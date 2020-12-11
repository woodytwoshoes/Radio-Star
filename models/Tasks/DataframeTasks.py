import logging
from luigi import Task
from luigi.parameter import BoolParameter, IntParameter
from luigi.task import ExternalTask
from luigi.target import Target
import luigi
from csci_utils.luigi.dask.target import CSVTarget
from csci_utils.luigi.dask.target import ParquetTarget
from csci_utils.luigi.task import Requirement
from csci_utils.luigi.task import Requires
from csci_utils.luigi.task import TargetOutput
from luigi.contrib.s3 import S3Target
import pandas as pd
import pandas as pd
import numpy as np
import pathlib
from dask import dataframe as dd
from sklearn.metrics.pairwise import cosine_similarity, nan_euclidean_distances
from sklearn.preprocessing import LabelEncoder, normalize
import dask.array as da
import glob
import matplotlib;

from .normalize_functions import encode_objects_general, normalize_chex


class ChexpertDataframe(ExternalTask):

    s3_path = "s3://radio-star-csci-e-29/unzipped/"

    output = TargetOutput(
        file_pattern="", ext="train.csv", target_class=S3Target, path=s3_path
    )


class ProcessChexpertDfToParquet(Task):
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



class NormalizeDF(Task):
    """The Dataframe is best normalized before similarity calculations are
    run on it."""


    requires = Requires()
    proc_chexpertdf = Requirement(ProcessChexpertDfToParquet)

    output = TargetOutput(
        target_class=ParquetTarget,
        path="../data/processed/",
        ext="",
        flag=False,
        storage_options=dict(requester_pays=True),
    )

    def run(self):
        ddf = self.input()["proc_chexpertdf"].read_dask()
        ddf_raw = ddf.copy()
        ddf = ddf.drop(columns=['Path'])
        object_cols = ddf.dtypes[(ddf.dtypes == object)].index.values

        ddf = encode_objects_general(ddf, object_cols)

        ddf = normalize_chex(ddf, object_cols)

        self.output().write_dask(ddf, compression="gzip")


class FindSimilar(Task):
    """The Dataframe is best normalized before similarity calculations are
    run on it."""

    requires = Requires()
    proc_chexpertdf = Requirement(ProcessChexpertDfToParquet)
    normalize_df = Requirement(NormalizeDF)
    comparator_index = IntParameter(default=78414)
    n_images = IntParameter(default=5)

    output = TargetOutput(
        target_class=ParquetTarget,
        path="../data/processed/",
        ext="",
        flag=False,
        storage_options=dict(requester_pays=True),
    )

    def run(self):
        ddf = self.input()["normalize_df"].read_dask()
        ddf_raw = self.input()["proc_chexpertdf"].read_dask()

        object_cols = ddf.dtypes[(ddf.dtypes == object)].index.values

        row_comparator_raw = ddf.loc[self.comparator_index]

        # This compensate for a bug in dask row equality calculations
        row_comparator_na = row_comparator_raw.isna().compute().iloc[0]

        similar_features_idx = (ddf.isna() == row_comparator_na).sum(
            1).compute().nlargest(n=100).index

        argsorted = nan_euclidean_distances(
            row_comparator_raw.compute().values.reshape(1, -1),
            ddf.loc[similar_features_idx.to_list()].compute().values).argsort()

        top_n = similar_features_idx[argsorted][0][:self.n_images]

        top_n_close_images = ddf_raw.loc[top_n]

        self.output().write_dask(top_n_close_images, compression="gzip")

class ChexpertDataBucket(ExternalTask):

    s3_path = 's3://radio-star-csci-e-29/'

    output = TargetOutput(
        file_pattern="",
        ext="",
        target_class=S3Target,
        path=s3_path
    )

class PullSimilarImages(Task):
    """The Dataframe is best normalized before similarity calculations are
    run on it."""

    requires = Requires()
    find_similar = Requirement(FindSimilar)
    chexpert_data_images = Requirement(ChexpertDataBucket)

    output = TargetOutput(
        target_class=Target,
        path="../data/processed/",
        ext="")

    def run(self):
        simil_dir_path = self.input()['find_similar'].path
        simil_path = glob.glob(os.path.join(simil_dir_path, '*.parquet'))[0]
        df = pd.read_parquet(simil_path)
        s3_parent_dir = self.input()['chexpert_data_images'].path
        for index, row in df_simil.iterrows():
            rel_path = pathlib.Path(*pathlib.Path(row['Path']).parts[2:])
            s3_img_path = os.path.join(s3_parent_dir, rel_path)
