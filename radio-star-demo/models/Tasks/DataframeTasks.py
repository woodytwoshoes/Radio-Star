import glob
import os
import matplotlib.pyplot as plt
import logging
import pandas as pd
import pathlib
from dask import dataframe as dd
from sklearn.metrics.pairwise import nan_euclidean_distances

from luigi import Task, LocalTarget
from luigi.parameter import IntParameter, FloatParameter
from luigi.task import ExternalTask
from luigi.contrib.s3 import S3Target

from csci_utils.luigi.dask.target import ParquetTarget
from csci_utils.luigi.task import Requirement
from csci_utils.luigi.task import Requires
from csci_utils.luigi.task import TargetOutput


from ..helper_functions.process_image_functions.S3_image_functions import S3Images
from ..helper_functions.process_df_funcs.normalize_functions import (
    encode_objects_general,
    normalize_chex,
)
from ..helper_functions.process_df_funcs.proximity_functions import (
    return_df_close_rows,
)


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
        ddf = ddf.drop(columns=["Path"])
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
    fractional_search = FloatParameter(default=0.2)

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

        object_cols = ddf_raw.dtypes[(ddf_raw.dtypes == object).values]

        row_comparator = ddf.loc[self.comparator_index]

        # This is a cheap calculation to do as it only requires an equality
        # and a summation (I estimate that it's on the order N_samples *
        # N_features complexity. I used it to remain within the dask
        # dataframe in terms of memory, and identify key indices which I can
        # use as a subset to make a more expensive calculation below.

        most_similar_idx = (
            (ddf.values == row_comparator.values)
            .sum(axis=1)
            .astype("int")
            .compute()
            .argsort()[::-1]
        )

        # In preparation for euclidian distances, I select a likely subset of
        # the data

        idx_partition_to_view = most_similar_idx[
            : int(most_similar_idx.size * self.fractional_search)
        ]

        # Here we set our indices to sample only 1/5 of the dataset,
        # that which is closest to our images. And go to pandas for the
        # euclidian distance calculation

        df = ddf.loc[idx_partition_to_view].compute()

        # Another possible method here is k-nearest neighbours. However,
        # this is difficult to implement with dask

        dist_in_space = nan_euclidean_distances(
            df.fillna(0), df.loc[self.comparator_index].fillna(0).values.reshape(1, -1)
        )

        close_idx = pd.DataFrame(data=dist_in_space, index=df.index)

        logging.info("retrieved list of close indices")

        row_comparator_raw = ddf_raw.loc[self.comparator_index].compute()

        logging.info("df shape is " + str(df.shape[0]) + "," + str(df.shape[1]))

        # the limitation of using df_close_rows is that, if it cannot find a
        # row which meets the condition within the idx_partition_to_view (our
        # subset), then it returns 'fail'. I believe this is a compromise
        # rather than committing a huge amount of compute power to find
        # distant neighbours within the full dataset.

        df_close_rows, mi_df = return_df_close_rows(df, row_comparator, close_idx)

        print("original feature values are: ")
        print(row_comparator_raw)

        print("close image ids are: ")
        print(mi_df)

        ddf_raw.loc[list(df_close_rows.index)].to_parquet(
            self.output().path + str("/Similar.parquet")
        )

        self.output().write_dask(
            ddf_raw.loc[list(df_close_rows.index)], compression="gzip"
        )


class ChexpertDataBucket(ExternalTask):

    s3_path = "s3://radio-star-csci-e-29/"

    output = TargetOutput(file_pattern="", ext="", target_class=S3Target, path=s3_path)


class PullSimilarImages(Task):
    """The Dataframe is best normalized before similarity calculations are
    run on it."""

    requires = Requires()
    find_similar = Requirement(FindSimilar)
    chexpert_data_images = Requirement(ChexpertDataBucket)

    def output(self):
        return LocalTarget("../data/processed/images_plot.jpg")

    def run(self):
        s3_bucket_path = ChexpertDataBucket().output().path
        bucket = str(pathlib.Path(pathlib.Path(s3_bucket_path).parts[1]))
        images = S3Images()

        simil_dir_path = self.input()["find_similar"].path
        simil_path = glob.glob(os.path.join(simil_dir_path, "*.parquet"))[0]
        df_simil = pd.read_parquet(simil_path)
        s3_parent_dir = pathlib.PurePosixPath(self.input()["chexpert_data_images"].path)

        fig = plt.figure(figsize=(10, 10))
        i = 1
        for index, row in df_simil.iterrows():
            l = df_simil.shape[0]
            rel_path = pathlib.PurePosixPath(*pathlib.Path(row["Path"]).parts[1:])
            rel_path = pathlib.PurePosixPath("unzipped") / rel_path

            s3_img_path = s3_parent_dir / rel_path
            logging.info("key/path is: ", s3_img_path)
            logging.info("bucket is: ", bucket)
            logging.info("key is: ", rel_path)
            image = images.from_s3(bucket=bucket, key=str(rel_path))
            fig.add_subplot(int(l / 3) + 1, 3, i).imshow(image, cmap="bone")

            i += 1

        plt.show()
        with self.output().temporary_path() as f:
            plt.savefig(f, format="jpg")
        print("similar images SUCCESSFULLY displayed and saved")
