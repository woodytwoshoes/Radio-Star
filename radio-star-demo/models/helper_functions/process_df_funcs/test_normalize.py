import unittest
import pandas as pd
import numpy as np
from dask import dataframe as dd
from .normalize_functions import (
    encode_objects_general,
    normalize_general,
    normalize_chex,
)


class NormalizeTests(unittest.TestCase):
    def test_encode_objects_general(self):

        strings_feats = "alpha bravo charlie delta echo".split(" ")
        strings_feats_rev = strings_feats[::-1]
        int_feats = list(range(len(strings_feats)))
        colnames = "A B C".split(" ")
        data_dict = {
            colname: data
            for colname, data in zip(
                colnames, (strings_feats, strings_feats_rev, int_feats)
            )
        }
        sequence = np.arange(0, 5)
        test_array = np.column_stack((sequence.T, sequence[::-1].T, sequence.T))


        with self.subTest('Pandas test'):
            mock_df = pd.DataFrame.from_dict(data=data_dict)
            encoded_df = encode_objects_general(mock_df, "A B".split(" "))
            df_test_groundtruth = pd.DataFrame(
                test_array,
                columns=colnames,
            )
            self.assertTrue(encoded_df.eq(df_test_groundtruth).all(axis=None))
        with self.subTest('Dask test'):
            mock_df = dd.from_pandas(pd.DataFrame.from_dict(data=data_dict),
                                     npartitions = 1)
            encoded_df = encode_objects_general(mock_df, "A B".split(" "))
            df_test_groundtruth = dd.from_array(
                test_array,
                columns=colnames,
            )
            self.assertTrue(encoded_df.eq(df_test_groundtruth).compute().all(
                axis=None))

    def test_normalize_general(self):

        sequence = np.arange(0, 5)
        test_array = np.column_stack((sequence.T, sequence[::-1].T, sequence.T))
        colnames = "A B C".split(" ")
        gt_sequence = np.arange(-1, 1.5, 0.5)
        gt_array = np.column_stack((gt_sequence.T, gt_sequence[::-1].T, gt_sequence.T))
        with self.subTest('Pandas test'):
            df = pd.DataFrame(
                test_array,
                columns=colnames,
            )
            df_norm = normalize_general(df, colnames)
            gt_df = pd.DataFrame(
                gt_array,
                columns=colnames,
            )
            self.assertTrue(df_norm.eq(gt_df).all(
                axis=None))
        with self.subTest('Dask test'):
            df = dd.from_array(
                test_array,
                columns=colnames,
            )
            df_norm = normalize_general(df, colnames)
            gt_df = dd.from_array(
                gt_array,
                columns=colnames,
            )
            self.assertTrue(df_norm.eq(gt_df).compute().all(
                axis=None))


if __name__ == "__main__":
    unittest.main()
