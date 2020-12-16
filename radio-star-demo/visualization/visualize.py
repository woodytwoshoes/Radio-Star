# -*- coding: utf-8 -*-
"""visualize the data in chexpert dataset.

This script demonstrates basic visualization of the chexpert images. It is an implemention of a set of plots and displayed images which I used to explain the project

Example:
    After the luigi pipeline has been run to achieve the following parquet files

    raw_df_path = '../data/processed/ProcessChexpertDfToParquet/part.0.parquet'
    normalized_df_path = '../data/processed/NormalizeDF/part.0.parquet'

        $ python visualize.py

will create a PCA scatterplot of the data contained therein.

Furthermore, the functions here can be imported to quickly view individual
images within the dataset via the python interpreter.

"""


import logging

logging.disable(logging.INFO)
import pandas as pd
import numpy as np
from sklearn.preprocessing import MinMaxScaler
from PIL import Image
import os
import matplotlib.pyplot as plt


encoded_dict = {1: "POSITIVE ", 0: "UNCERTAIN ", -1: "NEGATIVE ", np.NaN: "unmentioned"}
plt.style.use("default")


def show_image(idx):
    image_path = os.path.join("../data/raw/", raw_df.loc[idx]["Path"])
    im = Image.open(image_path)
    return im


def show_image_and_feats(idx):
    plt.figure(figsize=(20, 10))
    plt.imshow(show_image(idx), cmap="bone")
    print(pd.DataFrame(raw_df.loc[idx].drop("Path")).T.replace(to_replace=encoded_dict))
    plt.show()


def show_image_and_complement(idx, complementary_feat):
    row = raw_df.loc[idx]
    idx_complement = (
        (raw_df[raw_df[complementary_feat] == 0 - row[complementary_feat]] == row)
        .sum(axis=1)
        .idxmax()
    )

    # features as series
    feats_original = raw_df.loc[idx].drop("Path")
    feats_complement = raw_df.loc[idx_complement].drop("Path")
    comparison_table = pd.concat((feats_original, feats_complement), axis=1).T

    # images
    img_A = show_image(idx)
    img_B = show_image(idx_complement)

    # plot
    fig, ax = plt.subplots(1, 2, figsize=(30, 15))
    ax[0].imshow(img_A, cmap="bone")
    ax[0].set_title(
        str(encoded_dict[row[complementary_feat]])
        + str(complementary_feat)
        + " "
        + "Pt ID: "
        + str(idx)
        + "\n"
    )

    ax[1].imshow(img_B, cmap="bone")
    ax[1].set_title(
        str(encoded_dict[0 - row[complementary_feat]])
        + str(complementary_feat)
        + " "
        + "Pt ID: "
        + str(idx_complement)
        + "\n"
    )

    return comparison_table.replace(to_replace=encoded_dict)


raw_df_path = "../data/processed/ProcessChexpertDfToParquet/part.0.parquet"
normalized_df_path = "../data/processed/NormalizeDF/part.0.parquet"

raw_df = pd.read_parquet(raw_df_path)
normalized_df = pd.read_parquet(normalized_df_path)

special_1_idx = 78414

sample_df = normalized_df.replace([np.inf, -np.inf], np.nan).fillna(-1)

from sklearn.decomposition import PCA

X = PCA(n_components=2).fit_transform(sample_df)

c = MinMaxScaler().fit_transform((sample_df == True).sum(axis=1).values.reshape(-1, 1))

plt.figure(figsize=(16, 16), dpi=80)
plt.scatter(X[:, 0], X[:, 1], c=c, cmap="Reds")

plt.show()
