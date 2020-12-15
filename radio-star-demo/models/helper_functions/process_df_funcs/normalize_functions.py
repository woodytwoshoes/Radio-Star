from sklearn.preprocessing import LabelEncoder, normalize
import dask.array as da


def encode_objects_general(ddf, object_cols):
    LE = LabelEncoder()
    for object_col in object_cols:
        ddf[object_col] = da.from_array(LE.fit_transform(ddf[object_col].astype(str)))
    return ddf


def normalize_general(ddf, columns):
    result = ddf.copy()
    for feature_name in columns:
        max_value = ddf[feature_name].max()
        min_value = ddf[feature_name].min()
        result[feature_name] = (
            2 * (ddf[feature_name] - min_value) / (max_value - min_value) - 1
        )
    return result


def normalize_chex(ddf, object_cols):
    ddf = normalize_general(ddf, object_cols)
    ddf = normalize_general(ddf, ["Age"])
    return ddf
