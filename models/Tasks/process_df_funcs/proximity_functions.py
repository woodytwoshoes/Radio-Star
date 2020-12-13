import numpy as np

def find_close_row(df, close_idx, column, value):
    closestidx = close_idx[df[column] == value] \
        .drop(row_comparator.index.values[0], errors = 'ignore') \
        .idxmin()
    row = df.loc[closestidx]
    return row

def return_df_close_rows(df, row_comparator, close_idx):
    encoded_dict = {1: "POSITIVE ", 0: "UNCERTAIN ", -1: "NEGATIVE ",
                    np.NaN: 'unmentioned'}

    df_case_control = pd.DataFrame(columns=row_comparator.columns)
    cols_hierarchy = []
    for column in row_comparator:
        if row_comparator[column].values in (-1, 0, 1):

            for value in (-1, 1):

                try:
                    row = find_close_row(df, close_idx, column, value)

                    df_case_control = pd.concat((df_case_control, row))

                except:
                    pass
            cols_hierarchy.append(column)

    mi = pd.MultiIndex.from_frame(df_case_control[cols_hierarchy])

    multi_index_images_df = pd.Series(df_case_control.index, index=mi, name = 'Id')

    df_case_control = df_case_control.drop_duplicates()

    return df_case_control, multi_index_images_df