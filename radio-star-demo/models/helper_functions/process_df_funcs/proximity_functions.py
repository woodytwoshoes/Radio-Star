import numpy as np
import pandas as pd

def find_close_row(df, close_idx, column, value,row_comparator):
    """This function finds the row closest in euclidian space accordingt o
    the close_idx values which meets the requirement of value existing in
    column. Since this will be row_comparator (the original row) it excludes
    that result and searches for the next closest image"

    `The requirement of the value we are seeking is in practice, 
    the 'control' for the comparator image. We are seeking to find, for the 
    comparator, a row which is different in only one respect, value exists in column.'

    Args:
        df (DataFrame): a pandas dataframe.
        close_idx (Dask DataFrame): a pandas dataframe or dask dataframe of
        one column, which represents the results of nan euclidian distance
        calculations, and the index of the original images
        column (string): a string value which represents the name of the 
        column of df which we intend to search.
        value (int): an integer value which represents the value we are 
        seeking to find in column
        row_comparator(pandas Dataframe): the original row which we are 
        seeking to compare to.

    Returns:
        a pandas dataframe series representing the closest row in which the 
        condition is met

    """



    closestidx = close_idx[df[column] == value] \
        .drop(row_comparator.compute().index.values[0], errors = 'ignore') \
        .idxmin()
    row = df.loc[closestidx]
    return row

def return_df_close_rows(df, row_comparator, close_idx):
    """This function concatenates the rows closest in euclidian space
    according the close_idx values which meets the requirements of values
    existing in each column via the find_close_row column.

    In terms of which values it iterates through, it identifies features in
    the comparator row which are mentioned (i.e not nan) and then finds both
    a positive and negative control for that feature. This is an example of
    finding a 'case-control' patient set for each feature in question.

    The resulting dataframe contains, with hierarchical indices,
    the permutations of the row, which can be seen as the minimal set of scans
    that will give both a posititve and negative example for each feature.
    "

    Args:
        df (DataFrame): a pandas dataframe.
        close_idx (Dask DataFrame): a pandas dataframe or dask dataframe of
        one column, which represents the results of nan euclidian distance
        calculations, and the index of the original images
        row_comparator(pandas Dataframe): the original row which we are
        seeking to compare to.

    Returns:
        a pandas dataframe series representing the concatenated closest rows
        according to the conditions laid out above.

    """

    # dict created to print the correct status message in human readable form
    encoded_dict = {1: "POSITIVE ", 0: "UNCERTAIN ", -1: "NEGATIVE ",
                    np.NaN: 'unmentioned'}
    # empty df case-control created with appropriate headers
    df_case_control = pd.DataFrame(columns=row_comparator.columns)
    # cols_hierarchy collects column names for which similar rows are
    # successfully found. This list is used to build the multi-index object
    cols_hierarchy = []
    for column in row_comparator:
        # if the feature is mentioned
        if row_comparator[column].values in (-1, 0, 1):
            # for positive and negative findings
            for value in (-1, 1):
                # attempt to find a similar row
                try:
                    row = find_close_row(df, close_idx, column, value,row_comparator)
                    print('SUCCEEDED on find similar for feature: ', column,
                          encoded_dict[
                        value])
                    df_case_control = pd.concat((df_case_control, row))
                # if no similar row is found, abandon the and begin on a new
                # feature.
                except:
                    print('FAILED to find similar for', column, encoded_dict[
                        value])
                    pass

            cols_hierarchy.append(column)
    # create a multi-index df which we can use to visualize our 'hunt'.
    mi = pd.MultiIndex.from_frame(df_case_control[cols_hierarchy])

    multi_index_images_df = pd.Series(df_case_control.index, index=mi, name = 'Id')

    # since many rows are duplicates in that they represent the case-control
    # for more than one feature, drop them duplicates from the dataframe
    df_case_control = df_case_control.drop_duplicates()

    return df_case_control, multi_index_images_df