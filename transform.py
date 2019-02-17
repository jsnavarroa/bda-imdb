'''
Set of common transformations
'''

import numpy as np
import pandas as pd

def nconst_to_float(series):
    '''
    Transform the nconst column to float.
    The type float64 support NaN values in Pandas.
    '''
    return pd.to_numeric(series.str.replace('nm', ''), downcast='unsigned')


def tconst_to_float(series):
    '''
    Transform the tconst column to float.
    The type float64 support NaN values in Pandas.
    '''
    return pd.to_numeric(series.str.replace('tt', ''), downcast='unsigned')


def expand_rows_using_repeat(df, target_column, separator):
    '''
    Expand the rows of a DataFrame splitting values of the target column using numpy repeat.
    '''
    target_df = df[target_column].str.split(separator)
    lens = [len(item) for item in target_df]

    other_df = df[df.columns.difference([target_column])]

    other_array = np.repeat(other_df.values, lens, axis=0)

    # Put each target element in a row
    target_array = np.concatenate(target_df.values).reshape(-1, 1)

    data = np.concatenate((other_array, target_array), axis=1)

    columns = np.append(other_df.columns.values, target_column)

    final_df = pd.DataFrame(data=data, columns=columns)

    # Preserve original column order
    final_df = final_df[df.columns]

    return final_df
