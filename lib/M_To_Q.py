import os
import pathlib as p
import pandas as pd

monthly_folder = 'Time-series Data Monthly'
quarterly_folder = 'Time-series Data'
workingDir = p.Path(__file__).parents[2]


def remove_files(folder):
    for file in os.listdir(folder):
        os.remove(folder + '/' + file)


def transform_file(path: p.Path):
    file_name = path.name
    df = pd.read_csv(path, index_col=0)
    df_last = df.iloc[-1]
    return df


def transform():
    # remove_files(workingDir / quarterly_folder)
    return transform_file(workingDir / monthly_folder / 'FaCS Canadian Active.xlsx')
