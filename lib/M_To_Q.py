import os
import pathlib as p
import pandas as pd
from datetime import datetime

monthly_folder = 'Time-series Data Monthly'
quarterly_folder = 'Time-series Data'
workingDir = p.Path(__file__).parents[2]


def remove_files(folder):
    for file in os.listdir(folder):
        os.remove(folder + '/' + file)


def check_quarter(dt: datetime) -> bool:
    return dt.month % 3 == 0


def transform_file(path: p.Path):
    file_name = path.name

    def _transform(df) -> pd.DataFrame:
        if df['Date'].map(check_quarter).iloc[-1]:
            return df.query('Date.map(@check_quarter)', engine='python')
        else:
            return pd.concat([df.query('Date.map(@check_quarter)', engine='python'), df.iloc[-1].to_frame().T], ignore_index=True)

    def _save_excel(df) -> pd.DataFrame:
        df.to_excel(workingDir / quarterly_folder / file_name)
        print(f'finished transforming {file_name} from monthly to quarterly.')
        return df
    return (
        pd.read_excel(path)
        .pipe(_transform)
        .assign(Date=lambda df: df['Date'].dt.date)
        .set_index('Date')
        .pipe(_save_excel)
    )


def transform():
    for file in os.listdir(workingDir / monthly_folder):
        if file.endswith('.xlsx'):
            transform_file(workingDir / monthly_folder / file)
