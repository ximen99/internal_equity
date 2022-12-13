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

    def _transform(df) -> pd.DataFrame:
        if df['Date'].dt.is_quarter_end.iloc[-1]:
            return df.query('Date.dt.is_quarter_end', engine='python')
        else:
            return pd.concat([df.query('Date.dt.is_quarter_end', engine='python'), df.iloc[-1].to_frame().T], ignore_index=True)

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
