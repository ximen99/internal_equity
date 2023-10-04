from pathlib import Path
from datetime import datetime, timedelta
import re
import pandas as pd
from typing import List
import lib
import json


# BASE_PATH = Path(
#     r'S:\ISR\Branch - Investment Risk\Risk Reporting\All Dashboards Working\BPM External Dashboard\Auto\Time-series Data\archive\facs vs bmk')

# PORT_CODES = ['MSCI EM ext_ptf']
# RE_DATE_FORMAT = '(\d{4}-\d{2}-\d{2})'
# RE_PORT_CODE = '(' + '|'.join(PORT_CODES) + ')'
# data_df_dict = {}


# def remove_substring_from_columns(data: pd.DataFrame, patterns: List[str]) -> pd.DataFrame:
#     for i in patterns:
#         data.columns = data.columns.str.replace(i, '')
#     return data


# for port_code in PORT_CODES:
#     data_df_dict[port_code] = pd.DataFrame()

# # use regex to extract portfolio codes from the file names
# for dir in BASE_PATH.iterdir():
#     port_code = re.search(RE_PORT_CODE, dir.name).group(0)
#     date = re.search(RE_DATE_FORMAT, dir.name).group(0)
#     for file in dir.iterdir():
#         if 'Facs exposure vs BMK' in file.name:
#             df = (
#                 pd.read_csv(file, skiprows=11)
#                 .pipe(remove_substring_from_columns, ['FaCS ', ' - Active'])
#                 .query('`Asset Name`.isnull()', engine='python')
#                 .filter(['Growth', 'Liquidity', 'Momentum', 'Quality', 'Size', 'Value', 'Volatility', 'Yield'], axis=1)
#                 .assign(Date=date)
#                 .set_index("Date")
#             )
#             data_df_dict[port_code] = data_df_dict[port_code].append(df)

# for port_code, df in data_df_dict.items():
#     df.to_csv(BASE_PATH / f'{port_code}.csv')


base_path = Path(
    r'S:\ISR\Branch - Investment Risk\Risk Reporting\All Dashboards Working\BPM External Dashboard\2023')


def month_end_iterator(start_date: str, end_date: str):
    date = datetime.strptime(start_date, '%Y-%m-%d')
    while date <= datetime.strptime(end_date, '%Y-%m-%d'):
        if date.month == 12:
            next_date = date.replace(
                year=date.year+1, month=1, day=1) - timedelta(days=1)
        else:
            next_date = date.replace(
                month=date.month+1, day=1) - timedelta(days=1)
        yield next_date
        date = next_date + timedelta(days=1)


port_data = json.load(open(lib.config.EXTERNAL_JSON_DIR, 'r'))
time_series_data = {}
for date in month_end_iterator('2023-06-30', '2023-08-31'):
    pth = base_path / date.strftime('%Y%m%d') / 'Working' / 'Python Data'
    for prefix in ['tem']:
        df = pd.read_csv(pth / f'{prefix}_sector_positioning.csv')
        df = df[['Name', 'Active Exposure']]
        df.columns = ['Name', date.strftime('%Y-%m-%d')]
        df = df.set_index('Name')
        df = df.T
        if prefix in time_series_data:
            time_series_data[prefix] = time_series_data[prefix].append(df)
        else:
            time_series_data[prefix] = df

for prefix, df in time_series_data.items():
    df.to_csv(base_path / f'{prefix}_sector_positioning.csv')
