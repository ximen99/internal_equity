from pathlib import Path
import re
import pandas as pd
from typing import List


BASE_PATH = Path(
    r'S:\ISR\Branch - Investment Risk\Risk Reporting\All Dashboards Working\BPM External Dashboard\Auto\Time-series Data\archive\facs vs bmk')

PORT_CODES = ['MSCI EM ext_ptf']
RE_DATE_FORMAT = '(\d{4}-\d{2}-\d{2})'
RE_PORT_CODE = '(' + '|'.join(PORT_CODES) + ')'
data_df_dict = {}


def remove_substring_from_columns(data: pd.DataFrame, patterns: List[str]) -> pd.DataFrame:
    for i in patterns:
        data.columns = data.columns.str.replace(i, '')
    return data


for port_code in PORT_CODES:
    data_df_dict[port_code] = pd.DataFrame()

# use regex to extract portfolio codes from the file names
for dir in BASE_PATH.iterdir():
    port_code = re.search(RE_PORT_CODE, dir.name).group(0)
    date = re.search(RE_DATE_FORMAT, dir.name).group(0)
    for file in dir.iterdir():
        if 'Facs exposure vs BMK' in file.name:
            df = (
                pd.read_csv(file, skiprows=11)
                .pipe(remove_substring_from_columns, ['FaCS ', ' - Active'])
                .query('`Asset Name`.isnull()', engine='python')
                .filter(['Growth', 'Liquidity', 'Momentum', 'Quality', 'Size', 'Value', 'Volatility', 'Yield'], axis=1)
                .assign(Date=date)
                .set_index("Date")
            )
            data_df_dict[port_code] = data_df_dict[port_code].append(df)

for port_code, df in data_df_dict.items():
    df.to_csv(BASE_PATH / f'{port_code}.csv')
