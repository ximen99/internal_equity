# %%
import json
import pandas as pd
import lib
import pathlib as p
import warnings
warnings.filterwarnings("ignore")


code_folder = p.Path(__file__).parents[0]
json_dir = code_folder / "portfolios.json"

portfolio_data = open(json_dir)
portfolio_data = json.load(portfolio_data)

date = '2022-08-31'

source_folder = 'Download Extracted'
time_series_folder = 'Time-series Data'

# for portfolio in portfolio_data
for portfolio_name in portfolio_data:
    if portfolio_name in ['ca_ae', 'ca_sc', 'ca_aq']:
        portfolio = lib.Portfolio_CA(
            source_folder, time_series_folder, date, portfolio_data[portfolio_name])
    else:
        portfolio = lib.Portfolio(
            source_folder, time_series_folder, date, portfolio_data[portfolio_name])
    portfolio.load()
    print(f'finished loading of {portfolio_name}')
    portfolio.transform()
    print(f'finished transforming of {portfolio_name}')
    portfolio.download_check_dfs()
    print(f'finished downloading source data for {portfolio_name}')
    portfolio.download_transform_dfs()
    print(f'finished downloading target data for {portfolio_name}')

# %%
