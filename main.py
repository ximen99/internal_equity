import json
import lib
import pathlib as p
import pandas as pd
from lib import config
import warnings
warnings.filterwarnings("ignore")


def loop_portfolios():
    json_dir = config.CODE_DIR / "portfolios.json"

    portfolio_data = open(json_dir)
    portfolio_data = json.load(portfolio_data)

    date = '2023-02-28'

    compile_dict = {'fill_in': pd.DataFrame(), 'beta': pd.DataFrame()}

    for portfolio_name in portfolio_data:
        if portfolio_name == 'ti':
            portfolio = lib.Portfolio_Total(
                date, portfolio_data[portfolio_name], compile_dict)
        else:
            portfolio = lib.Portfolio(
                date, portfolio_data[portfolio_name], compile_dict)
        portfolio.load()
        print(f'finished loading of {portfolio_name}')
        portfolio.transform()
        print(f'finished transforming of {portfolio_name}')
        portfolio.download_check_dfs()
        print(f'finished downloading source data for {portfolio_name}')
        portfolio.download_transform_dfs()
        print(f'finished downloading target data for {portfolio_name}')

    for table_name, table in compile_dict.items():
        table.to_csv(config.SAVE_DIR / (table_name+'.csv'), index=False)


def main():
    loop_portfolios()


if __name__ == "__main__":
    main()
