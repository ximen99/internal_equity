import json
import lib
import pathlib as p
import pandas as pd
from lib import config
import warnings
warnings.filterwarnings("ignore")

json_dir = config.CODE_DIR / "portfolios.json"

portfolio_data = open(json_dir)
portfolio_data = json.load(portfolio_data)

date_this_qtr = '2023-02-28'
date_last_qtr = '2022-11-30'


def loop_portfolios():

    compile_dict = {'fill_in': pd.DataFrame(), 'beta': pd.DataFrame()}

    for portfolio_name in portfolio_data:
        if portfolio_name == 'ti':
            portfolio = lib.Portfolio_Total(
                date_this_qtr, portfolio_data[portfolio_name], compile_dict)
        else:
            portfolio = lib.Portfolio(
                date_this_qtr, portfolio_data[portfolio_name], compile_dict)
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


def calculate_country_exposure():

    def load_portfolio(date):
        portfolio = lib.Portfolios_Country(date, portfolio_data)
        portfolio.load()
        portfolio.process()
        portfolio.redirect()
        portfolio.append()
        return portfolio

    port_this_qtr = load_portfolio(date_this_qtr)
    port_last_qtr = load_portfolio(date_last_qtr)

    comparison = lib.Comparison(port_this_qtr, port_last_qtr)
    comparison.calculate()
    comparison.write_to_csv()

    with open(config.ANALYSIS_DATE_DIR, 'w') as f:
        f.write(date_this_qtr)


def main():
    loop_portfolios()
    calculate_country_exposure()


if __name__ == "__main__":
    main()
