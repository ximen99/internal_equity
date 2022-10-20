# %%
import json
import lib
import pathlib as p
import warnings
warnings.filterwarnings("ignore")


def loop_portfolios():
    code_folder = p.Path(__file__).parents[0]
    json_dir = code_folder / "portfolios.json"

    portfolio_data = open(json_dir)
    portfolio_data = json.load(portfolio_data)

    date = '2022-09-30'

    source_folder = 'Download Extracted'
    time_series_folder = 'Time-series Data'

    for portfolio_name in portfolio_data:
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


def main():
    loop_portfolios()


if __name__ == "__main__":
    main()

# %%
