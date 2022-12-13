# %%
import json
import lib
import pathlib as p
import pandas as pd
import warnings
warnings.filterwarnings("ignore")


def loop_portfolios():
    code_folder = p.Path(__file__).parents[0]
    json_dir = code_folder / "portfolios.json"

    portfolio_data = open(json_dir)
    portfolio_data = json.load(portfolio_data)

    date = '2022-11-30'

    source_folder = 'DE'
    time_series_folder = 'Time-series Data Monthly'
    save_to_folder = 'Python Data'

    compile_dict = {'fill_in': pd.DataFrame(), 'beta': pd.DataFrame()}

    for portfolio_name in portfolio_data:
        portfolio = lib.Portfolio(
            source_folder, time_series_folder, save_to_folder, date, portfolio_data[portfolio_name], compile_dict)
        portfolio.load()
        print(f'finished loading of {portfolio_name}')
        portfolio.transform()
        print(f'finished transforming of {portfolio_name}')
        portfolio.download_check_dfs()
        print(f'finished downloading source data for {portfolio_name}')
        portfolio.download_transform_dfs()
        print(f'finished downloading target data for {portfolio_name}')

    for table_name, table in compile_dict.items():
        table.to_csv(code_folder.parent / save_to_folder /
                     (table_name+'.csv'), index=False)


def main():
    loop_portfolios()


if __name__ == "__main__":
    main()

# %%
