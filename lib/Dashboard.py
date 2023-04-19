from pathlib import Path
import json
from .FilesDir import FilesDir
from .Portfolio import Portfolio
from .Portfolio_Total import Portfolio_Total
from .country_exposure_summary import Portfolios_Country, Comparison
import pandas as pd
from . import config


class Dashboard:
    def __init__(self, date_this_qtr: str, date_last_qtr: str, json_dir: Path, dir: FilesDir) -> None:
        self.date_this_qtr = date_this_qtr
        self.date_last_qtr = date_last_qtr
        self.dir = dir
        self.port_data = json.load(open(json_dir, 'r'))
        self.compile_dict = {'fill_in': pd.DataFrame(), 'beta': pd.DataFrame()}

    def loop_portfolios(self) -> None:
        for portfolio_name in self.port_data:
            portfolio = self.portfolio_generator(portfolio_name)
            portfolio.load()
            print(f'finished loading of {portfolio_name}')
            portfolio.transform()
            print(f'finished transforming of {portfolio_name}')
            portfolio.download_check_dfs()
            print(f'finished downloading source data for {portfolio_name}')
            portfolio.download_transform_dfs()
            print(f'finished downloading target data for {portfolio_name}')

    def portfolio_generator(self, port_name) -> Portfolio:
        if port_name == 'ti':
            return Portfolio_Total(
                self.date_this_qtr, self.port_data[port_name], self.compile_dict, self.dir)
        else:
            return Portfolio(
                self.date_this_qtr, self.port_data[port_name], self.compile_dict, self.dir)

    def load_port_country(self, date: str) -> Portfolios_Country:
        port_country = Portfolios_Country(
            date, self.port_data, self.dir.source_dir)
        port_country.load()
        port_country.process()
        port_country.redirect()
        port_country.append()
        return port_country

    def calculate_country_exposure(self) -> None:
        port_this_qtr = self.load_port_country(self.date_this_qtr)
        port_last_qtr = self.load_port_country(self.date_last_qtr)

        comparison = Comparison(
            port_this_qtr, port_last_qtr, self.dir.save_dir)
        comparison.calculate()
        comparison.write_to_csv()

        with open(self.dir.analysis_date_dir, 'w') as f:
            f.write(self.date_this_qtr)

    def process(self) -> None:
        self.loop_portfolios()
        self.calculate_country_exposure()
