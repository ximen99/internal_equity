from . import Tranform_utils as tu
import pandas as pd
import numpy as np
from . import config
from .Table import Table
from pathlib import Path


class Portfolios_Country:
    def __init__(self, date: str, portfolios_config: dict, source_dir: Path) -> None:
        self.date = date
        self.portfolios_config = portfolios_config
        self.tables_dict = {}
        self.source_dir = source_dir
        self.benchmark_group = {}
        self.port_list = []
        self.init_update()

    def init_update(self) -> list:
        for port in self.portfolios_config:
            benchmark = self.get_benchmark(port)
            if benchmark:
                self.port_list.append(port)
                self.benchmark_group[benchmark] = {}

    def load(self) -> None:
        for port in self.port_list:
            portfolio_code = self.portfolios_config[port]['portfolio_code']
            portfolio_prefix = self.portfolios_config[port]['portfolio_prefix']
            table_name = 'country_exposure'
            table_config = self.portfolios_config[port]['tables'][table_name]
            table = Table(self.source_dir, self.date,
                          portfolio_code, portfolio_prefix, table_config, table_name)
            table.load()
            self.tables_dict[port] = table.table_dict['detail']
            print("finish loading country exposure for " +
                  self.portfolios_config[port]['portfolio_name'])

    def process(self) -> None:
        for port in self.tables_dict:
            df = self.tables_dict[port]['df']
            self.tables_dict[port]['country_exposure'] = (
                tu
                .country_exposure(df)
                .rename(columns={'Active Weight (%)': self.portfolios_config[port]['portfolio_name']})
            )
            print("finish calculating Country Active Weight of " +
                  self.portfolios_config[port]['portfolio_name'])

    def redirect(self) -> None:
        for port in self.port_list:
            portfolio_name = self.portfolios_config[port]['portfolio_name']
            portfolio_data = self.tables_dict[port]['country_exposure']
            to_update = {portfolio_name: portfolio_data}
            benchmark = self.get_benchmark(port)
            self.benchmark_group[benchmark].update(to_update)

    def get_benchmark(self, port):
        for benchmark in config.GLOBAL_BENCHMARK_MAPPING:
            if port in config.GLOBAL_BENCHMARK_MAPPING[benchmark]:
                return benchmark

    def append(self) -> None:
        for i in self.benchmark_group:
            lists = []
            for attribute, value in self.benchmark_group[i].items():
                lists.append(value)
            df = pd.concat(lists, axis=1)
            # sort index in specific order
            df = df.reindex(config.COUNTRY_EXPOSURE_ORDER, axis=0, level=0)
            self.benchmark_group[i]['combined_portfolio'] = df
            print("finish compiling benchmark " + attribute)


class Comparison:
    def __init__(self, port_this_qtr: dict, port_last_qtr: dict, save_dir: Path):
        self.port_this_qtr = port_this_qtr
        self.port_last_qtr = port_last_qtr
        self.dict = {}
        self.save_dir = save_dir

    def calculate(self) -> None:
        for (x, y) in zip(self.port_this_qtr.benchmark_group, self.port_last_qtr.benchmark_group):
            a = self.port_this_qtr.benchmark_group[x]['combined_portfolio']
            b = self.port_last_qtr.benchmark_group[y]['combined_portfolio']
            delta = (a
                     .subtract(b, fill_value=0)
                     .add_suffix(' Change')
                     )
            output = (
                pd
                .concat([a, delta], axis=1)
                .fillna(0)
            )
            output = output.reindex(sorted(output.columns), axis=1)
            totals = output.groupby(level=0).sum()
            totals.index = pd.MultiIndex.from_product(
                [totals.index, ['Total']], names=['Region', 'Country'])
            output = (
                pd.concat([output, totals])
                .sort_index(key=lambda x: x.map({'Total': 0}).fillna(1), level=1)
                .sort_index(level=0, sort_remaining=False)
                .reindex(config.COUNTRY_EXPOSURE_ORDER, axis=0, level=0)
                .reset_index()
                .assign(Country=lambda x: np.where(x['Country'] == 'Total', x['Region'], x['Country']))
            )
            self.dict[x] = output

    def write_to_csv(self) -> None:
        self.save_dir.mkdir(exist_ok=True)
        for (benchmark, dataframe) in self.dict.items():
            benchmark_str = benchmark.replace(r'/', '_')
            upload_file_dir = self.save_dir / \
                ("Country_" + benchmark_str + ".csv")
            dataframe.to_csv(upload_file_dir, index=False)
            print("Finish writing " + "Country_" + benchmark_str + ".csv")
