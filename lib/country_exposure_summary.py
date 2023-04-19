from . import Tranform_utils as tu
import pandas as pd
from . import config
from .Table import Table
from pathlib import Path


class Portfolios_Country:
    def __init__(self, date: str, portfolios_config: dict, source_dir: Path) -> None:
        self.date = date
        self.portfolios_config = portfolios_config
        self.tables_dict = {}
        self.global_port = ['gl_es', 'gl_fu', 'gl_aq', 'gl_th', 'gl_em']
        self.source_dir = source_dir

    def load(self) -> None:
        for port in self.global_port:
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
        self.benchmark_group = {'MSCI World ex Canada': {}, 'MSCI EM': {}}
        for port in self.global_port:
            portfolio_name = self.portfolios_config[port]['portfolio_name']
            portfolio_data = self.tables_dict[port]['country_exposure']
            to_update = {portfolio_name: portfolio_data}
            if portfolio_name == 'Global Emerging Markets':
                self.benchmark_group['MSCI EM'].update(to_update)
            else:
                self.benchmark_group['MSCI World ex Canada'].update(to_update)

    def append(self) -> None:
        for i in self.benchmark_group:
            lists = []
            for attribute, value in self.benchmark_group[i].items():
                lists.append(value)
            self.benchmark_group[i]['combined_portfolio'] = pd.concat(
                lists, axis=1)
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
            self.dict[x] = output

    def write_to_csv(self) -> None:
        self.save_dir.mkdir(exist_ok=True)
        for (benchmark, dataframe) in self.dict.items():
            benchmark_str = benchmark.replace(r'/', '_')
            upload_file_dir = self.save_dir / \
                ("Country_" + benchmark_str + ".csv")
            dataframe.to_csv(upload_file_dir)
            print("Finish writing " + "Country_" + benchmark_str + ".csv")
