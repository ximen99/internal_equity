from .Table import Table
import pathlib as p
import pandas as pd
from . import Tranform_utils as tu


class Portfolio:
    def __init__(self, sourceDir: str, time_series_folder: str, date: str, portfolio_config: dict) -> None:
        self.workingDir = p.Path(__file__).parents[2]
        self.portfolio_config = portfolio_config
        self.sourceDir = sourceDir
        self.date = date
        self.tables_dict = {}
        self.time_series_folder = time_series_folder
        self.check_dfs = []
        self.transform_dfs = []
        self.save_to_folder = 'Python Data'
        # create save to folder
        save_to_dir = self.workingDir / self.save_to_folder
        save_to_dir.mkdir(exist_ok=True)

    def _load_single_table(self, table_name: str) -> None:
        portfolio_code = self.portfolio_config['portfolio_code']
        table_config = self.portfolio_config['tables'][table_name]
        portfolio_prefix = self.portfolio_config['portfolio_prefix']
        table = Table(self.sourceDir, self.date,
                      portfolio_code, portfolio_prefix, table_config, table_name)
        table.load()
        self.tables_dict[table_name] = table.table_dict

    def load(self) -> None:
        for table_name in self.portfolio_config['tables']:
            self._load_single_table(table_name)

    def _update_check_list(self) -> None:
        for table_name in self.portfolio_config['tables']:
            for sub_table, sub_table_dict in self.tables_dict[table_name].items():
                if sub_table in ['detail', 'summary']:
                    self.check_dfs.append(sub_table_dict)

    def download_check_dfs(self) -> None:
        self._update_check_list()
        check_folder = 'check'
        for df_dict in self.check_dfs:
            download_dir = self.workingDir / check_folder
            download_dir.mkdir(exist_ok=True)
            df_dir = download_dir / df_dict['save_to_name']
            df_dict['df'].to_csv(df_dir, index=False)

    def _transform_facs_final(self) -> None:
        df = self.tables_dict['facs']['detail']['df']
        file_dir = self.workingDir / self.time_series_folder / 'FaCS Global Thematic.xlsx'
        result_df = tu.add_time_series(df, self.date, file_dir)
        file_name = self.portfolio_config['portfolio_prefix'] + '_' \
            'facs_final.csv'
        self.transform_dfs.append({'df': result_df, 'save_to_name': file_name})

    def _transform_return_decomp_by_factor(self) -> None:
        df = self.tables_dict['factors_return_decomp_by_factor']['detail']['df']
        result_df = tu.return_decomposition(df)
        # made changes to source data so update table dict
        self.tables_dict['factors_return_decomp_by_factor']['detail']['df'] = result_df
        self.transform_dfs.append(
            self.tables_dict['factors_return_decomp_by_factor']['detail'])

    def _transform_return_decomp_by_factor_single(self) -> None:
        df = self.tables_dict['factors_return_decomp_by_factor']['detail']['df']
        result_df = df.iloc[-1]
        result_df.name = 'Contribution'
        file_name = self.portfolio_config['portfolio_prefix'] + '_' \
            'factors_return_decomp_by_factor_single.csv'
        self.transform_dfs.append({'df': result_df, 'save_to_name': file_name})

    def _transform_factor_risk(self) -> None:
        df = self.tables_dict['factors_risk']['detail']['df']
        result_df = tu.risk_decomposition(df)
        self.tables_dict['factors_risk']['detail']['df'] = result_df
        self.transform_dfs.append(
            self.tables_dict['factors_risk']['detail'])

    def _transform_factor_risk_single(self) -> None:
        result_df = self.tables_dict['factors_risk']['detail']['df'].iloc[-1]
        file_name = self.portfolio_config['portfolio_prefix'] + '_' \
            'factors_risk_single.csv'
        self.transform_dfs.append({'df': result_df, 'save_to_name': file_name})

    def _transform_risk_summary(self) -> None:
        df = self.tables_dict['factors_return_attribution']['detail']['df']
        result_df = (
            df
            .query("`Source of Return` in ['Total Managed', 'Total Benchmark', 'Total Active', 'Country', 'Industry', 'Risk Indices', 'Specific', 'Currency', 'World']")
            .filter(['Source of Return', '[Full Time-span] [Cumulative] [Net] Risk'], axis=1)
            .set_index('Source of Return')
        )
        file_name = self.portfolio_config['portfolio_prefix'] + '_' \
            'risk_summary.csv'
        self.transform_dfs.append({'df': result_df, 'save_to_name': file_name})

    def _transform_industry_attribution(self) -> None:
        factor_df = self.tables_dict['industry_factor']['detail']['df']
        risk_df = self.tables_dict['industry_risk']['detail']['df']
        result_df = tu.industry_attribution(factor_df, risk_df)
        file_name = self.portfolio_config['portfolio_prefix'] + '_' \
            'industry_attribution.csv'
        self.transform_dfs.append({'df': result_df, 'save_to_name': file_name})

    def _transform_style_attribution(self) -> None:
        factor_df = self.tables_dict['style_factor']['detail']['df']
        risk_df = self.tables_dict['style_risk']['detail']['df']
        result_df = tu.style_attribution(factor_df, risk_df)
        file_name = self.portfolio_config['portfolio_prefix'] + \
            '_' + 'style_attribution.csv'
        self.transform_dfs.append({'df': result_df, 'save_to_name': file_name})

    def _transform_specific(self) -> None:
        df: pd.DataFrame = self.tables_dict['specific']['detail']['df']
        result_df = (
            df
            .query("`Asset Name`.str.contains('Total') == False", engine="python")
            .pipe(tu.remove_percentages, 'Average Portfolio Weight')
            .assign(**{'GICS Sector Name': lambda df_: df_['GICS Sector Name'].fillna('Cash')})
        )
        self.tables_dict['specific']['detail']['df'] = result_df
        self.transform_dfs.append(self.tables_dict['specific']['detail'])

    def _transform_stock_selection(self) -> None:
        df = self.tables_dict['specific']['detail']['df']
        result_df = tu.stock_selection(df)
        file_name = self.portfolio_config['portfolio_prefix'] + \
            '_' + 'stock_selection.csv'
        self.transform_dfs.append({'df': result_df, 'save_to_name': file_name})

    def _transform_attribution_matrix(self) -> None:
        df = self.tables_dict['specific']['detail']['df']
        result_df = tu.attribution_matrix(df)
        file_name = self.portfolio_config['portfolio_prefix'] + \
            '_' + 'attribution_matrix.csv'
        self.transform_dfs.append({'df': result_df, 'save_to_name': file_name})

    def _transform_top_bottom(self) -> None:
        df = self.tables_dict['factors_top_bottom_by_factor']['detail']['df']
        result_df = (
            df
            .query("`Asset Name`.str.contains('Total') == False", engine="python")
            .pipe(tu.remove_percentages, 'Average Active Weight')
        )
        # initialize transform sub table dict
        self.tables_dict['factors_top_bottom_by_factor']['transform'] = {}
        self.tables_dict['factors_top_bottom_by_factor']['transform']['df'] = result_df

    def _transform_top_ten(self) -> None:
        df = self.tables_dict['factors_top_bottom_by_factor']['transform']['df']
        result_df = (
            df
            .pipe(tu.top_bottom, 'FYTD Top 10')
            .nlargest(n=10, columns=['Total'])
        )
        file_name = self.portfolio_config['portfolio_prefix'] + \
            '_' + 'top_ten.csv'
        self.transform_dfs.append({'df': result_df, 'save_to_name': file_name})

    def _tranform_bottom_ten(self) -> None:
        df: pd.DataFrame = self.tables_dict['factors_top_bottom_by_factor']['transform']['df']
        result_df = (
            df
            .pipe(tu.top_bottom, 'FYTD Top 10')
            .nlargest(n=10, columns=['Total'])
        )
        file_name = self.portfolio_config['portfolio_prefix'] + \
            '_' + 'bottom_ten.csv'
        self.transform_dfs.append({'df': result_df, 'save_to_name': file_name})

    def transform(self) -> None:
        self._transform_facs_final()
        self._transform_return_decomp_by_factor()
        self._transform_return_decomp_by_factor_single()
        self._transform_factor_risk()
        self._transform_factor_risk_single()
        self._transform_risk_summary()
        self._transform_industry_attribution()
        self._transform_style_attribution()
        self._transform_specific()
        self._transform_stock_selection()
        self._transform_attribution_matrix()
        self._transform_top_bottom()
        self._transform_top_ten()
        self._tranform_bottom_ten()
        # load port_sum table to output tables
        self.transform_dfs.append(self.tables_dict['port_sum']['detail'])

    def download_transform_dfs(self) -> None:
        output_folder = 'Python Data'
        for df_dict in self.transform_dfs:
            download_dir = self.workingDir / output_folder
            download_dir.mkdir(exist_ok=True)
            df_dir = download_dir / df_dict['save_to_name']
            df_dict['df'].to_csv(df_dir)
