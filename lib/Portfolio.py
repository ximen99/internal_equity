from .Table import Table
import pandas as pd
from . import Tranform_utils as tu
from . import FilesDir
from . import config


class Portfolio:
    def __init__(self, date: str, portfolio_config: dict, compile_dict: dict, dir: FilesDir) -> None:
        self.portfolio_config = portfolio_config
        self.date = date
        self.tables_dict = {}
        self.check_dfs = []
        self.transform_dfs = []
        self.compile_dict = compile_dict
        self.dir = dir

    def _load_single_table(self, table_name: str) -> None:
        portfolio_code = self.portfolio_config['portfolio_code']
        table_config = self.portfolio_config['tables'][table_name]
        portfolio_prefix = self.portfolio_config['portfolio_prefix']
        table = Table(self.dir.source_dir, self.date,
                      portfolio_code, portfolio_prefix, table_config, table_name)            
        try:
            table.load()
        except Exception as e:
            print(f"An error occurred: {e}. Skipping load.")

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
        for df_dict in self.check_dfs:
            df_dir = self.dir.check_dir / df_dict['save_to_name']
            df_dict['df'].to_csv(df_dir, index=False)

    def _transform_facs_final(self) -> None:
        df = self.tables_dict['facs']['detail']['df']
        file_dir = self.dir.time_series_dir / \
            self.portfolio_config['time_series']
        result_df = tu.add_time_series(df, self.date, file_dir)
        file_name = self.portfolio_config['portfolio_prefix'] + '_' \
            'facs_final.csv'
        self.transform_dfs.append({'df': result_df, 'save_to_name': file_name})
        self.transform_dfs.append({'df': df.drop(
            columns='Asset ID').set_index('Asset Name'), 'save_to_name': self.tables_dict['facs']['detail']['save_to_name']})

    def _transform_return_decomp_by_factor(self) -> None:
        df = self.tables_dict['factors_return_decomp_by_factor']['detail']['df']
        result_df = (
            df
            .pipe(tu.daily_to_monthly, date_col=" ")
            .pipe(tu.return_decomposition)
        )
        # made changes to source data so update table dict
        self.tables_dict['factors_return_decomp_by_factor']['detail']['df'] = result_df
        self.transform_dfs.append(
            self.tables_dict['factors_return_decomp_by_factor']['detail'])

    def _transform_return_decomp_by_factor_single(self) -> None:
        df = self.tables_dict['factors_return_decomp_by_factor']['detail']['df']
        result_df = (
            df
            .iloc[-1]
            .to_frame()
            .filter(regex='.*Period$', axis=0)
            .T
            .pipe(tu.remove_substring_from_columns, [' Period'])
            .T
            .squeeze()
        )
        result_df.name = 'Contribution'
        file_name = self.portfolio_config['portfolio_prefix'] + '_' \
            'factors_return_decomp_by_factor_single.csv'
        self.transform_dfs.append({'df': result_df, 'save_to_name': file_name})

    def _transform_factor_risk(self) -> None:
        result_df = (
            self.tables_dict['factors_risk']['detail']['df']
            .pipe(tu.daily_to_monthly, date_col=" ")
            .pipe(tu.risk_decomposition)
        )
        self.tables_dict['factors_risk']['detail']['df'] = result_df
        self.transform_dfs.append(
            self.tables_dict['factors_risk']['detail'])

    def _transform_factor_risk_single(self) -> None:
        result_df = self.tables_dict['factors_risk']['detail']['df'].iloc[-1]
        result_df.name = 'Contribution'
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
        result_df = tu.factor_attribution(
            factor_df, risk_df, 'GICS Sector', '/Industry')
        file_name = self.portfolio_config['portfolio_prefix'] + '_' \
            'industry_attribution.csv'
        self.transform_dfs.append({'df': result_df, 'save_to_name': file_name})

    def _transform_style_attribution(self) -> None:
        factor_df = self.tables_dict['style_factor']['detail']['df']
        risk_df = self.tables_dict['style_risk']['detail']['df']
        result_df = tu.factor_attribution(
            factor_df, risk_df, 'Factor', '/Risk Indices')
        file_name = self.portfolio_config['portfolio_prefix'] + \
            '_' + 'style_attribution.csv'
        self.transform_dfs.append({'df': result_df, 'save_to_name': file_name})

    def _transform_country_attribution(self) -> None:
        factor_df = self.tables_dict['country_factor']['detail']['df']
        risk_df = self.tables_dict['country_risk']['detail']['df']
        result_df = tu.factor_attribution(
            factor_df, risk_df, 'Country', '/Country')
        file_name = self.portfolio_config['portfolio_prefix'] + \
            '_' + 'country_attribution.csv'
        self.transform_dfs.append({'df': result_df, 'save_to_name': file_name})

    def _transform_specific(self) -> None:
        df: pd.DataFrame = self.tables_dict['specific']['detail']['df']
        result_df = (
            df
            .query("`Asset Name`.str.contains('Total') == False", engine="python")
            .pipe(tu.remove_percentages, 'Average Portfolio Weight')
            .assign(**{'GICS Sector Name': lambda df_: df_['GICS Sector Name'].fillna('Cash & Others')})
        )
        self.tables_dict['specific']['detail']['df'] = result_df
        self.transform_dfs.append(self.tables_dict['specific']['detail'])

    def _transform_stock_selection(self) -> None:
        df = self.tables_dict['specific']['detail']['df']
        result_df = tu.stock_selection(df)
        file_name = self.portfolio_config['portfolio_prefix'] + \
            '_' + 'stock_selection.csv'
        self.transform_dfs.append({'df': result_df, 'save_to_name': file_name})

    def _transform_attribution_matrix(self, attrubution_matrix) -> None:
        df = self.tables_dict['specific']['detail']['df']
        result_df = attrubution_matrix(df)
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

    def _transform_bottom_ten(self) -> None:
        df: pd.DataFrame = self.tables_dict['factors_top_bottom_by_factor']['transform']['df']
        result_df = (
            df
            .pipe(tu.top_bottom, 'FYTD Bottom 10')
            .nsmallest(n=10, columns=['Total'])
        )
        file_name = self.portfolio_config['portfolio_prefix'] + \
            '_' + 'bottom_ten.csv'
        self.transform_dfs.append({'df': result_df, 'save_to_name': file_name})

    def _transform_fill_in(self) -> pd.DataFrame:
        dummy_row_name = '0'
        result_df = pd.DataFrame()

        result_df.loc[dummy_row_name,
                      'Porfolio Name'] = self.portfolio_config['portfolio_name']

        summary_df = (
            self.tables_dict['port_sum']['detail']['df']
            .set_index('Client Name:')
        )
        # add values to result df template
        for r in ['Active Share', 'Active Risk', 'Predicted Beta']:
            col_name = 'British Columbia Investment Management Corporation (bcIMC)'
            val = summary_df.loc[r, col_name]
            if r == 'Predicted Beta':
                result_df.loc[dummy_row_name, r] = float(val)
            else:
                result_df.loc[dummy_row_name, r] = val

        return_df = (
            self.tables_dict['as_return_table']['detail']['df']
            .set_index('Source of Return')
            .pipe(tu.remove_substring_from_columns, ['Net', 'Cumulative', '[', '] ', 'Full Time-span'])
        )
        # add values to result df template
        for r in ['Allocation', 'Selection', 'Currency']:
            if r in return_df.index:
                result_df.loc[dummy_row_name, r] = return_df.loc[r, 'Return']
            else:
                result_df.loc[dummy_row_name, r] = 0

        result_df = tu.remove_percentages(result_df, 'Active Share')
        result_df.loc[dummy_row_name,
                      'Portfolio Code'] = self.portfolio_config['portfolio_code']

        return result_df

    def _transform_sector_positioning(self) -> None:
        df: pd.DataFrame = self.tables_dict['sector_positioning']['detail']['df']
        result_df = (
            df
            .set_index('Name')
            .pipe(tu.remove_percentages)
        )
       # update time series file
        ts_portfolios = ['tem', 'wasatch', 'shroders', 'gqg', 'arga']
        # update time series file
        if self.portfolio_config['portfolio_prefix'] in ts_portfolios:
            file_dir = self.dir.time_series_dir / \
                f'{self.portfolio_config["portfolio_prefix"]}_sector_active_weight.csv'
            tu.add_time_series_sector_weight(self.date, file_dir, result_df)

        self.tables_dict['sector_positioning']['detail']['df'] = result_df
        self.transform_dfs.append(
            self.tables_dict['sector_positioning']['detail'])

    def _transform_beta(self) -> None:
        compile_df: pd.DataFrame = self.compile_dict['beta']
        df: pd.DataFrame = self.tables_dict['beta']['detail']['df'].set_index(
            ' ')
        compile_df.loc['beta', self.portfolio_config['portfolio_name']
                       ] = df.loc['Mean', 'Net Period Predicted Beta']

    def _transform_facs_active_exposure(self) -> None:
        self.tables_dict['facs_activeExp']['detail']['df'] = self.tables_dict['facs_activeExp']['detail']['df'].drop(
            columns='Asset ID').set_index('Asset Name')
        self.transform_dfs.append(
            self.tables_dict['facs_activeExp']['detail'])

    def _transform_time_series_return_attr(self) -> None:
        df = self.tables_dict['time_series_return_attr']['detail']['df']
        result_df = (
            df
            .pipe(tu.remove_substring_from_columns, ['Net', 'Period', '[', '] '])
            .rename(columns={' ': 'Date'})
            .query("Date.str.contains('Mean|Std Dev|Min|Max') == False", engine="python")
            .pipe(tu.remove_percentages, 'Benchmark')
            .set_index('Date')
        )
        self.tables_dict['time_series_return_attr']['detail']['df'] = result_df
        self.transform_dfs.append(
            self.tables_dict['time_series_return_attr']['detail'])

    def _need_country_attribution(self) -> bool:
        port = self.portfolio_config['portfolio_prefix']
        for ls in config.GLOBAL_BENCHMARK_MAPPING.values():
            if port in ls:
                return True
        return False

    def transform(self) -> None:
        self._transform_facs_final()
        self._transform_return_decomp_by_factor()
        self._transform_return_decomp_by_factor_single()
        self._transform_factor_risk()
        self._transform_factor_risk_single()
        self._transform_risk_summary()
        self._transform_industry_attribution()
        self._transform_style_attribution()

        if self._need_country_attribution():
            self._transform_country_attribution()

        self._transform_specific()
        self._transform_stock_selection()

        if self.portfolio_config['portfolio_prefix'] in ['ca_ae', 'ca_sc', 'ca_aq']:
            self._transform_attribution_matrix(tu.attribution_matrix_canadian)
        else:
            self._transform_attribution_matrix(tu.attribution_matrix)

        self._transform_top_bottom()
        self._transform_top_ten()
        self._transform_bottom_ten()
        self.transform_dfs.append(
            self.tables_dict['port_sum']['detail'])
        self._transform_sector_positioning()
        self.compile_dict['fill_in'] = pd.concat(
            [self.compile_dict['fill_in'], self._transform_fill_in()], ignore_index=True)
        self._transform_beta()
        self._transform_facs_active_exposure()
        self._transform_time_series_return_attr()

    def download_transform_dfs(self) -> None:
        for df_dict in self.transform_dfs:
            df_dir = self.dir.save_dir / df_dict['save_to_name']
            df_dict['df'].to_csv(df_dir)
