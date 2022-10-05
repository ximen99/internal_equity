from . import Tranform_utils as tu
from . import Portfolio


class Portfolio_CA(Portfolio):
    def __init__(self, sourceDir: str, time_series_folder: str, date: str, portfolio_config: dict) -> None:
        super().__init__(sourceDir, time_series_folder, date, portfolio_config)

    def _transform_attribution_matrix(self) -> None:
        df = self.tables_dict['specific']['detail']['df']
        result_df = tu.attribution_matrix_canadian(df)
        file_name = self.portfolio_config['portfolio_prefix'] + \
            '_' + 'attribution_matrix.csv'
        self.transform_dfs.append({'df': result_df, 'save_to_name': file_name})
