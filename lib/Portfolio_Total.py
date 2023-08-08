from . import Portfolio
import pandas as pd
from . import Tranform_utils as tu


class Portfolio_Total(Portfolio):

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

        result_df = tu.remove_percentages(result_df, 'Active Share')
        result_df.loc[dummy_row_name,
                      'Portfolio Code'] = self.portfolio_config['portfolio_code']
        return result_df

    # def transform(self) -> None:

    #     self._transform_facs_final()
    #     self._transform_return_decomp_by_factor()
    #     self._transform_return_decomp_by_factor_single()
    #     self._transform_factor_risk()
    #     self._transform_factor_risk_single()
    #     self._transform_risk_summary()
    #     self._transform_industry_attribution()
    #     self._transform_style_attribution()

    #     self._transform_country_attribution()

    #     self._transform_specific()
    #     self._transform_stock_selection()

    #     self._transform_attribution_matrix(tu.attribution_matrix)

    #     # self._transform_top_bottom()
    #     # self._transform_top_ten()
    #     # self._transform_bottom_ten()
    #     self.transform_dfs.append(
    #         self.tables_dict['port_sum']['detail'])
    #     self._transform_sector_positioning()
    #     self.compile_dict['fill_in'] = pd.concat(
    #         [self.compile_dict['fill_in'], self._transform_fill_in()], ignore_index=True)
    #     # self._transform_beta()
    #     self._transform_facs_active_exposure()
    #     self._transform_time_series_return_attr()

    def transform(self) -> None:
        self._transform_facs_final()
        self.transform_dfs.append(
            self.tables_dict['port_sum']['detail'])
        self._transform_sector_positioning()
        self.compile_dict['fill_in'] = pd.concat(
            [self.compile_dict['fill_in'], self._transform_fill_in()], ignore_index=True)
