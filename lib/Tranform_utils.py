from typing import List
import pandas as pd
import numpy as np
import pathlib as p
from . import config
from datetime import date


def remove_substring_from_columns(data: pd.DataFrame, patterns: List[str]) -> pd.DataFrame:
    for i in patterns:
        data.columns = data.columns.str.replace(i, '')
    return data


def add_time_series(data: pd.DataFrame, date: str, file_dir: p.Path) -> pd.DataFrame:
    df = (
        remove_substring_from_columns(data, ['FaCS ', ' - Active'])
        .query('`Asset Name`.isnull()', engine='python')
        .filter(['Growth', 'Liquidity', 'Momentum', 'Quality', 'Size', 'Value', 'Volatility', 'Yield'], axis=1)
        .assign(Date=date)
        .set_index("Date")
    )
    time_series_data_updated = time_series(
        file_dir, df)
    return time_series_data_updated


def time_series(file_dir: p.Path, dataframe_to_append: pd.DataFrame) -> pd.DataFrame:
    time_series_df = (
        pd.read_excel(file_dir)
        .assign(Date=lambda df_: pd.to_datetime(df_.Date).dt.date)
    )
    dataframe_to_append = (
        dataframe_to_append
        .reset_index()
        .assign(Date=lambda df_: pd.to_datetime(df_.Date).dt.date)
    )
    new_dates = dataframe_to_append['Date'].tolist()
    if time_series_df['Date'].isin(new_dates).any() == False:
        appended_data = time_series_df.append(
            dataframe_to_append).set_index(['Date'])
    else:
        appended_data = (
            time_series_df
            .query('Date not in @new_dates')
            .append(dataframe_to_append)
            .sort_values('Date', ascending=True)
            .set_index(['Date'])
        )
    appended_data.to_excel(file_dir)
    return appended_data


def add_time_series_sector_weight(date: str, file_dir: p.Path, df_to_append: pd.DataFrame) -> pd.DataFrame:
    series_to_append = df_to_append['Avg Act Weight'].rename(date)
    df_time_series = pd.read_csv(file_dir, index_col=0)
    if date in df_time_series.index:
        df_time_series = df_time_series.drop(date)
    df_time_series = df_time_series.append(
        series_to_append).sort_index(ascending=True)
    df_time_series.to_csv(file_dir)


def remove_percentages(data: pd.DataFrame, column_start: str = None) -> pd.DataFrame:
    for i in data.loc[:, column_start:]:
        if data[i].dtypes == 'object':
            data[i] = data[i].str.rstrip('%').astype('float') / 100.0
    return data


def return_decomposition(data: pd.DataFrame) -> pd.DataFrame:
    def restructure(df: pd.DataFrame) -> pd.DataFrame:
        df_cumulative = (
            df
            .filter(regex='.*Cumulative$', axis=1)
            .pipe(remove_substring_from_columns, [' Period Cumulative'])
        )

        df_period = (
            df
            .filter(regex='.*Period$', axis=1)
            .pipe(remove_substring_from_columns, [' Period'])
            .pipe(lambda df_: pd.concat([df_, pd.Series(data=df_cumulative.iloc[-1].values, name='Total FYTD', index=df_cumulative.columns).to_frame().T], axis=0))
            .add_suffix(' Period')
        )
        cumulative_active = df_cumulative['Active'].to_list()
        df_period.insert(0, "Active Period Cumulative",
                         cumulative_active + [cumulative_active[-1]])
        return df_period

    result_df = (
        data
        .pipe(remove_substring_from_columns, [' Net Contribution ', 'Return', '% ', '(', ')'])
        .pipe(lambda df_: df_.set_index(df_.columns[0]))
        .pipe(remove_percentages)
        .pipe(restructure)
    )
    return result_df


def risk_decomposition(data: pd.DataFrame) -> pd.DataFrame:
    return (
        data
        .pipe(remove_substring_from_columns, ['Net Period Forecast ', ' Active Risk Contribution', ' Risk'])
        .pipe(remove_percentages, 'Active')
        .pipe(lambda df_: df_.set_index(df_.columns[0]))
    )


def replace_column_names(data: pd.DataFrame, source_of_return: str) -> pd.DataFrame:
    df_cols = data.columns
    col_names_to_replace = ['Source of Return',
                            'Total Contribution', 'Average active exposure']
    new_column_names = [
        source_of_return, 'Active Return Contribution', 'Average Active Exposure (Z-Sc)']
    for to_replace_name, new_name in zip(col_names_to_replace, new_column_names):
        df_cols = df_cols.str.replace(to_replace_name, new_name)
    data.columns = df_cols
    return data


def factor_attribution(factor_data: pd.DataFrame, risk_data: pd.DataFrame, col_name: str, parent_node: str) -> pd.DataFrame:
    factor_data_cleaned = (
        factor_data
        .pipe(remove_substring_from_columns, ['Net', 'Cumulative', '[', '] '])
        .pipe(remove_percentages, 'Total Contribution')
        .pipe(replace_column_names, col_name)
        .query(f"`Parent Node` == '{parent_node}'")
    )
    risk_data_cleaned = (
        risk_data
        .pipe(remove_percentages, 'Active Risk Contribution')
        .assign(**{col_name: lambda df_: df_.Factor})
    )
    final = (
        pd.merge(factor_data_cleaned, risk_data_cleaned, on=[
            col_name, 'Parent Node'], how='left')
        .filter([col_name, 'Average Active Exposure (Z-Sc)', 'Active Return Contribution', 'Active Risk Contribution'], axis=1)
        .set_index(col_name)
    )
    return final


def status(data: pd.DataFrame) -> pd.DataFrame:
    conditions = [
        (data['Periods In Benchmark'] == 0),
        (data['Periods In Portfolio'] == 0) & (
            data['Periods In Benchmark'] != 0),
        (data['Periods In Portfolio'] != 0) & (
            data['Periods In Benchmark'] != 0)
    ]
    choices = [
        'Out of Bmk Stock',
        'Bmk Stock Not Held',
        'Bmk Stock Held'
    ]
    data['Status'] = np.select(conditions, choices, default='Error')
    return data


def stock_selection(data: pd.DataFrame) -> pd.DataFrame:
    result_df = (
        data
        .pipe(status)
        .pivot_table(values='Cumulative Net Specific Contribution', index='GICS Sector Name', columns='Status', aggfunc='sum', fill_value=0)
        .assign(**{
            'Grand Total': lambda df_: df_.sum(axis=1),
            'Bmk Stock Not Held': lambda df_: 0 if 'Bmk Stock Not Held' not in df_ else df_['Bmk Stock Not Held']
        })
        .filter(['Bmk Stock Held', 'Bmk Stock Not Held', 'Out of Bmk Stock', 'Grand Total'], axis=1)
    )
    return result_df


def attribution_matrix(data: pd.DataFrame) -> pd.DataFrame:
    new_data = data.pivot_table(values=['Average Active Weight', 'Cumulative Net Active Return Contribution', 'Cumulative Net Country Contribution', 'Cumulative Net Industry Contribution',
                                'Cumulative Net Risk Indices Contribution', 'Cumulative Net Specific Contribution', 'Cumulative Net Currency Contribution', 'Cumulative Net World Contribution'], index='GICS Sector Name', aggfunc='sum')
    new_data.columns = ['Avg Act Weight', 'Active Return', 'Country',
                        'Currency', 'Industry', 'Style', 'Specific', 'World']
    new_data = new_data[['Avg Act Weight', 'Active Return', 'Country',
                         'Industry', 'Style', 'Specific', 'Currency', 'World']]
    new_data['Total Active'] = new_data['Active Return']
    del new_data['Active Return']
    return new_data


def attribution_matrix_canadian(data: pd.DataFrame) -> pd.DataFrame:
    new_data = data.pivot_table(values=['Average Active Weight', 'Cumulative Net Active Return Contribution', 'Cumulative Net Country Contribution',
                                'Cumulative Net Industry Contribution', 'Cumulative Net Risk Indices Contribution', 'Cumulative Net Specific Contribution'], index='GICS Sector Name', aggfunc='sum')
    new_data.columns = ['Avg Act Weight', 'Active Return',
                        'Country', 'Industry', 'Style', 'Specific']
    new_data = new_data[['Avg Act Weight', 'Active Return',
                         'Country', 'Industry', 'Style', 'Specific']]
    new_data['Total Active'] = new_data['Active Return']
    del new_data['Active Return']
    return new_data


def top_bottom(data: pd.DataFrame, table_name: str) -> pd.DataFrame:
    final_df = (
        data
        .query("`GICS Sector Name` != 'Cash'")
        .pivot_table(values=['Periods In Portfolio', 'Periods In Benchmark', 'Average Active Weight', 'Cumulative Specific Contribution', 'Cumulative Active Return Contribution'], index=['Asset Id', 'Asset Name', 'GICS Sector Name'], aggfunc='sum')
        .filter(['Periods In Portfolio', 'Periods In Benchmark', 'Average Active Weight', 'Cumulative Specific Contribution', 'Cumulative Active Return Contribution'], axis=1)
        .reset_index()
    )
    final_df.columns = ['Asset ID', table_name, 'GICS Sector', 'Periods In Portfolio',
                        'Periods In Benchmark', 'Average Active Weight', 'Selection', 'Total']
    return final_df


def daily_to_monthly(df: pd.DataFrame, date_col: str) -> pd.DataFrame:
    return (
        df
        .assign(**{date_col: lambda df_: pd.to_datetime(df_[date_col])})
        .assign(**{'month': lambda df_: df_[date_col].dt.to_period('M')})
        .assign(**{'month end': lambda df_: df_.groupby('month')[date_col].transform('max')})
        .pipe(lambda df_: df_.loc[df_[date_col] == df_['month end']])
        .assign(**{date_col: lambda df_: pd.to_datetime(df_['month'].dt.to_timestamp()).dt.date})
        .drop(['month', 'month end'], axis=1)
        .reset_index(drop=True)
    )


def country_exposure(data: pd.DataFrame) -> pd.DataFrame:
    country_code = pd.read_csv(config.COUNTRY_CODES_DIR)
    processed_data = (
        data
        .iloc[1:]
        [(data['Parent Node'].str.fullmatch(r'\/[a-zA-Z ]+')
          == True) | (data['Parent Node'] == "/")]
        .assign(Region=lambda x: x['Parent Node'].str.replace('/', ''))
        .assign(Region=lambda x: x.apply(lambda y: y['Region'] if y['Region'] != '' else np.nan, axis=1))
        .assign(Region=lambda x: x['Region'].fillna(x['Asset ID']))
        [['Region', "Asset ID", "Weight (%)", "Active Weight (%)"]]
    )
    processed_data = remove_percentages(processed_data, 'Weight (%)')
    processed_data = (pd
                      .merge(left=processed_data, right=country_code, left_on="Asset ID", right_on="Alpha-3 code", how="left")
                      [['Region', 'Country',
                          'Weight (%)', 'Active Weight (%)']]
                      .assign(Country=lambda x: x.apply(lambda y: y['Region'] if y['Region'] == 'U.S.' else y['Country'], axis=1))
                      .replace('U.S.', 'United States of America')
                      .dropna()
                      .assign(Region=lambda x: x.apply(lambda y: 'China' if y['Country'] == 'China' else y['Region'], axis=1))
                      .groupby(["Region", "Country"])
                      [["Active Weight (%)", "Weight (%)"]]
                      .agg({'Active Weight (%)': 'sum', 'Weight (%)': 'sum'})
                      [["Active Weight (%)"]]
                      )
    return processed_data
