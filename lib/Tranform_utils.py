import pandas as pd
import numpy as np


def remove_substring_from_columns(data, patterns):
    for i in patterns:
        data.columns = data.columns.str.replace(i, '')
    return data


def add_time_series(data, date_directory, file_dir):
    new_data = remove_substring_from_columns(data, ['FaCS ', ' - Active'])
    new_data_two = new_data[new_data['Asset Name'].isnull()]
    new_data_three = new_data_two[[
        'Growth', 'Liquidity', 'Momentum', 'Quality', 'Size', 'Value', 'Volatility', 'Yield']]
    new_data_three['Date'] = date_directory
    new_data_three = new_data_three.set_index('Date')
    time_series_data_updated = time_series(
        file_dir, new_data_three)
    return time_series_data_updated


def time_series(file_dir, dataframe_to_append):
    new_data = pd.read_excel(file_dir)
    new_data['Date'] = pd.to_datetime(new_data['Date']).dt.date
    x = dataframe_to_append.reset_index()
    x['Date'] = pd.to_datetime(x['Date']).dt.date
    if new_data['Date'].isin(x['Date']).any() == False:
        appended_data = new_data.append(x)
    else:
        appended_data = new_data[new_data['Date'].isin(x['Date']) == False]
        appended_data = appended_data.append(x)
    # Sort by recency
    sorted_data = appended_data.sort_values('Date', ascending=True)
    excel_data = sorted_data.set_index(['Date'])
    excel_data.to_excel(file_dir)
    return excel_data


def remove_percentages(data, column_start):
    for i in data.loc[:, column_start:]:
        if data[i].dtypes == 'object':
            data[i] = data[i].str.rstrip('%').astype('float') / 100.0
    return data


def return_decomposition(data):
    data = remove_substring_from_columns(
        data, [' Period Net Contribution ', ' Period Cumulative Net Contribution ', 'Return', '% ', '(', ')'])
    data = remove_percentages(data, 'Active')
    data = data.set_index(data.columns[0])
    return data


def risk_decomposition(data):
    data = remove_substring_from_columns(
        data, ['Net Period Forecast ', ' Active Risk Contribution', ' Risk'])
    data = remove_percentages(data, 'Active')
    data = data.set_index(data.columns[0])
    return data


def replace_column_names(data, source_of_return):
    df_cols = data.columns
    col_names_to_replace = ['Source of Return',
                            'Total Contribution', 'Average active exposure']
    new_column_names = [
        source_of_return, 'Active Return Contribution', 'Average Active Exposure (Z-Sc)']
    for i, h in zip(col_names_to_replace, new_column_names):
        df_cols = df_cols.str.replace(i, h)
        data.columns = df_cols
    return data


def industry_attribution(exposure_data, risk_data):
    cleaned_exposure_data = remove_substring_from_columns(
        exposure_data, ['Net', 'Cumulative', '[', '] '])
    cleaned_exposure_data_two = remove_percentages(
        cleaned_exposure_data, 'Total Contribution')
    final_exposure_data = replace_column_names(
        cleaned_exposure_data_two, 'GICS Sector')
    final_exposure_data = final_exposure_data[final_exposure_data['Parent Node'] == '/Industry']

    cleaned_risk_data_two = remove_percentages(
        risk_data, 'Active Risk Contribution')
    cleaned_risk_data_two['GICS Sector'] = cleaned_risk_data_two['Factor']
    final = pd.merge(final_exposure_data, cleaned_risk_data_two, on=[
                     'GICS Sector', 'Parent Node'], how='left')
    final = final[[
        'GICS Sector', 'Average Active Exposure (Z-Sc)', 'Active Return Contribution', 'Active Risk Contribution']]
    data = final.set_index('GICS Sector')
    return data


def style_attribution(exposure_data, risk_data):
    cleaned_exposure_data = remove_substring_from_columns(
        exposure_data, ['Net', 'Cumulative', '[', '] '])
    cleaned_exposure_data_two = remove_percentages(
        cleaned_exposure_data, 'Total Contribution')
    final_exposure_data = replace_column_names(
        cleaned_exposure_data_two, 'Factor')
    final_exposure_data = final_exposure_data[final_exposure_data['Parent Node']
                                              == '/Risk Indices']

    cleaned_risk_data_two = remove_percentages(
        risk_data, 'Active Risk Contribution')
    final = pd.merge(final_exposure_data, cleaned_risk_data_two,
                     on=['Factor', 'Parent Node'], how='left')
    final = final[[
        'Factor', 'Average Active Exposure (Z-Sc)', 'Active Return Contribution', 'Active Risk Contribution']]
    data = final.set_index('Factor')
    return data


def status(data):
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


def stock_selection(data):
    data = status(data)
    data = data.pivot_table(values='Cumulative Net Specific Contribution',
                            index='GICS Sector Name', columns='Status', aggfunc='sum', fill_value=0)
    data['Grand Total'] = data.sum(axis=1)
    data.name = 'gl_th_factors_specific_contrib_by_factor.csv'
    if 'Bmk Stock Not Held' not in data:
        data['Bmk Stock Not Held'] = 0
    data = data[['Bmk Stock Held', 'Bmk Stock Not Held',
                 'Out of Bmk Stock', 'Grand Total']]
    return data


def attribution_matrix(data):
    new_data = data.pivot_table(values=['Average Active Weight', 'Cumulative Net Active Return Contribution', 'Cumulative Net Country Contribution', 'Cumulative Net Industry Contribution',
                                'Cumulative Net Risk Indices Contribution', 'Cumulative Net Specific Contribution', 'Cumulative Net Currency Contribution', 'Cumulative Net World Contribution'], index='GICS Sector Name', aggfunc='sum')
    new_data.columns = ['Avg Act Weight', 'Active Return', 'Country',
                        'Currency', 'Industry', 'Style', 'Specific', 'World']
    new_data = new_data[['Avg Act Weight', 'Active Return', 'Country',
                         'Industry', 'Style', 'Specific', 'Currency', 'World']]
    new_data['Total Active'] = new_data['Active Return']
    del new_data['Active Return']
    return new_data


def attribution_matrix_canadian(data):
    new_data = data.pivot_table(values=['Average Active Weight', 'Cumulative Net Active Return Contribution', 'Cumulative Net Country Contribution',
                                'Cumulative Net Industry Contribution', 'Cumulative Net Risk Indices Contribution', 'Cumulative Net Specific Contribution'], index='GICS Sector Name', aggfunc='sum')
    new_data.columns = ['Avg Act Weight', 'Active Return',
                        'Country', 'Industry', 'Style', 'Specific']
    new_data = new_data[['Avg Act Weight', 'Active Return',
                         'Country', 'Industry', 'Style', 'Specific']]
    new_data['Total Active'] = new_data['Active Return']
    del new_data['Active Return']
    return new_data


def top_bottom(data, table_name):
    filtered_data = data[data['GICS Sector Name'] != 'Cash']
    new_data = pd.pivot_table(filtered_data, values=['Periods In Portfolio', 'Periods In Benchmark', 'Average Active Weight',
                              'Cumulative Specific Contribution', 'Cumulative Active Return Contribution'], index=['Asset Id', 'Asset Name', 'GICS Sector Name'], aggfunc='sum')
    new_data = new_data[['Periods In Portfolio', 'Periods In Benchmark', 'Average Active Weight',
                         'Cumulative Specific Contribution', 'Cumulative Active Return Contribution']]
    new_data = new_data.reset_index()
    new_data.columns = ['Asset ID', table_name, 'GICS Sector', 'Periods In Portfolio',
                        'Periods In Benchmark', 'Average Active Weight', 'Selection', 'Total']
    return new_data
