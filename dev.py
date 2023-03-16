# %%
import lib
from lib import config
import json


date_this_qtr = '2023-02-28'
date_last_qtr = '2022-11-30'

json_dir = config.CODE_DIR / "portfolios.json"
portfolio_data = open(json_dir)
portfolio_data = json.load(portfolio_data)


def load_portfolio(date):
    portfolio = lib.Portfolios_Country(date, portfolio_data)
    portfolio.load()
    portfolio.process()
    portfolio.redirect()
    portfolio.append()
    return portfolio


port_this_qtr = load_portfolio(date_this_qtr)
# %%
port_last_qtr = load_portfolio(date_last_qtr)

comparison = lib.Comparison(port_this_qtr, port_last_qtr)
comparison.calculate()
# %%
comparison.write_to_csv()

with open(config.ANALYSIS_DATE_DIR, 'w') as f:
    f.write(date_this_qtr)


# %%
