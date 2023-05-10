import lib
from lib import config
import warnings
warnings.filterwarnings("ignore")


def main():
    this_qtr = '2023-04-30'
    last_qtr = '2023-03-31'
    dashboard = lib.Dashboard(
        this_qtr, last_qtr, config.INTERNAL_JSON_DIR, config.INTERNAL_DIR)
    dashboard.process()


if __name__ == "__main__":
    main()
