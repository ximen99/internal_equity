import lib
from lib import config
import warnings
warnings.filterwarnings("ignore")


def main():
    this_qtr = '2022-12-31'
    last_qtr = '2022-12-31'
    dashboard = lib.Dashboard(
        this_qtr, last_qtr, config.EXTERNAL_JSON_DIR, config.EXTERNAL_DIR)
    dashboard.process()


if __name__ == "__main__":
    main()
