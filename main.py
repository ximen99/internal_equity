import lib
from lib import config
import warnings
warnings.filterwarnings("ignore")


def main():
    this_qtr = '2023-09-30'
    last_qtr = '2023-06-30'
    dashboard = lib.Dashboard(
        this_qtr, last_qtr, config.EXTERNAL_JSON_DIR, config.EXTERNAL_DIR)
    dashboard.process()


if __name__ == "__main__":
    main()
