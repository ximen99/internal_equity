# %%
from pathlib import Path
from dataclasses import dataclass
from .FilesDir import FilesDir


CODE_DIR = Path(__file__).parents[1].absolute()
COUNTRY_CODES_DIR = CODE_DIR / 'country_codes.csv'
INTERNAL_DIR = FilesDir(Path(
    'S:\ISR\Branch - Investment Risk\Risk Reporting\All Dashboards Working\BPM Internal Active Dashboards\Auto'))
EXTERNAL_DIR = FilesDir(Path(
    'S:\ISR\Branch - Investment Risk\Risk Reporting\All Dashboards Working\BPM External Dashboard\Auto'))
TEST_DIR = FilesDir(CODE_DIR.parent)
INTERNAL_JSON_DIR = CODE_DIR / 'internal_portfolios.json'
EXTERNAL_JSON_DIR = CODE_DIR / 'external_portfolios.json'
