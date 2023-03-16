from pathlib import Path

WORKING_DIR = Path(__file__).absolute().parents[2]
CODE_DIR = Path(__file__).absolute().parents[1]
SOURCE_DIR = WORKING_DIR / 'DE'
SAVE_DIR = WORKING_DIR / 'Python Data'
TIME_SERIES_DIR = WORKING_DIR / 'Time-series Data'
CHECK_DIR = WORKING_DIR / 'Check'
COUNTRY_CODES_DIR = CODE_DIR / 'country_codes.csv'
ANALYSIS_DATE_DIR = WORKING_DIR / 'analysis_date.txt'
