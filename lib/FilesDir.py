from pathlib import Path
from dataclasses import dataclass


@dataclass
class FilesDir:
    working_dir: Path

    def __post_init__(self):
        self.source_dir = self.working_dir / 'DE'
        self.save_dir = self.working_dir / 'Python Data'
        self.time_series_dir = self.working_dir / 'Time-series Data'
        self.check_dir = self.working_dir / 'Check'
        self.analysis_date_dir = self.working_dir / 'analysis_date.txt'
