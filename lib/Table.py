import pathlib as p
import pandas as pd


class Table:
    def __init__(self, source_folder: str, date: str, portfolio_code: str, portfolio_prefix: str, table_config: dict, table_name: str) -> None:
        self.workingDir = p.Path(__file__).parents[2]
        self.source_folder = source_folder
        self.portfolio_code = portfolio_code
        self.portfolio_prefix = portfolio_prefix
        self.date = date
        self.file_dir = None
        self.table_config = table_config
        self.table_name = table_name
        self.table_dict = {}

    def _infer_dir_port(self, portfolio: str, settings: str, filename: str) -> None:
        file_dir_str = self.date + '_' + portfolio + ' ' + settings
        self._folder_dir = self.workingDir / self.source_folder / self.date / file_dir_str
        self._start_str = self.date + '_' + portfolio + '_' + filename + '#'

    def _infer_dir_trailing_name(self, portfolio: str, trailing_name: str, settings: str, filename: str) -> None:
        file_dir_str = self.date + '_' + portfolio + ' ' + trailing_name + ' ' + settings
        self._folder_dir = self.workingDir / self.source_folder / self.date / file_dir_str
        self._start_str = self.date + '_' + portfolio + '_' + filename + '#'

    def _infer_dir_prefix_name(self, portfolio: str, prefix_name: str, settings: str, filename: str) -> None:
        file_dir_str = self.date + '_' + prefix_name + ' ' + portfolio + ' ' + settings
        self._folder_dir = self.workingDir / self.source_folder / self.date / file_dir_str
        self._start_str = self.date + '_' + portfolio + '_' + filename + '#'

    def _infer_dir_filename_prefix(self, portfolio: str, filename_prefix: str, settings: str, filename: str) -> None:
        file_dir_str = self.date + '_' + portfolio + ' ' + settings
        self._folder_dir = self.workingDir / self.source_folder / self.date / file_dir_str
        self._start_str = self.date + '_' + portfolio + \
            '_' + filename_prefix + '_' + filename + '#'

    def _infer_dir_custom(self, folder_name: str, filename: str) -> None:
        file_dir_str = self.date + '_' + folder_name
        self._folder_dir = self.workingDir / self.source_folder / self.date / file_dir_str
        self._start_str = self.date + '_' + filename + '#'

    def _find_file(self) -> None:
        for file_dir in self._folder_dir.iterdir():
            root = file_dir.name
            suffix = file_dir.suffix
            if root.startswith(self._start_str) and suffix == '.csv':
                self.file_dir = str(file_dir)
        if not self.file_dir:
            raise Exception(
                f"Can't find file in {self._folder_dir} starts with {self._start_str}")

    def _infer_dir(self) -> None:
        table_config = self.table_config
        if 'trailing_name' in table_config:
            self._infer_dir_trailing_name(
                self.portfolio_code, table_config['trailing_name'], table_config['settings'], table_config['filename'])
        elif 'prefix_name' in table_config:
            self._infer_dir_prefix_name(
                self.portfolio_code, table_config['prefix_name'], table_config['settings'], table_config['filename'])
        elif 'filename_prefix' in table_config:
            self._infer_dir_filename_prefix(
                self.portfolio_code, table_config['filename_prefix'], table_config['settings'], table_config['filename'])
        elif 'settings' in table_config:
            self._infer_dir_port(
                self.portfolio_code, table_config['settings'], table_config['filename'])
        else:
            self._infer_dir_custom(
                table_config['folder_name'], table_config['filename'])

    def _read_csv(self, start_row=None, end_row=None, start_col=None, end_col=None, **kwargs) -> pd.DataFrame:
        return pd.read_csv(self.file_dir,  on_bad_lines='skip', **kwargs).iloc[start_row:end_row, start_col:end_col]

    def _determine_prefix(self, table_type: str) -> str:
        if table_type == 'detail':
            return self.portfolio_prefix
        if table_type == 'summary':
            return 'as_'+self.portfolio_prefix

    def _load_df_dict(self) -> None:
        sub_tables_config = self.table_config['sub_tables']

        for sub_table, sub_table_config in sub_tables_config.items():
            sub_table_prefix = self._determine_prefix(sub_table)
            self.table_dict[sub_table] = {'df': self._read_csv(**sub_table_config),
                                          'save_to_name': sub_table_prefix + "_" + self.table_name + ".csv"
                                          }

    def load(self) -> None:
        # find folder and start string
        self._infer_dir()
        # locate file
        self._find_file()
        # load csv and store dataframe to list
        self._load_df_dict()
