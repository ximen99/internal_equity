import pathlib as p

class LoadTable:
    def __init__(self):
        self.workingDir = p.Path(__file__).parents[2]
        self.folder_dir = None
        self.start_str = None
        self.file_dir = None

    def infer_dir_port(self, sourceDir:str, date:str, portfolio:str, settings:str, filename:str):
        file_dir_str = date + '_' + portfolio + ' ' + settings
        self.folder_dir = self.workingDir / sourceDir / date / file_dir_str    
        self.start_str = date + '_' + portfolio + '_' + filename + '#'
    
    def infer_dir_custom(self, sourceDir:str, date:str, folder_name:str, filename:str):        
        self.folder_dir = self.workingDir / sourceDir / date / folder_name
        self.start_str = date + '_' + filename + '#'
    
    def find_file(self):
        for file_dir in self.folder_dir.iterdir():
            root = file_dir.name
            suffix = file_dir.suffix
            if root.startswith(self.start_str) and suffix == '.csv':
                self.file_dir = str(file_dir)
        
        if not self.file_dir:
            raise Exception(f"Can't find file in {self.folder_dir} starts with {self.start_str}")
    