import pathlib as p



def infer_name_port(sourceDir:str, date:str, portfolio:str, settings:str, filename:str) -> str:
    workingDir = p.Path(__file__).parents[1]
    file_dir_str = date + '_' + portfolio + ' ' + settings
    file_dir = workingDir / sourceDir / date / file_dir_str

    result = None

    for file_dir in file_dir.iterdir():
        root = file_dir.name
        suffix = file_dir.suffix
        start_str = date + '_' + portfolio + '_' + filename + '#'
        if root.startswith(start_str) and suffix == '.csv':
            result = str(file_dir)
    
    if not result:
        raise Exception(f"Can't find file! in {file_dir} with name {filename}")

    return result

def infer_name_custom(sourceDir:str, date:str, folder_name:str, filename:str) -> str:
    workingDir = p.Path(__file__).parents[1]
    file_dir = workingDir / sourceDir / date / folder_name

    result = None

    for file_dir in file_dir.iterdir():
        root = file_dir.name
        suffix = file_dir.suffix
        start_str = date + '_' + filename + '#'
        if root.startswith(start_str) and suffix == '.csv':
            result = str(file_dir)
    
    if not result:
        raise Exception(f"Can't find file! in {file_dir} with name {filename}")

    return result

