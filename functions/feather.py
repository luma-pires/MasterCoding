from Functions.system import System
import pandas as pd


class Feather:

    def __init__(self):
        self.project_path = System().get_project_path()

    def saving_df(self, df, path_from_project_folder):
        full_path = f'{self.project_path}\\{path_from_project_folder}'
        df.to_feather(full_path)

    def retrieving_df(self, path_from_project_folder):
        try:
            full_path = f'{self.project_path}\\{path_from_project_folder}'
            return pd.read_feather(full_path)
        except FileNotFoundError:
            return None
