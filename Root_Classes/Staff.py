import inspect
from datetime import datetime
from Functions.gitt import Git
from Functions.color_print import ColorPrint
from Functions.feather import Feather
from Functions.system import System
from Functions.sleep import Sleep
from Functions.date_and_time import Date_and_Time
from Functions.string_normalization import StringNormalization
from Functions.versions import Versions


class Staff:

    def __init__(self):

        self.system = System()
        self.sleep = Sleep()
        self.color_print = ColorPrint()
        self.string_norm = StringNormalization()
        self.feather = Feather()
        self.date_and_time = Date_and_Time()
        self.git = Git()
        self.versions = Versions()

    @staticmethod
    def describe_error(e):
        caller_file = inspect.stack()[1].filename
        print(f'{datetime.now()}-{caller_file} | Error in line {e.__traceback__.tb_lineno}: {e}')
