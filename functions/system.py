from datetime import datetime
import time
import socket
import os


class System:

    def __init__(self):
        self.machine = self.get_machine_name()
        self.project_path = self.get_project_path()

    @staticmethod
    def get_machine_name():
        machine_name = socket.gethostname()
        return machine_name.lower()

    @staticmethod
    def get_project_path():
        target_folder = '.idea'
        current_path = os.getcwd()

        while True:
            if current_path == r'C:\\':
                return None
            folders = os.listdir(current_path)
            if target_folder in folders:
                break
        return current_path

    def get_project_name(self):
        return self.project_path.split('\\')[-1]

    @staticmethod
    def get_user_folder():
        current_path = os.getcwd()
        folders = current_path.split('\\')
        user_folder = folders[folders.index('Users') + 1]
        return user_folder

    @staticmethod
    def relative_path(down, up):
        return os.path.relpath(down, up)

    @staticmethod
    def get_datetime_last_modification(file_path):
        time_last_modification = os.path.getmtime(file_path)
        time_last_modification = time.ctime(time_last_modification)
        time_last_modification_dt = datetime.strptime(time_last_modification, '%a %b %d %H:%M:%S %Y')
        return time_last_modification_dt

    @staticmethod
    def get_file_name(file_path=None):

        if file_path is None:
            file_path = __file__

        current_file_complete = os.path.basename(file_path)
        current_file = os.path.splitext(current_file_complete)[0]

        return current_file

    @staticmethod
    def check_paths(list_paths):
        [os.makedirs(path) for path in list_paths if not os.path.exists(path)]
