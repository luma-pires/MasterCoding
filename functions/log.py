from Functions.system import System
from datetime import datetime


class Log:

    def __init__(self):
        self.path = f"{System().get_project_path()}//Logs"
        self.loggers = {}

    def saving_log(self, file_name, message):

        with open(f"{self.path}//{file_name}", 'a') as log_file:
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            message = f"{current_time} - {message}"
            log_file.write(message + '\n')

    def erasing_log(self, file_name):
        with open(f"{self.path}//{file_name}", 'w') as log_file:
            pass
