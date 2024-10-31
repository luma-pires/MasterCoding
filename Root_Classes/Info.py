from Root_Classes.Staff import Staff
from SQL.SQL import SQL


class Info(Staff):

    def __init__(self, sett):
        super().__init__()
        self.main_schema = 'main'

        self.sett = sett

        self.sett_all_machines = sett.machines
        self.general = self.sett.general

        self.source_name = self.general.source.lower()
        self.sport_name = self.general.sport.lower()
        self.currency_name = self.general.currency.lower()

        self.sql = SQL(self.sett.sql)

        self.source_id = self.sql.get_id(self.source_name, self.main_schema, 'sources', 'source_id', 'source_name')
        self.sport_id = self.sql.get_id(self.sport_name, self.main_schema, 'sports', 'sport_id', 'sport_name')
        self.currency_id = self.sql.get_id(self.currency_name, self.main_schema, 'currency', 'currency_id',
                                           'currency_name')
        self.version_id = self.git.get_version()
        self.machine = self.system.get_machine_name()
        self.machine_id = self.sql.get_id(self.machine, self.main_schema, 'machines', 'machine_id', 'machine_name')

        self.sett_machines = self.sett_all_machines.get(self.machine)

        self.sql.close_connection()
        self.sql = None
