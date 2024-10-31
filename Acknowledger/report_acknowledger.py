from Acknowledger.acknowledger import Acknowledger
from datetime import datetime
import pandas as pd


class Report_Acknowledger(Acknowledger):
    def __init__(self, source_id, sport_id, machine_id, version_id):
        super().__init__(source_id, sport_id, machine_id, version_id)

    def acknowledge(self, sql, role, total, collected, delay):

        role_id = self.get_role_id(sql, role)

        while True:

            try:

                data = {
                    'source_id': [self.source_id],
                    'sport_id': [self.sport_id],
                    'role_id': [role_id],
                    'total': [total],
                    'collected': [collected],
                    'delay': [delay],
                    'ref_date_db': [datetime.now()],
                    'machine_id': [self.machine_id],
                    'version_id': [self.version_id]
                }

                df = pd.DataFrame(data)
                control = sql.insert_data(df, 'main', 'reports', id_increment=False)

                if control:
                    return
                elif control is None:
                    print(f'{datetime.now()} - Control is None')
                    pass

            except Exception as e:
                print(f"report_acknowledger.py | Error in line {e.__traceback__.tb_lineno}: {e}")
                return
