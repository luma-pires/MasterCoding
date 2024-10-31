from Acknowledger.acknowledger import Acknowledger
import time
import pandas as pd
from datetime import datetime


class Roles_Acknowledger(Acknowledger):

    def __init__(self, source_id, sport_id, machine_id, version_id):
        super().__init__(source_id, sport_id, machine_id, version_id)
        self.dict_sleep_role = {'main_games': 5, 'main_scores': 5}

    def acknowledge(self, sql, successful, role):

        while True:

            try:

                role_id = self.get_role_id(sql, role)
                self.sleep_depending_on_role(role)

                data = {
                    'source_id': [self.source_id],
                    'sport_id': [self.sport_id],
                    'role_id': [role_id],
                    'successful': [successful],
                    'ref_date_db': [datetime.now()],
                    'machine_id': [self.machine_id],
                    'version_id': [self.version_id]
                }

                df = pd.DataFrame(data)
                control = sql.insert_data(df, 'main', 'acknowledger', id_increment=False)

                if control:
                    return
                elif control is None:
                    print(f'{datetime.now()} - Control is None')
                    pass

            except Exception as e:
                print(f"Error in line {e.__traceback__.tb_lineno}: {e}")

    def sleep_depending_on_role(self, key):
        value = self.dict_sleep_role.get(key)
        time.sleep(value) if value is not None else None
