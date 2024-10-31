import pandas as pd
from datetime import datetime


class Versions:

    def check_if_version_is_in_db(self, version_id, machine_id, sql):

        df_versions = sql.get_df('main', 'versions')
        versions = set(list(df_versions['version_id']))

        if version_id not in versions:
            self.saving_version(version_id, machine_id, sql)

    @staticmethod
    def saving_version(version_id, machine_id, sql):

        df = pd.DataFrame({
            'version_id': [version_id],
            'branch': [version_id.split('-')[0]],
            'machine_id': machine_id,
            'datetime_release': datetime.now()
        })

        sql.insert_data(df, 'main', 'versions', id_increment=False)
