class Acknowledger:

    def __init__(self, source_id, sport_id, machine_id, version_id):

        self.source_id = source_id
        self.sport_id = sport_id
        self.machine_id = machine_id
        self.version_id = version_id

    @staticmethod
    def get_role_id(sql, role):

        while True:

            try:

                role_id = sql.get_id(role, 'main', 'roles', 'role_id', 'role_name')
                check = role_id + 1
                return role_id

            except TypeError:
                sql.conn.commit()