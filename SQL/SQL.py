from Functions.date_and_time import Date_and_Time
import psycopg2
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from Functions.log import Log


class SQL:

    def __init__(self, sett_sql):
        self.sett_sql = sett_sql
        self.conn = self.open_connection()
        self.cur = self.get_cur()

    # # # Connection:

    def open_connection(self):
        while True:
            try:
                return psycopg2.connect(**self.sett_sql)
            except psycopg2.OperationalError:
                Log().saving_log("db_connection.log", "Error in database connection")
                print(f"Error in database connection - {datetime.now()}")

    def close_connection(self):
        self.conn.close()

    def get_cur(self):

        while True:
            try:
                return self.conn.cursor()
            except psycopg2.OperationalError:
                self.conn = self.open_connection()

    # Basics:
    def query(self, query):

        while True:

            try:
                return pd.read_sql_query(query, self.conn)

            except psycopg2.InterfaceError:
                self.conn = self.open_connection()
                self.conn.commit()
                self.cur = self.get_cur()

            except Exception as e:
                print(e)
                return

    def get_header(self, schema, tbl_name):

        while True:
            try:
                query_heads = (f"SELECT column_name FROM information_schema.columns WHERE table_schema = '{schema}' AND"
                               f" table_name = '{tbl_name}'")
                self.cur.execute(query_heads)
                return [row[0] for row in self.cur.fetchall()]

            except psycopg2.InterfaceError:
                self.conn = self.open_connection()
                self.conn.commit()
                self.cur = self.get_cur()

            except Exception as e:
                print(e)
                return

    def get_df(self, schema, tbl_name, columns_select=None):

        while True:

            try:

                if columns_select is None:
                    name_columns = self.get_header(schema, tbl_name)
                    columns_select_query = '*'
                else:
                    name_columns = columns_select
                    columns_select_query = ', '.join(columns_select)

                query_data = f"SELECT {columns_select_query} FROM {schema}.{tbl_name}"
                self.cur.execute(query_data)

                return pd.DataFrame(self.cur.fetchall(), columns=name_columns)

            except psycopg2.InterfaceError:
                self.conn = self.open_connection()
                self.conn.commit()
                self.cur = self.get_cur()

            except psycopg2.errors.SyntaxError:
                pass

            except Exception as e:
                print(e)

    # Matching formats:

    def matching_formats(self, df):

        df = self.convert_datetime(df)
        df = self.treating_null_values(df)

        return df

    @staticmethod
    def replace_null_with_none(tpl):
        return tuple(None if value == 'NULL' else value for value in tpl)

    @staticmethod
    def convert_datetime(df):

        date_columns = [col for col in df.columns if pd.api.types.is_datetime64_any_dtype(df[col])]

        for col in date_columns:
            df[col] = df[col].dt.strftime('%Y-%m-%d %H:%M:%S')

        return df

    @staticmethod
    def treating_null_values(df):

        df = df.replace({np.nan: None})
        df = df.replace({None: 'NULL'})

        return df

    # IDs:

    def get_id(self, reference, schema, tbl_name, col_id_name, col_ref_name):
        """
        :param reference: item whose id is required
        :param schema: schema
        :param tbl_name: table
        :param col_id_name: name of column with the id
        :param col_ref_name: name of column with the reference data
        :return: item's id
        """
        while True:

            try:
                df = self.get_df(schema, tbl_name)
                dict_ids = dict(zip(df[col_ref_name], df[col_id_name]))
                return dict_ids.get(reference)

            except psycopg2.InterfaceError:
                self.conn = self.open_connection()
                self.conn.commit()
                self.cur = self.get_cur()

            except Exception as e:
                print(e)
                return

    def get_item_from_id(self, reference, schema, tbl_name, col_id_name, col_ref_name):
        """
        :param reference: id whose item is required
        :param schema: schema
        :param tbl_name: table
        :param col_id_name: name of column with the id
        :param col_ref_name: name of column with the reference data
        :return: id's item
        """
        while True:

            try:

                try:
                    self.cur.execute(f"SELECT {col_ref_name} FROM {schema}.{tbl_name} "
                                     f"WHERE {col_id_name} = '{reference}'")
                    return self.cur.fetchone()[0]
                except TypeError:
                    return None

            except psycopg2.InterfaceError:
                self.conn = self.open_connection()
                self.conn.commit()
                self.cur = self.get_cur()

            except Exception as e:
                print(e)
                return

    def get_max_id(self, schema, tbl_name, col_id_name):

        while True:

            try:

                list_ids = list(self.get_df(schema, tbl_name)[col_id_name])
                try:
                    return max(list_ids) + 1
                except ValueError:
                    return 1

            except psycopg2.InterfaceError:
                self.conn = self.open_connection()
                self.conn.commit()
                self.cur = self.get_cur()

            except Exception as e:
                print(e)
                return

    def turn_col_into_id(self, df, df_col_name, schema, tbl_name, tbl_col_id, tbl_col_name):

        while True:

            try:

                df_ids = self.get_df(schema, tbl_name, [tbl_col_id, tbl_col_name])
                # TODO: Pegar sempre o valor debaixo, no caso de competições, para não pegar o ID de uma competição
                #  fechada!!!
                dict_with_ids = dict(zip(list(df_ids[tbl_col_name]), list(df_ids[tbl_col_id])))
                df[df_col_name] = df[df_col_name].map(dict_with_ids).fillna(df[df_col_name])

                return df

            except psycopg2.InterfaceError:
                self.conn = self.open_connection()
                self.conn.commit()
                self.cur = self.get_cur()

            except Exception as e:
                print(e)
                return

    # Insert:

    def insert_data(self, df, schema, tbl_name, id_increment=True, id_column=None):
        """
        :param df: df to be inserted in database
        :param schema: schema to be inserted
        :param tbl_name: tbl_name to be inserted
        :param id_increment: True if new df new data require new ids
        :param id_column: name of the column with the ids
        """

        while True:

            data = None

            try:
                df = df.reset_index(drop=True)

                if id_increment:

                    id_column = tbl_name[:-1] + "_id" if id_column is None else id_column
                    next_id = self.get_max_id(schema, tbl_name, id_column)
                    df[id_column] = next_id + pd.Series(range(len(df)))

                df = self.matching_formats(df)

                columns = list(df.columns)

                query = (f"INSERT INTO {schema}.{tbl_name} ({', '.join(columns)}) "
                         f"VALUES ({', '.join(['%s'] * len(columns))})")
                query = query.replace("'NULL'", "NULL")

                data = [tuple(x) for x in df.to_numpy()]
                data = [self.replace_null_with_none(tpl) for tpl in data]

                try:
                    self.cur.executemany(query, data)
                    self.conn.commit()
                    return True

                except psycopg2.errors.NotNullViolation:
                    self.conn.commit()
                    print(f"No data saved. NotNullViolation in {data}")
                    # TODO: Arquivo log para avisar que os dados não foram salvos
                    return False

                except Exception as e:
                    print(f'\033[91mCould not insert data: {e}\033[0m')
                    return False

            except psycopg2.InterfaceError:
                self.conn = self.open_connection()
                self.conn.commit()
                self.cur = self.get_cur()

            except psycopg2.errors.UniqueViolation:
                self.conn.commit()
                return None

            except Exception as e:
                print(e)
                return False

    # Update:
    def update_data(self, df_old, df_new, schema, tbl_name, col_update, col_id=None):
        """
        :param df_old: old df
        :param df_new: new df
        :param schema: schema to be updated
        :param tbl_name: table to be updated
        :param col_update: column to be updated (!!! only one at a time !!!)
        :param col_id: column id name
        :return: None
        """
        while True:

            try:

                df_old = self.matching_formats(df_old)
                df_new = self.matching_formats(df_new)

                columns = list(df_old.columns)
                columns.remove(col_update)

                p1 = ''
                p2 = ''
                query = f"UPDATE {schema}.{tbl_name} SET {col_update} = CASE"

                if col_id is None:

                    dict_old = {
                        tuple(getattr(row, col) for col in columns): getattr(row, col_update)
                        for row in df_old.itertuples(index=False)
                    }

                    dict_new = {
                        tuple(getattr(row, col) for col in columns): getattr(row, col_update)
                        for row in df_new.itertuples(index=False)
                    }

                    for key, value in dict_old.items():

                        case = " ".join([f"{columns[i]} = '{key[i]}' AND"
                                         if isinstance(key[i], str) else f"{columns[i]} = {key[i]} AND"
                                         for i in range(0, len(columns))])[:-4]
                        case = f"WHEN {case} THEN '{dict_new[key]}'" if (
                            isinstance(dict_new[key], str)) else f"WHEN {case} THEN {dict_new[key]}"
                        p1 = f"{p1} {case}"

                        where = f'({case[5:].split("THEN")[0][:-1]}) OR'
                        p2 = f"{p2} {where}"

                    query = f"{query} {p1[1:]} ELSE {col_update} END WHERE {p2[1:-3]}"

                else:

                    dict_old = df_old.set_index(col_id)[col_update].to_dict()
                    dict_new = df_new.set_index(col_id)[col_update].to_dict()

                    for key, value in dict_old.items():

                        quote_key = "'" if isinstance(key, str) else ""
                        quote_value = "'" if isinstance(value, str) else ""

                        p1 = (f"{p1} WHEN {col_id} = {quote_key}{key}{quote_key} "
                              f"THEN {quote_value}{dict_new[key]}{quote_value}")
                        p2 = f"{p2} {quote_key}{key}{quote_key},"

                    query = f"{query} {p1} ELSE {col_update} END WHERE {col_id} IN ({p2[1:-1]})"

                query = query.replace("'NULL'", "NULL")
                self.cur.execute(query)
                self.conn.commit()
                return

            except psycopg2.InterfaceError:
                self.conn = self.open_connection()
                self.cur = self.get_cur()

            except Exception as e:
                print(e)
                return

    # Time filter:

    def get_only_today_data(self, schema, tbl_name, datetime_col):

        while True:

            try:

                today = Date_and_Time.today_date()
                tomorrow = Date_and_Time.tomorrow_date()
                df = self.get_df(schema, tbl_name)
                df.set_index(datetime_col, inplace=True)
                df_today = df.loc[today:tomorrow - pd.Timedelta(seconds=1)]
                df_today.reset_index(inplace=True)
                return df_today

            except psycopg2.InterfaceError:
                self.conn = self.open_connection()
                self.cur = self.get_cur()

            except Exception as e:
                print(e)
                return

    def get_only_tomorrow_data(self, schema, tbl_name, datetime_col):

        while True:

            try:

                today_plus = Date_and_Time.today_date() + timedelta(days=1)
                tomorrow_plus = Date_and_Time.tomorrow_date() + timedelta(days=1)
                df = self.get_df(schema, tbl_name)
                df.set_index(datetime_col, inplace=True)
                df_tomorrow = df.loc[today_plus:tomorrow_plus - pd.Timedelta(seconds=1)]
                df_tomorrow.reset_index(inplace=True)
                return df_tomorrow

            except psycopg2.InterfaceError:
                self.conn = self.open_connection()
                self.conn.commit()
                self.cur = self.get_cur()

            except Exception as e:
                print(e)
                return

    def get_df_time_window(self, schema, tbl_name, datetime_col, min_before, min_after):

        while True:

            try:

                inf_limit = datetime.now() - timedelta(minutes=min_before)
                sup_limit = datetime.now() + timedelta(minutes=min_after)

                df = self.get_df(schema, tbl_name)

                df_window = df.loc[(df[datetime_col] > inf_limit) & (df[datetime_col] < sup_limit)]

                df_window.reset_index(inplace=True)

                return df_window

            except psycopg2.InterfaceError:
                self.conn = self.open_connection()
                self.conn.commit()
                self.cur = self.get_cur()

            except Exception as e:
                print(e)
                return

    @staticmethod
    def get_filter_data(date_start, date_end, date_column):
        try:
            date_start = date_start.strftime('%Y-%m-%d')
        except TypeError:
            date_start = None

        try:
            date_end = date_end.strftime('%Y-%m-%d')
        except TypeError:
            date_end = None

        if date_start is None and date_end is None:
            return ''
        elif date_start is None:
            return f" WHERE '{date_column}' < '{date_end}' "
        elif date_end is None:
            return f" WHERE '{date_column}' > '{date_end}' "
        else:
            return f" WHERE '{date_column}' BETWEEN '{date_start}' AND '{date_end}' "

    def get_existing_schemas(self, database='postgres'):
        query_schemas = f"SELECT schema_name FROM information_schema.schemata WHERE catalog_name = '{database}';"
        return self.query(query_schemas)
