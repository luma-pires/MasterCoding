from SQL.SQL import SQL
from Managerr.Info import Info
from Managerr.Staff import Staff
import re


class Integrator(Staff, Info):

    def __init__(self, sql, machine_id, version_id):

        self.sql = SQL(sql)
        self.ids_sources = self.get_sources_relation()
        self.schemas = self.existing_schemas_sources()
        self.start_date = None
        self.end_date = None
        self.team_columns = ['team_1', 'team_2']

    # Turn source_ids and source_names into a dict:
    def get_sources_relation(self):
        df_sources = self.sql.get_df('main', 'sources')
        return df_sources.set_index('source_name')['source_id'].to_dict()

    # Get sources already inserted as schema in database:
    def existing_schemas_sources(self):
        all_schemas = set(self.sql.get_existing_schemas())
        return list(all_schemas.intersection(set(self.ids_sources.keys())))

    # Get df_game from source schema with the names (teams, competition)  instead of ids (filtered by date):
    def getting_game_strings_from_source_schema(self, source):

        data_filter = self.sql.get_filter_data(self.start_date, self.end_date, 'ref_date_db')

        query_game_strings = (
            f"SELECT "
            f"g.game_id, "
            f"g.home_first, "
            f"g.start_date, "
            f"g.ref_date_db, "
            f"t.team_name AS team_1, "
            f"t.team_name AS team_2, "
            f"c.competition_name AS competition "
            f"FROM {source}.games g "
            f"INNER JOIN {source}.teams t "
            f"ON g.team_1 = t.team_id "
            f"INNER JOIN {source}.competitions c ON "
            f"g.competition_id = c.competition_id"
            f"{data_filter}"
        )

        df = self.sql.query(query_game_strings)
        df['source_id'] = self.ids_sources[source]

        return df

    # Get dict with source names as keys and df_games joined with names (strings) as values:
    def getting_dfs_game_strings_all_sources(self):
        return {source: self.getting_game_strings_from_source_schema(source)
                for source in list(self.existing_schemas_sources())}

    # # # Normalization of strings
    def norm_df_games_joined_with_names(self, df):

        treated_df = self.treating_team_strings(df)
        treated_df = self.treating_competition_strings(treated_df)

        return treated_df

    def treating_team_strings(self, df):

        columns = self.team_columns

        df = self.treatment_sport_club_acronyms(df, columns)
        df = self.treatment_sub(df, columns)
        df = self.treatment_numbers(df, columns)
        df = self.treatment_acronyms(df, columns)
        df = self.treat_reserves(df, columns)

        return df

    @staticmethod
    def treating_competition_strings(df):
        return df

    # Replacements:

    @staticmethod
    def replace_terms(df, list_terms, list_columns, new_term=''):

        pattern = '|'.join([rf'\b{term}\b' for term in list_terms])
        df[list_columns] = df[list_columns].str.replace(pattern, new_term, regex=True)

        return df

    @staticmethod
    def treatment_sport_club_acronyms(df, columns):

        useless_terms = ['fc', 'clube', 'sub', 'sg', 'sc', '(ksa)', 'sv', 'sk', 'skn', 'tsv', 'acs', 'csc', 'bk', 'if',
                         'fk', 'rcd', 'ca', 'sc', 'sl', 'cf', 'calcio']
        useless_terms = {key: '*' for key in useless_terms}

        df[columns] = df[columns].replace(useless_terms)

        return df

    @staticmethod
    def treatment_sub(df, columns):

        sub = {' sub': '*', ' u': '*'}
        df[columns] = df[columns].replace(sub)
        df[columns] = df[columns].apply(lambda x: x.str.split('*').str[0])

        return df

    @staticmethod
    def treatment_numbers(df, columns):

        numbers = {'i': '1', 'ii': '2', 'iii': '3', 'iv': '4', 'v': '5'}
        df[columns] = df[columns].replace(numbers)

        return df

    @staticmethod
    def treatment_acronyms(df, columns):

        acronyms = {'utd': 'united', 'jrs': 'juniors'}
        df[columns] = df[columns].replace(acronyms)

        return df

    @staticmethod
    def treat_reserves(df, columns):

        reserve = {'- reservas': '', '(res)': '', 'reserves': ''}
        df[columns] = df[columns].replace(reserve)

        return df

    # Merging:
    def merging_game_ids(self):
        pass

    def look_up_value(self, cols_reference):
        pass

