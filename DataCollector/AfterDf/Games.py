from DataCollector.AfterDf.Data import DataCollector
from datetime import datetime
from dateutil.relativedelta import relativedelta
import pandas as pd


class GamesCollector(DataCollector):

    def __init__(self, sql, sett):
        super().__init__(sql, sett)

        self.sett = sett
        self.sql = sql
        self.month_patience_comps = 3
        self.possible_opened_comps = self.get_possible_opened_comps(sql)

    def main_games(self, sql, df_not_registered, list_live_competitions):

        df_not_registered = self.treatment_strings(df_not_registered)
        list_live_competitions = [self.string_norm.treatment_strings(i) for i in list_live_competitions]

        self.update_competitions(sql, list_live_competitions)
        self.insert_new_teams(sql, df_not_registered)
        self.save_new_games(sql, df_not_registered)

        print(f'\nLast game collection was on {datetime.now()}')

    def saving_games_from_file(self, sql, file_path):

        df_not_registered = self.getting_info_from_file(file_path)

        while not df_not_registered.empty:

            start = datetime.now()
            self.main_games(sql, df_not_registered, list(set(df_not_registered['competition'])))
            end = datetime.now()

            df_not_registered = self.cleaning_file_after_saving_new_info(df_not_registered, end, start, file_path)

        print(f'There are not new games to register - {datetime.now()}')

    def get_possible_opened_comps(self, sql):

        comps_in_db = sql.get_df(self.source_name, 'competitions')
        comps_in_db = comps_in_db.sort_values(by='competition_id', ascending=False)
        comps_closed = comps_in_db.dropna(subset=['end_date'])

        if len(comps_closed) > 0:

            reference_date = datetime.now(comps_in_db.iloc[0]['start_date'].tzinfo) - relativedelta(
                             months=self.month_patience_comps)
            forever_closed_comps = comps_closed[comps_closed['end_date'] <= reference_date].index.tolist()

            possible_opened_comps = comps_in_db.drop(forever_closed_comps)
            possible_opened_comps.reset_index(drop=True, inplace=True)

        else:
            possible_opened_comps = comps_in_db

        return possible_opened_comps

    def turn_new_comps_into_insert_df(self, list_new_comps):

        # TODO:
        # list_countries = self.extract_country(list_new_comps)

        insert_comps_df = pd.DataFrame()
        insert_comps_df['competition_name'] = list(set(list_new_comps))
        insert_comps_df['start_date'] = datetime.now()
        insert_comps_df['end_date'] = None
        insert_comps_df['sport_id'] = self.sport_id
        insert_comps_df['machine_id'] = self.machine_id
        insert_comps_df['version_id'] = self.version_id
        insert_comps_df['ref_date_db'] = datetime.now()
        # insert_comps_df['country_id'] = list_countries

        return insert_comps_df

    # @staticmethod
    # def extract_country(list_new_comps):
    #     # TODO:
    #     # countries = self.sql.get_df('main', 'countries')
    #     return None

    def update_competitions(self, sql, list_competitions):

        # Dict with the competitions in database and their 'end_date' values:
        end_dates = self.possible_opened_comps['end_date']

        # Competitions collected:
        opened_comps_name = list(set(list_competitions))

        # NOT(Competitions forever closed in database):
        possible_opened_comps_db = self.possible_opened_comps['competition_name']

        # Which possible opened competitions (in database) are in the collected ones:
        in_opened_comps_name = possible_opened_comps_db.isin(set(opened_comps_name))

        # Competitions in database which 'end_date' is not None (i.e. closed competitions):
        not_na_end_dates = end_dates.notna()

        # Closed competitions in database which actually are opened (exists in opened_competitions):
        list_update_end_date_to_none = possible_opened_comps_db[in_opened_comps_name & not_na_end_dates]
        list_update_end_date_to_none_ids = list_update_end_date_to_none.index.tolist()

        # Opened competitions in database which actually are closed (do not exist in opened_competitions)
        list_update_end_date_to_closed = possible_opened_comps_db[~in_opened_comps_name & ~not_na_end_dates]
        list_update_end_date_to_closed_ids = list_update_end_date_to_closed.index.tolist()

        # In database | Closed Competitions >>> Opened Competitions:
        if list_update_end_date_to_none_ids:
            df_old = self.possible_opened_comps.loc[list_update_end_date_to_none_ids]
            df_new = df_old
            df_new['end_date'] = None
            sql.update_data(df_old, df_new, self.source_name, 'competitions', 'end_date')

        # In database | Opened Competitions >>> Closed Competitions:
        if list_update_end_date_to_closed_ids:
            df_old = self.possible_opened_comps.loc[list_update_end_date_to_closed_ids]
            df_new = df_old
            df_new['end_date'] = datetime.now()
            sql.update_data(df_old, df_new, self.source_name, 'competitions', 'end_date', 'competition_id')

        # Insert opened competitions that are not in database yet:
        opened_comps_name = pd.Series(opened_comps_name)
        opened_competitions_in_db_filter = opened_comps_name.isin(possible_opened_comps_db)
        list_comps_insert = opened_comps_name[~opened_competitions_in_db_filter]

        if not list_comps_insert.empty:
            df_insert = self.turn_new_comps_into_insert_df(list_comps_insert)
            control = sql.insert_data(df_insert, self.source_name, 'competitions')
            self.acknowledger.acknowledge(sql, control, "insert_competitions")
            self.color_print.green_print('-' * 50)
            self.color_print.green_print(f'{len(list_comps_insert)} new competitions collected from '
                                         f'{self.source_name} - {datetime.now()}')

    def insert_new_teams(self, sql, df_not_registered):

        teams_collected = self.get_collected_teams(df_not_registered)
        registered_teams = self.get_teams_registered_in_db()

        teams_not_in_db = ~teams_collected.isin(registered_teams)
        df_new_teams = self.turn_new_teams_into_insert_df(teams_collected[teams_not_in_db])
        df_new_teams = df_new_teams.reset_index(drop=True)

        if not df_new_teams.empty:
            control = sql.insert_data(df_new_teams, self.source_name, 'teams')
            # self.acknowledger.acknowledge(sql, control, "insert_teams")
            self.color_print.green_print('-' * 50)
            self.color_print.green_print(f'{len(df_new_teams)} new teams collected from '
                                         f'{self.source_name} - {datetime.now()}')
        else:
            pass

    @staticmethod
    def get_collected_teams(df_not_registered):

        team_1 = list(df_not_registered['team_1'])
        team_2 = list(df_not_registered['team_2'])

        teams_collected = team_1 + team_2
        teams_collected = pd.Series(list(set(teams_collected)))

        return teams_collected

    def get_teams_registered_in_db(self):

        registered_teams = self.sql.get_df(self.source_name, 'teams', ['team_name', 'sport_id'])
        registered_teams.set_index('sport_id', inplace=True)

        try:
            registered_teams = registered_teams.loc[self.sport_id]
        except KeyError:
            pass

        registered_teams = pd.Series(list(set(registered_teams['team_name'].reset_index(drop=True))))

        return registered_teams

    def turn_new_teams_into_insert_df(self, list_new_teams):

        df = pd.DataFrame()

        if list_new_teams.empty:
            pass

        else:

            df['team_name'] = list_new_teams
            df['sport_id'] = self.sport_id
            df = self.insert_additional_info_in_df(df)

        return df

    def turn_new_games_into_insert_df(self, df):

        df = df.rename(columns={'competition': 'competition_id'})
        cols = ['competition_id', 'team_1', 'team_2', 'home_first', 'start_date']
        df = df[cols]

        df['sport_id'] = self.sport_id
        df['announcement_date'] = datetime.now()

        df = self.insert_additional_info_in_df(df)

        return df

    def check_only_not_registered_games(self, sql, df):

        df = self.get_game_ids(sql, df, 48*60, 48*60)
        df = df[df['game_id'].isna()]
        df = df.drop('game_id', axis=1)
        df = df.reset_index(drop=True)

        return df

    def save_new_games(self, sql, df):

        if not df.empty:

            df = self.treatment_home_first(df, {'team_1': 'team_2', 'team_2': 'team_1'})
            df = self.get_team_ids(sql, df)
            df = self.get_competition_ids(sql, df)
            df = self.turn_new_games_into_insert_df(df)
            df = self.check_only_not_registered_games(sql, df)

        if not df.empty:
            control = sql.insert_data(df, self.source_name, 'games')
            self.acknowledger.acknowledge(sql, control, "insert_games")
            self.color_print.green_print('-'*50)
            self.color_print.green_print(f'{len(df)} new games collected from '
                                         f'{self.source_name} - {datetime.now()}')
