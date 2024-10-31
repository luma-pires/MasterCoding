from Root_Classes.Info import Info
from Root_Classes.Staff import Staff
from datetime import datetime
import pandas as pd
from Acknowledger.roles_acknowledger import Roles_Acknowledger


class DataCollector(Info, Staff):

    def __init__(self, sql, sett):
        super().__init__(sett)
        self.sett = sett
        self.sql = sql
        self.min_time_window_inf = 60 * 48
        self.min_time_window_sup = 60 * 48
        self.file_name = self.system.get_file_name()
        self.acknowledger = Roles_Acknowledger(self.source_id, self.sport_id, self.machine_id, self.version_id)

    def getting_info_from_file(self, file_path):
        df_not_registered = self.feather.retrieving_df(file_path)
        # TODO: Acknowledge
        return df_not_registered

    def saving_df_in_file(self, df, path):
        self.feather.saving_df(df, path)

    def cleaning_file_after_saving_new_info(self, df_not_registered, end, start, file_path):

        if datetime.now() - df_not_registered['ref_date_db'][0] > end - start:
            self.saving_df_in_file(pd.DataFrame(), file_path)

        return self.getting_info_from_file(file_path)

    def treatment_strings(self, df):
        return df.apply(lambda col: col.map(self.string_norm.treatment_strings))

    @staticmethod
    def treatment_home_first(df, columns_switch):

        if 'home_first' in list(df.columns):
            return df

        df = df.dropna(subset=['team_1', 'team_2']).reset_index(drop=True)
        df['home_first'] = df['team_1'] < df['team_2']

        df_no_switch = df[df['home_first']]
        df_switch = df[~df['home_first']]

        if len(df_switch) > 0:
            df_switch = df_switch.rename(columns=columns_switch)

        df_final = pd.concat([df_switch, df_no_switch], ignore_index=True)

        return df_final

    def get_team_ids(self, sql, df):

        final_df = df

        try:
            final_df = sql.turn_col_into_id(df, 'team_1', self.source_name, 'teams', 'team_id', 'team_name')
            final_df = sql.turn_col_into_id(final_df, 'team_2', self.source_name, 'teams', 'team_id', 'team_name')
        except Exception as e:
            self.describe_error(e)

        return final_df

    def get_competition_ids(self, sql, df):
        # TODO: Pegar apenas os das competições abertas
        df = sql.turn_col_into_id(
            df, 'competition', self.source_name, 'competitions', 'competition_id', 'competition_name')
        return df

    def insert_additional_info_in_df(self, df):

        df = df.copy()

        df.loc[:, 'machine_id'] = self.machine_id
        df.loc[:, 'version_id'] = self.version_id
        df.loc[:, 'ref_date_db'] = datetime.now()

        return df

    def get_game_ids(self, sql, df, min_window_inf=None, min_window_sup=None):

        try:

            list_cols = ['team_1', 'team_2', 'sport_id', 'game_id']
            list_cols_without_game_id = ['team_1', 'team_2', 'sport_id']

            info_cols_with_ids = ['team_1', 'team_2']
            mask_info_not_identified = df[info_cols_with_ids].map(lambda x: isinstance(x, str)).any(axis=1)

            df_with_info_not_identified = df[mask_info_not_identified].reset_index(drop=True)
            df_with_all_info_identified = df[~mask_info_not_identified].reset_index(drop=True)

            if min_window_inf is None:
                min_window_inf = self.min_time_window_inf

            if min_window_sup is None:
                min_window_sup = self.min_time_window_inf

            df_possible_games = sql.get_df_time_window(self.source_name, 'games', 'ref_date_db', min_window_inf,
                                                       min_window_sup)[list_cols]
            df_merge = df_with_all_info_identified[list_cols_without_game_id]

            if len(df_possible_games) > 0:

                df_registered = pd.merge(df_merge, df_possible_games, on=list_cols_without_game_id, how='inner')
                dict_registered = df_registered.groupby(['team_1', 'team_2'])['game_id'].first().to_dict()
                df_with_all_info_identified['key'] = list(zip(df_with_all_info_identified['team_1'],
                                                              df_with_all_info_identified['team_2']))
                df_with_all_info_identified['game_id'] = df_with_all_info_identified['key'].map(dict_registered)
                df_with_all_info_identified.drop(columns=['key'], inplace=True)
                df_with_all_info_identified = df_with_all_info_identified.sort_values(by='game_id', ascending=False)
                df_with_all_info_identified.drop_duplicates(subset=['team_1', 'team_2'], inplace=True)
                df_with_all_info_identified.reset_index(inplace=True, drop=True)

                df_with_info_not_identified['game_id'] = None

                df = pd.concat([df_with_all_info_identified, df_with_info_not_identified], axis=0, ignore_index=False)
                df = df.reset_index(drop=True)

            else:
                df['game_id'] = None

        except Exception as e:
            self.describe_error(e)

        return df

    @staticmethod
    def clean_df(df):

        df = df.dropna(axis=1, how='all')
        df = df.dropna(axis=0, how='all')

        return df
