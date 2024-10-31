import pandas as pd
from DataCollector.AfterDf.Scores import ScoresCollector
from DataCollector.AfterDf.Data import DataCollector
from DataCollector.BeforeDf.Soup import SoupToDf
from datetime import datetime


class OddsCollector(DataCollector):

    def __init__(self, sql, sett, columns_switch_home_first, columns_odds):
        super().__init__(sql, sett)

        self.sett = sett
        self.sql = sql
        self.cols_home_first = columns_switch_home_first
        self.cols_odds = columns_odds
        self.scores_obj = ScoresCollector(self.sql, self.sett)
        self.odds_data_with_competitions = {'betfair': False, 'bet365': True}
        self.finished = SoupToDf.finished

    def main_odds(self, sql, df):

        try:

            df = df.drop_duplicates(subset=['team_1', 'team_2']).reset_index(drop=True)
            df_final = self.get_only_live_games(df)

            if not df_final.empty:

                df_final = self.treatment_strings(df_final)
                df_final = self.treatment_home_first(df_final, self.cols_home_first)

                df_registered, df_not_registered = self.get_ids(sql, df_final)
                time_end_insertion = self.saving_odds(sql, df_registered)

                df_registered = self.saving_finished_games(df_registered)

                return df, df_registered, df_not_registered, time_end_insertion

            else:
                return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), datetime.now()

        except Exception as e:
            self.describe_error(e)

    @staticmethod
    def get_only_live_games(df):

        df = df.drop_duplicates(subset=['team_1', 'team_2']).reset_index(drop=True)
        df = df.dropna(subset=['goal_team_1', 'goal_team_2'])

        if df.empty:
            print(f'{datetime.now()} - There are no games happening right now')
            return pd.DataFrame()

        return df.reset_index(drop=True)

    @staticmethod
    def is_float(value):
        return isinstance(value, float)

    def check_cols_int(self, df, list_cols):

        mask_int = [list(df[col].apply(self.is_float)) for col in list_cols]
        mask_int = [all(sublist[i] for sublist in mask_int) for i in range(len(mask_int[0]))]

        df_registered = df[pd.Series(mask_int)].reset_index(drop=True)
        df_not_registered = df[~pd.Series(mask_int)].reset_index(drop=True)

        return df_registered, df_not_registered

    def split_into_registered_and_not_registered(self, df):

        df_registered, df_not_registered = self.check_cols_int(df, ['team_1', 'team_2'])

        if self.odds_data_with_competitions[self.source_name]:

            df_registered_comps_none = df_registered.loc[df_registered['competition_id'].isna()].reset_index(drop=True)
            df_registered_comps_not_none = df_registered.loc[
                df_registered['competition_id'].notna()].reset_index(drop=True)

            if not df_registered_comps_none.empty:
                df_registered = df_registered_comps_not_none
                df_not_registered = pd.concat([df_not_registered, df_registered_comps_none], axis=0,
                                              ignore_index=True).reset_index(drop=True)

        return df_registered, df_not_registered

    def get_ids(self, sql, df):

        try:

            df['sport_id'] = self.sport_id

            df_final = self.get_team_ids(sql, df)
            df_final = self.get_competition_ids(sql, df_final)

            df_final = self.get_game_ids(sql, df_final)

            df_final = df_final.rename(columns={'competition': 'competition_id'})

            mask = pd.notna(df_final['game_id'])

            df_registered = df_final[mask]
            df_not_registered = df_final[~mask]

            return df_registered, df_not_registered

        except Exception as e:
            self.describe_error(e)

    def saving_odds(self, sql, df_registered):

        if not df_registered.empty:

            df_registered = df_registered.copy()

            df_registered.loc[:, 'currency_id'] = self.currency_id
            df_registered = self.insert_additional_info_in_df(df_registered)

            df_registered = df_registered[self.cols_odds]
            control = sql.insert_data(df_registered, self.source_name, 'odds', id_increment=False)
            self.acknowledger.acknowledge(sql, control, "insert_odds")

        time_end_insertion = datetime.now()

        return time_end_insertion

    def saving_finished_games(self, df):

        if df.empty:
            return df

        mask_df_finished = df['back_win'] == self.finished
        df_finished = df[mask_df_finished].reset_index(drop=True)

        if not df_finished.empty:

            self.scores_obj.saving_df_odds_in_file(df_finished)
            self.color_print.yellow_print(f'\nSaved scores - {datetime.now()}\n')

            df = df[~mask_df_finished].reset_index(drop=True)

        return df
