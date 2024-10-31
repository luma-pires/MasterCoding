from DataCollector.AfterDf.Data import DataCollector
from datetime import datetime


class ScoresCollector(DataCollector):

    def __init__(self, sql, sett):

        super().__init__(sql, sett)
        self.sett = sett
        self.sql = sql
        self.sec_control_delay = 5
        self.data_collector_path = f'DataCollector\\support_dfs\\{self.source_name}'
        self.path_scores = f'{self.data_collector_path}\\scores.feather'

    def saving_df_odds_in_file(self, df):
        self.feather.saving_df(df, self.path_scores)

    def saving_scores(self, sql, df_finished):

        try:

            tbl_name = 'scores_' + self.sport_name.lower()

            df_finished = df_finished[df_finished['game_id'].notna()]

            columns = ['game_id', 'goal_team_1', 'goal_team_2']
            df_finished = df_finished[columns]
            df_finished = df_finished.dropna(subset=columns)

            games_with_scores = sql.get_df_time_window(self.source_name, tbl_name, 'ref_date_db', 3*60, 3*60)
            games_with_scores = list(games_with_scores['game_id'])

            df_finished = df_finished[~df_finished['game_id'].isin(games_with_scores)]

            if not df_finished.empty:

                df_finished = df_finished.reset_index(drop=True)
                df_finished = self.insert_additional_info_in_df(df_finished)

                control = sql.insert_data(df_finished, self.source_name, tbl_name, id_increment=False)
                self.acknowledger.acknowledge(sql, control, "insert_scores")
                self.color_print.green_print(f'\n{len(df_finished)} new scores collected - {datetime.now()}\n')

        except Exception as e:
            self.describe_error(e)

    def main_scores(self, sql):

        df_finished = self.getting_info_from_file(self.path_scores)

        while not df_finished.empty:

            start = datetime.now()
            self.saving_scores(sql, df_finished)
            end = datetime.now()

            df_finished = self.cleaning_file_after_saving_new_info(df_finished, end, start, self.path_scores)

        print(f'There is not finished games to register - {datetime.now()}')
