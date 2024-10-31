from Root_Classes.Info import Info
from DataCollector.BeforeDf.Outliers import Outliers
from Sources.settings_classes import sett_classes
from DataCollector.BeforeDf.Soup import SoupToDf
from Registration.Scraper import dict_scrapers
from Acknowledger.report_acknowledger import Report_Acknowledger
import pandas as pd
from datetime import datetime


class Source(Info, Outliers):

    def __init__(self, sett):

        super().__init__(sett)

        self.sett = sett
        self.classes = sett_classes
        self.soup_to_df = SoupToDf(self.classes.get(self.source_name))

        self.sec_sleep = {'odds': self.general.sec_frequency_collection}
        self.support_dfs_path = f'DataCollector\\support_dfs'
        self.acknowledger = Report_Acknowledger(self.source_id, self.sport_id, self.machine_id, self.version_id)

    def turn_soup_into_df(self, soup):
        return self.soup_to_df.get_df(soup)

    def open_browser_depending_on_worker(self, link, scraper, workers_open_browser, worker):
        return dict_scrapers()[scraper](self.sett, link, self.classes) if worker in workers_open_browser else None

    @staticmethod
    def get_only_live_games(df):
        df = df.dropna(subset=['goal_team_1', 'goal_team_2'])
        return df

    def get_soup(self):
        pass

    def get_df(self, soup):
        pass

    def specific_treatment(self, df, soup):
        pass

    def report_odds(self, sql, df, df_registered, df_not_registered, time_start_insertion, time_end_insertion):

        report = pd.DataFrame(columns=['total games opened',
                                       'total games collected',
                                       'finished odds',
                                       'left games',
                                       'datetime',
                                       'delay'])

        df = self.get_only_live_games(df)
        df_not_registered = self.get_only_live_games(df_not_registered)
        df_registered = self.get_only_live_games(df_registered)

        total_games_opened = len(df)
        finished_odds = len(df[df['back_win'] == self.soup_to_df.finished])
        total_games_collected = len(df_registered) - finished_odds
        left_games = len(df_not_registered)
        delay = time_end_insertion - time_start_insertion

        report.loc[0] = [total_games_opened,
                         total_games_collected,
                         finished_odds,
                         left_games,
                         datetime.now(),
                         delay]

        self.color_print.green_print(f"\n{'*' * 20} Odds Report {'*' * 20}\n{report.iloc[0]}\n{'*' * 53}")
        self.acknowledger.acknowledge(sql, 'insert_odds', total_games_opened, total_games_collected, delay)

    def saving_odds(self, sql, odds_obj, df, time_start_insertion):
        pass

    def saving_games(self, sql, games_obj):
        pass

    @staticmethod
    def saving_scores(sql, scores_obj):
        pass

    def update_page(self):
        pass

    def refresh_browser(self):
        pass
