from Sources.Source import Source
from Sources.settings_classes import sett_classes
from datetime import datetime, timedelta
import pandas as pd


class Betway(Source):

    def __init__(self, sett, worker, link, odds_columns):
        super().__init__(sett)

        self.sett = sett
        self.worker = worker
        self.link = link
        self.odds_columns = odds_columns

        self.source_name = self.__class__.__name__.lower()
        self.classes = sett_classes.get(self.source_name)
        self.workers_open_browser = ['odds', 'trader']

        self.null_odds_symbol = '-'
        self.min_before_games_starting = 17

        self.min_refresh_browser = {'odds': 6 * 60}
        self.min_update_page = {'odds': 3 * 60}

        self.scraper = 'selenium'
        self.scraping = self.open_browser_depending_on_worker(self.link, self.scraper, self.workers_open_browser,
                                                              self.worker)
        self.prepare_for_data_gathering()

        self.data_collector_path = f'DataCollector\\support_dfs\\{self.source_name}'
        self.path_scores = f'{self.data_collector_path}\\scores.feather'
        self.path_games = f'{self.data_collector_path}\\df_not_registered.feather'
        self.system.check_paths([self.data_collector_path])

        self.switch_cols_home_first = {'team_1': 'team_2', 'team_2': 'team_1', 'goal_team_1': 'goal_team_2',
                                       'goal_team_2': 'goal_team_1', 'back_win': 'back_defeat',
                                       'back_defeat': 'back_win'}

    # Opening hidden info:
    def prepare_for_data_gathering(self):
        self.open_closed_comp_box() if self.worker in self.workers_open_browser else None

    def open_closed_comp_box(self):
        self.scraping.open_hidden_content(self.classes.element_used_to_check_if_content_is_hidden)

    # Collection Odds:
    def saving_odds(self, sql, odds_obj, df, time_start_insertion):

        try:

            raw_df = df
            df, df_registered, df_not_registered, time_end_insertion = odds_obj.main_odds(sql, df)
            self.report_odds(sql, df, df_registered, df_not_registered, time_start_insertion, time_end_insertion)
            self.prepare_for_games(raw_df)
            self.open_closed_comp_box()

            return time_end_insertion

        except Exception as e:
            self.describe_error(e)
            return None

    def prepare_for_games(self, df):
        self.feather.saving_df(df, self.path_games)

    # Collecting Games:
    def saving_games(self, sql, games_obj):
        start = datetime.now()
        games_obj.saving_games_from_file(sql, self.path_games)
        return start, datetime.now()

    # Collecting Scores:
    @staticmethod
    def saving_scores(sql, scores_obj):
        scores_obj.main_scores(sql)

    # Turning into df:
    def get_soup(self):
        return self.scraping.get_soup()

    def get_df(self, soup):

        try:
            df = self.turn_soup_into_df(soup)
            df['start_date'] = datetime.now() + timedelta(minutes=self.min_before_games_starting)
            df = self.specific_treatment(df, soup)

        except IndexError:
            df = pd.DataFrame()

        return df

    # Specific Treatment:
    def specific_treatment(self, df, soup):

        df = df.copy()
        df = self.get_odds(df)
        df = self.treating_game_time(df)
        df = self.treatment_e_soccer(df)

        return df

    def get_odds(self, df):
        columns_with_odds = ['back_win', 'back_draw', 'back_defeat']
        return self.soup_to_df.get_odds(df, columns_with_odds, self.null_odds_symbol) if self.worker == 'odds' else df

    @staticmethod
    def treatment_e_soccer(df):

        keyword = 'soccer'
        mask = ~df['competition'].str.contains(keyword, case=False, na=False)
        df_filtered = df.loc[mask].reset_index(drop=True)

        return df_filtered

    @staticmethod
    def treating_game_time(df):

        mask = df['game_time'].str.contains('Aguardando começo,')

        df.loc[~mask, 'game_time'] = (
            df.loc[~mask, 'game_time']
            .str.split(',')
            .str[1]
            .str.rstrip()
            .replace("", "0'")
            .str[:-1]
        )

        df.loc[~mask, 'game_time'] = (
                pd.to_numeric(df.loc[~mask, 'game_time'], errors='coerce') * 60
        ).replace(0, None)

        df.loc[mask, 'game_time'] = None

        return df

    # Checking Downtime:
    # TODO: Chamaer em algum lugar. Vem onde levanta o erro
    def check_downtime(self):
        self.scraping.click(self.classes.downtime)

    # Refreshing pages:
    def refresh_browser(self):

        if self.scraping is None:
            return

        self.scraping.close_browser()
        self.scraping = self.open_browser_depending_on_worker(self.link,
                                                              self.scraper,
                                                              self.workers_open_browser,
                                                              self.worker)

    def update_page(self):

        if self.scraping is None:
            return

        self.scraping.refresh()
        self.open_closed_comp_box()
