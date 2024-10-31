from Sources.Source import Source
import time
import pandas as pd
from datetime import datetime, timedelta
from Scraping.Autogui.Autogui import Autogui
from Sources.settings_classes import sett_classes
from Functions.system import System
from Functions.feather import Feather
from Functions.color_print import ColorPrint


class bet365(Source):

    def __init__(self, dict_workers, link, sport, odds_columns, finished_odds_alternative):
        super().__init__()
        self.source_name = "bet365"
        self.sport = sport.lower()
        self.workers = dict_workers
        self.link = link
        self.scraping = Autogui(self.link)
        self.prepare_for_data_gathering_depending_on_worker()
        self.classes = sett_classes.get(self.source_name)
        self.min_update_page = {'odds': 3*60, 'trading': 20}
        self.min_refresh_browser = {'odds': 6*60, 'trading': 1*60}
        self.odds_columns = odds_columns
        self.min_before_games_starting = 15
        self.finished = finished_odds_alternative
        self.switch_cols_home_first = {'team_1': 'team_2', 'team_2': 'team_1', 'goal_team_1': 'goal_team_2',
                                       'goal_team_2': 'goal_team_1', 'back_win': 'back_defeat',
                                       'back_defeat': 'back_win'}
        self.system = System()
        self.feather = Feather()
        self.color_print = ColorPrint()
        self.data_collector_path = f'DataCollector\\support_dfs\\{self.source_name}'
        self.path_games = f'{self.data_collector_path}\\df_not_registered.feather'
        self.path_competitions = f'{self.data_collector_path}\\opened_competitions.feather'
        self.path_scores = f'{self.data_collector_path}\\scores.feather'

    def prepare_for_data_gathering_depending_on_worker(self):
        if self.workers['games'] or self.workers['scores']:
            self.scraping.close_browser()

    # Collection Odds:
    def saving_odds(self, odds_obj, df, time_start_insertion):

        raw_df = df
        df, df_registered, df_not_registered, time_end_insertion = odds_obj.main_odds(df)
        self.report_odds(df, df_registered, df_not_registered, time_start_insertion, time_end_insertion)
        self.prepare_for_games(raw_df)

        return time_end_insertion

    # Collecting Scores:
    @staticmethod
    def saving_scores(scores_obj):
        scores_obj.main_scores()

    # Collecting Games:
    def saving_games(self, games_obj):

        df_not_registered = self.getting_not_registered_games_df()
        competitions = self.feather.retrieving_df(self.path_competitions)

        if df_not_registered.empty:
            print(f'{datetime.now()} - No new games or teams to register yet')

        while not df_not_registered.empty:

            start = datetime.now()
            games_obj.main_games(df_not_registered, list(set(competitions['competition'])))
            end = datetime.now()

            df_not_registered = self.getting_not_registered_games_df()

            if datetime.now() - df_not_registered['ref_date_db'][0] > end - start:
                self.feather.saving_df(pd.DataFrame(), self.path_games)
                df_not_registered = self.getting_not_registered_games_df()

    # Getting soup:
    def get_soup(self):

        soup = self.scraping.get_soup()
        soup = self.checking_loading(soup)
        sport = self.getting_page_sport(soup)
        soup = self.checking_link(soup, sport)

        return soup

    def checking_loading(self, soup):

        loading_symbol = soup.find_all(self.classes.loading_symbol)

        while loading_symbol:

            self.refresh_browser()
            soup = self.scraping.get_soup()
            loading_symbol = soup.find_all(self.classes.loading_symbol)

        return soup

    def checking_link(self, soup, sport):

        while sport != self.sport:

            self.scraping.get_link(self.link)
            soup = self.scraping.get_soup()
            sport = self.getting_page_sport(soup)

        return soup

    def getting_page_sport(self, soup):

        while True:

            try:

                sport = soup.find_all('div', class_=self.classes.sport)
                return sport[0].text.lower()

            except IndexError:
                return None

            except Exception as e:
                print(e)
                return None

    # Extracting from HTML:
    def getting_teams(self, box):

        teams = box.find_all(name='div', class_=self.classes.box_teams)[0]

        team_1 = teams.find_all(name='div', class_=self.classes.team_name)[0].text
        team_2 = teams.find_all(name='div', class_=self.classes.team_name)[1].text

        return team_1, team_2

    def getting_goals(self, box):

        goals_1 = int(box.find('div', class_=self.classes.goal_home).text)
        goals_2 = int(box.find('div', class_=self.classes.goal_away).text)

        return goals_1, goals_2

    def getting_running_time(self, box):

        running_time = box.find('div', class_=self.classes.game_time)

        try:
            running_time = running_time.text
            return running_time

        except (AttributeError, ValueError):
            return None

    @staticmethod
    def transforming_running_time_into_seconds(str_running_time):

        if not str_running_time:
            return None

        minutes, seconds = map(int, str_running_time.split(':'))
        return minutes * 60 + seconds

    def getting_odds(self, box):

        # Win, draw and defeat?
        if box.find('div', self.classes.alternative_odds) is not None:
            back_win = back_draw = back_defeat = self.finished

        # Hidden:
        elif box.find('div', class_=self.classes.suspended_odds) is not None:
            back_win = back_draw = back_defeat = None

        # Real odds:
        else:

            odds = box.find_all('span', class_=self.classes.odds_box)

            if odds:

                back_win = float(odds[0].text)
                back_draw = float(odds[1].text)
                back_defeat = float(odds[2].text)

            else:
                back_win = back_draw = back_defeat = None

        return back_win, back_draw, back_defeat

    def getting_competitions(self, soup):

        box_competitions = soup.find_all('div', class_=self.classes.competition_box)

        list_comps = [
            (box.find('div', class_=self.classes.competition_name
                      ).text if box.find('div', class_=self.classes.competition_name) is not None else None)
            for box in box_competitions
            for _ in range(len(box.find_all('div', class_=self.classes.team_name)) // 2)
        ]

        return list_comps

    def get_df(self, soup):

        ref_date_db = datetime.now()
        boxes = soup.find_all('div', class_=self.classes.boxes)

        if len(boxes) == 0:
            print(f'{self.source_name} | No game occurring right now - {datetime.now()}')
            return pd.DataFrame()

        try:

            data = [
                {
                    'team_1': self.getting_teams(box)[0],
                    'team_2': self.getting_teams(box)[1],
                    'game_time': self.getting_running_time(box),
                    'goal_team_1': self.getting_goals(box)[0],
                    'goal_team_2': self.getting_goals(box)[1],
                    'back_win': self.getting_odds(box)[0],
                    'back_draw': self.getting_odds(box)[1],
                    'back_defeat': self.getting_odds(box)[2]
                }

                for box in boxes
            ]

            time.sleep(0.025)
            df = pd.DataFrame().from_records(data)

            df['competition'] = self.getting_competitions(soup)
            df = df.dropna(subset=['competition']).reset_index(drop=True)
            df['ref_date_db'] = ref_date_db
            df['start_date'] = datetime.now() + timedelta(minutes=self.min_before_games_starting)

            df = self.specific_treatment(df)

            return df

        except Exception as e:
            self.describe_error(e)
            return None

    # Row none:
    def row_all_none(self):
        return {key: None for key in self.odds_columns()}

    # Specific Treatment:
    def specific_treatment(self, df):
        df = df.copy()
        df = self.drop_missing_odds(df)
        df = self.treatment_e_soccer(df)
        df = self.treatment_datetime(df)
        return df

    @staticmethod
    def drop_missing_odds(df):

        col_odds = ['back_win', 'back_draw', 'back_defeat']
        df = df.dropna(subset=col_odds).reset_index(drop=True)

        return df

    @staticmethod
    def treatment_e_soccer(df):
        keyword = 'soccer'
        df = df.copy()
        real_competitions = [keyword not in i.lower() if i is not None else i for i in list(df['competition'])]
        df['real'] = real_competitions
        df = df.loc[df['real']]
        del df['real']
        df.reset_index(drop=True, inplace=True)
        return df

    def treatment_datetime(self, df):
        df = df.copy()
        df['game_time'] = df['game_time'].apply(self.transforming_running_time_into_seconds)
        return df

    # Games:
    def prepare_for_games(self, df):
        self.feather.saving_df(df, self.path_games)
        self.feather.saving_df(pd.DataFrame({'competition': list(set(df['competition']))}), self.path_competitions)

    def getting_not_registered_games_df(self):
        return self.feather.retrieving_df(self.path_games)

    # Report:
    def report_odds(self, df, df_registered, df_not_registered, time_start_insertion, time_end_insertion):

        if not df.empty:
            report = pd.DataFrame(columns=['total games opened',
                                           'total games collected',
                                           'finished odds',
                                           'left games',
                                           'datetime',
                                           'delay'])

            finished_odds = len(df[df['back_win'] == self.finished])
            total_games_opened = len(df) - finished_odds

            total_games_collected = len(df_registered) - finished_odds
            total_games_collected = total_games_collected if total_games_collected > 0 else 0

            left_games = len(df_not_registered)
            delay = time_end_insertion - time_start_insertion

            report.loc[0] = [total_games_opened,
                             total_games_collected,
                             finished_odds,
                             left_games,
                             datetime.now(),
                             delay]

            self.color_print.green_print(f"\n{'*' * 20} Odds Report {'*' * 20}\n{report.iloc[0]}\n{'*' * 53}")

    # Refreshing pages:
    def refresh_browser(self):
        self.scraping.close_browser()
        self.open_first_sheet_browser()

    def update_page(self):
        self.scraping.refresh_sheet()

    def open_first_sheet_browser(self):
        self.scraping.open_browser()
        self.scraping.get_link(self.link)
