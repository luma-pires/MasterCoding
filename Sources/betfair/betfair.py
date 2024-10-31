from Sources.Source import Source
from Sources.settings_classes import sett_classes
from datetime import datetime, timedelta
import pandas as pd
import time


class Betfair(Source):

    def __init__(self, sett, worker, link, odds_columns):
        super().__init__(sett)

        self.sett = sett
        self.worker = worker
        self.link = link
        self.odds_columns = odds_columns

        self.source_name = self.__class__.__name__.lower()
        self.classes = sett_classes.get(self.source_name)
        self.workers_open_browser = ['odds', 'games', 'trader']

        self.null_odds_symbol = ''
        self.min_before_games_starting = 15

        self.min_refresh_browser = {'odds': 6 * 60, 'games': 1 * 60}
        self.min_update_page = {'odds': 10 * 60}

        self.last_refresh_games = datetime.now()

        self.scraper = 'selenium'
        self.scraping = self.open_browser_depending_on_worker(self.link, self.scraper, self.workers_open_browser,
                                                              self.worker)
        self.prepare_for_data_gathering()

        self.data_collector_path = f'SettingUp\\DataCollector\\support_dfs\\{self.source_name}'
        self.path_scores = f'{self.data_collector_path}\\scores.feather'
        self.system.check_paths([self.data_collector_path])

        self.link_second_page = f"{self.link}/2"
        self.freq_min_games_gathering = 5
        self.limit_future_games = 5
        self.sec_sleep['games'] = self.freq_min_games_gathering * 60

        self.amount_columns = [i for i in odds_columns if 'amount' in i.lower()]

        self.switch_cols_home_first = {'team_1': 'team_2', 'team_2': 'team_1', 'goal_team_1': 'goal_team_2',
                                       'goal_team_2': 'goal_team_1', 'back_win': 'back_defeat',
                                       'back_defeat': 'back_win', 'lay_win': 'lay_defeat', 'lay_defeat': 'lay_win',
                                       'amount_back_win': 'amount_back_defeat', 'amount_back_defeat': 'amount_back_win',
                                       'amount_lay_win': 'amount_lay_defeat', 'amount_lay_defeat': 'amount_lay_win'}

        self.datetime_gathering = None

    # # # # # # Basics:
    def prepare_for_data_gathering(self):

        dict_order = {'odds': 'date', 'games': 'competition', 'trader': 'date'}

        try:

            if self.worker in ['odds', 'games', 'trader']:
                self.order_data(dict_order[self.worker])

                if self.worker == 'odds':
                    self.get_second_page()

        except Exception as e:
            self.describe_error(e)
            return

    # # # # # # Collection Odds:
    def saving_odds(self, sql, odds_obj, df, time_start_insertion):

        try:

            df, df_registered, df_not_registered, time_end_insertion = odds_obj.main_odds(sql, df)
            self.report_odds(sql, df, df_registered, df_not_registered, time_start_insertion, time_end_insertion)
            return time_end_insertion

        except Exception as e:
            print(e)
            return None

    # # # # # # # Collecting Scores:
    def saving_scores(self, sql, scores_obj):
        scores_obj.main_scores(sql)

    # # # # # # # Collecting Games:
    def saving_games(self, sql, games_obj):

        start = datetime.now()

        df_games = [self.get_df(soup) for soup in self.scraping.getting_content_from_multiple_pages()]
        df_games = pd.concat(df_games, ignore_index=True)

        games_obj.main_games(sql, df_games, list(set(df_games['competition'])))

        print(f'Next: {datetime.now() + timedelta(minutes=self.freq_min_games_gathering)}')

        self.refresh_browser() if datetime.now() - self.last_refresh_games >= timedelta(
            minutes=self.min_refresh_browser[self.worker]) else False

        return start, datetime.now()

    # # # # # # Turning into df:
    def get_soup(self):
        return self.scraping.get_soup()

    def get_df(self, soup):

        df = self.turn_soup_into_df(soup)

        if self.worker == 'odds' and self.check_if_second_page_must_be_collected(soup):
            df = pd.concat([df, self.get_second_page_df()], axis=0, ignore_index=True)

        df = self.specific_treatment(df, soup)

        return df

    def get_second_page_df(self):
        self.go_to_second_page()
        soup_2 = self.scraping.get_soup()
        self.go_to_first_page()
        return self.turn_soup_into_df(soup_2)

    def specific_treatment(self, df, soup):
        df = df.copy()
        df = self.get_odds(df)
        df = self.get_remaining_info_from_soup(soup, df)
        df = self.treating_game_time(df)
        df = self.treatment_date(df)
        df = self.treatment_amounts(df)
        return df

    @staticmethod
    def treat_total_amount(df):
        mask = df['total_amount'] == 'R$0'
        return df[~mask].reset_index(drop=True)

    # Getting odds:
    def get_odds(self, df):
        columns_with_odds = ['back_win', 'lay_win', 'back_draw', 'lay_draw', 'back_defeat', 'lay_defeat']
        return self.soup_to_df.get_odds(df, columns_with_odds, self.null_odds_symbol) if self.worker == 'odds' else df

    # Remaining info:
    def get_remaining_info_from_soup(self, soup, df):

        game_boxes = self.soup_to_df.find_all(soup, self.classes.boxes)

        df = self.get_start_data(df, game_boxes) if self.worker == 'games' else df
        df = self.get_amounts(df, game_boxes) if self.worker == 'odds' else df

        df = self.treat_total_amount(df) if self.worker == 'odds' else df

        return df

    def get_start_data(self, df, game_boxes):
        data = [{'start_date': self.get_start_date(game_box)} for game_box in game_boxes]
        return pd.concat([df, pd.DataFrame(data)], axis=1)

    def get_amounts(self, df, game_boxes):

        data = [
            {
                'total_amount': self.get_total_amount(game_box),
                'amount_back_win': self.get_back_amounts(game_box)[0],
                'amount_back_draw': self.get_back_amounts(game_box)[1],
                'amount_back_defeat': self.get_back_amounts(game_box)[2],
                'amount_lay_win': self.get_lay_amounts(game_box)[0],
                'amount_lay_draw': self.get_lay_amounts(game_box)[1],
                'amount_lay_defeat': self.get_lay_amounts(game_box)[2]
            }
            for game_box in game_boxes
        ]

        return pd.concat([df, pd.DataFrame(data)], axis=1)

    def get_lay_odds(self, game_box):
        odds_lay = [self.soup_to_df.find(n, self.classes.odds_lay_value).text for n in game_box]
        return [None if item == '' else item for item in odds_lay]

    def get_total_amount(self, game_box):
        matched_amount = self.soup_to_df.find(game_box, self.classes.matched_amount)
        return matched_amount if matched_amount is not None else None

    def get_back_amounts(self, game_box):
        return [self.soup_to_df.find(n, self.classes.amount_back) if self.soup_to_df.find(
            n, self.classes.amount_back) is not None else None for n in self.soup_to_df.find_all(
            game_box, self.classes.box_odds_back)]

    def get_lay_amounts(self, game_box):
        return [self.soup_to_df.find(n, self.classes.amount_lay) if self.soup_to_df.find(
            n, self.classes.amount_lay) is not None else None for n in self.soup_to_df.find_all(
            game_box, self.classes.box_odds_lay)]

    def get_start_date(self, box):
        try:
            return self.soup_to_df.find(box, self.classes.start_date)
        except (AttributeError, TypeError, ValueError):
            return None

    # Treating game time:
    def treating_game_time(self, df):

        try:
            df = df.copy()
            df['game_time'] = df['game_time'].where(df['game_time'] != 'INT', "45'")

            mask = pd.isna(df['game_time'])
            df_none = df[mask]
            df_not_none = df[~mask]

            df_not_none = df_not_none.copy()
            df_not_none['game_time'] = [
                (int(item.split("'")[0]) * 60 + int(item.split("'")[1][1:])) if '+' in item else
                (int(item.split("'")[0]) * 60 if item not in ['Ao vivo', 'FIM'] else None)
                for item in list(df_not_none['game_time'])
            ]

            df_none = df_none.copy()
            df_none['game_time'] = None

            return pd.concat([df_not_none, df_none], axis=0)

        except Exception as e:

            self.describe_error(e)

            df['game_time'] = None
            return df

    # # # # # # Preparing for collection:
    def order_data(self, by):

        while True:

            dict_order = {"date": self.classes.order_by_date,
                          "competition": self.classes.order_by_competition,
                          "amount": self.classes.box_order_page}

            self.scraping.click(self.classes.box_order_page)
            self.scraping.click(dict_order[by])
            time.sleep(self.sett_machines.sec_sleep_load_page)

            if self.check_if_is_correctly_ordered(by):
                return
            else:
                print(f'{datetime.now()} - Error in ordering page. Trying again...')
                self.update_page()

    def check_if_is_correctly_ordered(self, by):

        dict_check = {"date": "Data",
                      "competition": "Competição",
                      "amount": "Montante Correspondido"}

        current_order = self.soup_to_df.find(self.scraping.get_soup(), self.classes.selected_option)

        return True if current_order.strip().lower() == dict_check[by].lower() else False

    # Dealing with second page:
    def get_second_page_link(self):
        return f"{self.link}/2"

    def get_second_page(self):

        if self.scraping.get_number_of_sheets() == 1:
            self.scraping.open_new_sheet()

        self.scraping.go_to_next_sheet()
        self.scraping.get_link(self.get_second_page_link())
        self.order_data("date")
        self.scraping.go_to_original_sheet()

    def check_if_second_page_must_be_collected(self, soup):

        n_pages = self.get_number_of_pages(soup)
        n_live = sum(1 for span in soup.find_all('span',
                                                 class_='label') if span.get_text(strip=True).lower() == 'ao vivo')
        n_future_games = len(soup.find_all('div', class_='start-date-wrapper')) - n_live

        if n_pages == 1:
            return False
        elif n_future_games > self.limit_future_games:
            return False
        else:
            return True

    def get_number_of_pages(self, soup):
        n_pages = len(self.soup_to_df.find_all(soup, self.classes.pages))
        return 1 if n_pages == 0 else n_pages

    def go_to_first_page(self):
        self.scraping.go_to_original_sheet()

    def go_to_second_page(self):
        if self.scraping.go_to_next_sheet():
            self.get_second_page()
            self.scraping.go_to_next_sheet()

    # # # # # # Specific Treatment:

    # Datetime:
    def treatment_date(self, df_all):

        if not self.worker == 'games':
            return df_all

        self.datetime_gathering = df_all.loc[0, 'ref_date_db'].replace(second=0, microsecond=0)

        dict_start_date_values = [self.treat_date_data(k) for k in list(df_all['start_date'])]
        df_all['start_date'] = pd.Series(dict_start_date_values)
        df_all.replace(pd.NaT, None, inplace=True)
        return df_all

    def treat_date_data(self, x):
        try:
            return self.clean_valid_date_data(x) if self.check_words_in_date_data(x) else None
        except (KeyError, AttributeError, ValueError, IndexError):
            return None
        except Exception as e:
            self.describe_error(e)

    def clean_valid_date_data(self, x):

        try:

            if x.lower().startswith('começa'):
                return self.datetime_gathering + timedelta(minutes=int(x.split(' ')[-1][:-1]))

            elif x[-3] == ':':
                day = x[:-6]
                datetime_str = x[-5:]
                return self.treat_date(day).replace(hour=int(datetime_str[:2]), minute=int(datetime_str[-2:]),
                                                    second=0, microsecond=0)
            else:
                return None

        except Exception as e:
            self.describe_error(e)

    def treat_date(self, date_str):

        dict_months = {'jan': 1, 'fev': 2, 'mar': 3, 'abr': 4, 'mai': 5, 'jun': 6,
                       'jul': 7, 'ago': 8, 'set': 9, 'out': 10, 'nov': 11, 'dez': 12}
        dict_week = {'seg': 0, 'ter': 1, 'qua': 2, 'qui': 3, 'sex': 4, 'sáb': 5, 'dom': 6}

        len_date = len(date_str)

        if len_date == 6:

            date_str = date_str.split(' ')
            month = dict_months[date_str[0]]
            day = int(date_str[1])
            year_now = datetime.now().year
            date = datetime(year=year_now, month=month, day=day)

            if date < datetime.now():
                date = datetime(year=year_now + 1, month=month, day=day)

            date_str = date

        elif len_date == 3:
            date = self.date_and_time.today_date() + timedelta(days=1)
            date_str = date if date.weekday() == dict_week[date_str] else date_str

        elif date_str.lower() == 'hoje às':
            date_str = self.date_and_time.today_date()

        return date_str

    @staticmethod
    def check_words_in_date_data(date_data):
        list_strings_in_date_data = ['ao vivo', 'em breve']
        return False if date_data.lower() in list_strings_in_date_data or date_data is None else True

    # Amounts:
    def treatment_amounts(self, df):

        if not self.worker == 'odds':
            return df

        df = df.copy()

        for col in self.amount_columns:

            try:
                df[col] = pd.to_numeric(df[col].replace({r'R\$': '', ',': '', '': 0}, regex=True),
                                        errors='coerce').fillna(0, downcast='infer').astype(int)
                df[col] = df[col].replace(0, None)

            except Exception as e:
                self.describe_error(e)

        mask = pd.isna(df['total_amount'])
        return df[~mask].reset_index(drop=True)

    # Refreshing page:
    def open_first_sheet_browser(self):
        self.scraping.open_browser()
        self.scraping.get_link(self.link)

    def update_page(self):

        self.scraping.refresh()

        if self.scraping.get_number_of_sheets() > 1:
            self.go_to_second_page()
            self.scraping.get_link(self.link_second_page)
            self.go_to_first_page()

    def refresh_browser(self):
        self.scraping.close_browser()
        self.open_first_sheet_browser()
        self.prepare_for_data_gathering()
        self.last_refresh_games = datetime.now()
