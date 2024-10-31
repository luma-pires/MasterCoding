from Root_Classes.Info import Info
from Root_Classes.Staff import Staff
from Registration.Roles import dict_objs
from Registration.Source import dict_sources
from Acknowledger.roles_acknowledger import Roles_Acknowledger
from datetime import datetime, timedelta
from SQL.SQL import SQL
import warnings
import sys


class Manager(Info, Staff):

    def __init__(self, sett):

        super().__init__(sett)

        self.sett = sett
        self.dict_main = {'odds': self.main_odds, 'games': self.main_games, 'scores': self.main_scores}
        self.acknowledger = Roles_Acknowledger(self.source_id, self.sport_id, self.machine_id, self.version_id)
        self.current_worker = None
        self.source_obj = None
        self.worker_obj = None
        self.n_start_refreshing = None
        self.n_start_updating = None
        self.link = None
        self.odds_columns = None
        self.dict_args_objs = None
        self.dict_objs = dict_objs

    def manage(self, current_worker):

        print(f'Start {current_worker} - {datetime.now()}')

        self.sql = SQL(self.sett.sql)
        self.initial_checking()
        self.odds_columns = self.get_headers_odds()
        self.get_neutral_source_obj()

        self.dict_args_objs = {
            'odds': [self.sql, self.sett, self.source_obj.switch_cols_home_first, self.odds_columns],
            'games': [self.sql, self.sett],
            'scores': [self.sql, self.sett]
        }

        self.current_worker = current_worker
        self.link = self.get_links()[self.current_worker]
        self.get_source_object()
        self.get_worker_object()
        self.sql.conn.close()

        while True:

            try:

                self.n_start_refreshing = datetime.now()
                self.n_start_updating = datetime.now()
                self.sql = SQL(self.sett.sql)

                # Main depending on worker:
                control, start, end = self.dict_main[self.current_worker](self.sql)
                self.acknowledger.acknowledge(self.sql, control, f"main_{self.current_worker}")

                # Update/Refresh?
                end = self.refresh_and_updating_pages_and_browser(self.source_obj, end)
                self.sleep_depending_on_worker(self.source_obj, start, end)
                self.sql.close_connection()

            except Exception as e:
                self.describe_error(e)
                self.restart_manage()
                self.sql.conn.commit()
                self.sql.close_connection()

    def get_neutral_source_obj(self):
        self.source_obj = dict_sources()[self.source_name](self.sett, 'scores', None, self.odds_columns)

    def initial_checking(self):
        self.disable_warnings()
        self.check_source()
        self.versions.check_if_version_is_in_db(self.version_id, self.machine_id, self.sql)

    def sleep_depending_on_worker(self, source_obj, start, end):
        frequency = source_obj.sec_sleep.get(self.current_worker)
        self.sleep.sleep(start, end, frequency / 60) if frequency is not None else None

    def get_source_object(self):

        while True:

            try:
                self.source_obj = dict_sources()[self.source_name](
                    self.sett, self.current_worker, self.link, self.odds_columns)
                return

            except Exception as e:
                self.describe_error(e)
                self.restart_manage()

    def get_worker_object(self):
        self.worker_obj = self.dict_objs[self.current_worker](*self.dict_args_objs[self.current_worker])

    # # # Mains:
    def main_odds(self, sql):

        control = False
        start = end = datetime.now()

        try:

            time_start_insertion = datetime.now()

            soup = self.source_obj.get_soup()
            df = self.source_obj.get_df(soup)

            if df is None:
                end = time_start_insertion + timedelta(seconds=self.general.sec_frequency_collection)

            elif df.empty:
                # TODO: Colocar algum controle para saber se não tem nenhum jogo mesmo ou se o site os escondeu.
                #  No último caso, atualizar a página/refresh browser
                print(f'{datetime.now()} - There are no games happening right now')
                end = datetime.now()

            else:
                control = True
                end = self.source_obj.saving_odds(sql, self.worker_obj, df, time_start_insertion)

        except TypeError:
            self.sql.conn.commit()
            self.sql = SQL(self.sett.sql)

        except Exception as e:
            self.describe_error(e)
            self.refresh_browser()

        return control, start, end

    def main_games(self, sql):

        start = datetime.now()
        control = False

        try:
            start, end = self.source_obj.saving_games(sql, self.worker_obj)
            control = True

        except Exception as e:
            self.describe_error(e)

        return control, start, datetime.now()

    def main_scores(self, sql):

        start = datetime.now()
        control = False

        try:
            self.source_obj.saving_scores(sql, self.worker_obj)
            control = True

        except Exception as e:
            self.describe_error(e)

        return control, start, datetime.now()

    # Warnings:
    @staticmethod
    def disable_warnings():
        warnings.filterwarnings("ignore", category=FutureWarning)
        warnings.filterwarnings("ignore",
                                message="pandas only supports SQLAlchemy connectable.*",
                                category=UserWarning)

    # Checks:
    def check_source(self):
        possible_sources = list(self.sql.get_df('main', 'sources')['source_name'])
        if self.source_name not in possible_sources:
            self.color_print.red_print(f"ERROR: {self.source_name} isn't a valid name. Set a valid source name at "
                                       f"config file.")
            sys.exit()

    # # # Getting Info:

    # Links:
    def get_links(self):
        links = self.sql.query(f"SELECT which_data, link FROM main.links WHERE source_id = {self.source_id}"
                               f" AND sport_id = {self.sport_id}")
        dict_links = dict(zip(links['which_data'], links['link']))
        return dict_links

    # Headers:
    def get_headers_odds(self):
        headers = self.sql.get_header(self.source_name, 'odds')
        return headers

    # # # Refresh + Update:

    # Refreshing page:
    def update_page(self):
        self.source_obj.update_page()

    def refresh_browser(self):
        self.source_obj.refresh_browser()

    def refresh_and_updating_pages_and_browser(self, source_obj, time_end_insertion):

        if self.current_worker not in source_obj.workers_open_browser:
            return datetime.now()

        refreshing = False

        try:

            refresh = (datetime.now() - self.n_start_refreshing >=
                       timedelta(minutes=self.source_obj.min_refresh_browser[self.current_worker]))

            if refresh:
                self.n_start_refreshing = datetime.now()
                self.refresh_browser()
                time_end_insertion = datetime.now()
                refreshing = True

        except KeyError:
            pass

        try:

            update = (datetime.now() - self.n_start_updating >= timedelta(
                minutes=self.source_obj.min_update_page[self.current_worker]))

            if update and not refreshing:
                self.n_start_updating = datetime.now()
                self.update_page()
                time_end_insertion = datetime.now()

        except KeyError:
            pass

        return time_end_insertion

    def restart_manage(self):

        try:
            self.source_obj.scraping.close_browser()
            self.get_source_object()
        except AttributeError:
            pass
