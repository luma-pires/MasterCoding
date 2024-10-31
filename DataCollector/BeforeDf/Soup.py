from Root_Classes.Staff import Staff
from datetime import datetime
import pandas as pd
import time
import numpy as np


class SoupToDf(Staff):

    finished = -2

    def __init__(self, classes):
        super().__init__()
        self.classes = classes
        self.file_name = 'Soup.py'

    def find(self, soup, class_dict):

        div = class_dict[0]
        label = class_dict[1]

        try:

            element = soup.find(div, class_=label) if not isinstance(label, dict) else soup.find(div, attrs=label)
            element = self.check_element_class(element, label)

            return element.text if element is not None else soup.find(div)[class_dict].text

        except (AttributeError, IndexError, TypeError):
            return None

    @staticmethod
    def check_element_class(element, target_class):
        return None if ' '.join(element['class']) != target_class else element

    @staticmethod
    def get_exact_class(tag, div, class_name):
        return tag.name == div and tag.get('class') == [class_name]

    @staticmethod
    def find_all(soup, class_dict):
        try:
            label = class_dict[1]
            return soup.find_all(class_dict[0], class_=label
                                 ) if not isinstance(label, dict) else soup.find_all(class_dict[0], attrs=label)
        except IndexError:
            soup.find_all(class_dict[0])

    def get_df(self, soup):

        ref_date_db = datetime.now()
        boxes = self.find_all(soup, self.classes.boxes)

        if len(boxes) == 0:
            print(f'No game occurring right now - {datetime.now()}')
            return pd.DataFrame()

        while True:

            try:

                data = [
                    {
                        'team_1': self.getting_teams(box)[0],
                        'team_2': self.getting_teams(box)[1],
                        'game_time': self.getting_running_time(box),
                        'goal_team_1': self.getting_goal_home(box),
                        'goal_team_2': self.getting_goal_away(box),
                        'odds': self.getting_odds(box),
                    }
                    for box in boxes
                ]

                time.sleep(0.005)

                df = pd.DataFrame().from_records(data)

                df['competition'] = self.getting_competitions(soup)
                df['ref_date_db'] = ref_date_db

                return df

            except Exception as e:
                self.describe_error(e)
                return None

    def getting_teams(self, box):

        teams = self.find_all(box, self.classes.team_name)

        team_1 = teams[0].text.strip()
        team_2 = teams[1].text.strip()

        return team_1, team_2

    def getting_running_time(self, box):
        try:
            game_time = self.find(box, self.classes.game_time)
            return game_time if game_time is not None else None
        except AttributeError:
            return None

    def getting_goal_home(self, box):
        try:
            return int(self.find(box, self.classes.goal_home))
        except (AttributeError, TypeError):
            return None

    def getting_goal_away(self, box):
        try:
            return int(self.find(box, self.classes.goal_away))
        except (AttributeError, TypeError):
            return None

    def getting_odds(self, box):

        # Win, draw and defeat?
        if self.find(box, self.classes.finished_games) is not None:
            odds = [self.finished]

        # Hidden/Suspended:
        elif self.find(box, self.classes.suspended_odds) is not None:
            odds = [None]

        # Real odds:
        else:

            odds = self.find_all(box, self.classes.odds_box)
            odds = [odd.text for odd in odds]

        return odds

    @staticmethod
    def get_odds(df, list_odds, null_odds_symbol):

        df['odds'] = [i * len(list_odds) if len(i) == 1 else i for i in df['odds']]

        odds_array = np.array(df['odds'].tolist())
        mask = ~pd.isnull(odds_array)

        df[list_odds] = np.where(mask, odds_array, np.nan)
        df[list_odds] = df[list_odds].apply(lambda col: col.str.replace(',', '.', regex=False))
        df[list_odds] = df[list_odds].replace(null_odds_symbol, np.nan).astype(float)

        df = df.drop('odds', axis=1)
        df = df.dropna(subset=list_odds).reset_index(drop=True)

        return df

    def getting_competitions(self, soup):

        try:
            box_competitions = self.find_all(soup, self.classes.box_competitions)

            list_comps = [
                self.find_all(box, self.classes.competition_name) for box in box_competitions
            ]

            list_n_teams = [
                len(self.find_all(comp, self.classes.team_name))//2 for comp in box_competitions
            ]

            list_comp_names = [[item.get_text(separator=" ").strip() if hasattr(
                item, 'text') else '' for item in lista] for lista in list_comps]

            treated_list_comp_names = [None if item == '' else item for item in [
                ''.join(competition_info) for competition_info in list_comp_names]]

            return [item for item, mult in zip(treated_list_comp_names, list_n_teams) for _ in range(mult)]

        except AttributeError:
            return None
