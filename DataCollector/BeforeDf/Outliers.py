from DataCollector.BeforeDf.Soup import SoupToDf
import pandas as pd


class Outliers:

    def __init__(self):
        self.limit_back = 1.1
        self.limit_lay = 0.9

    def treatment_outliers(self, df):

        mask_finished = df['back_win'] == SoupToDf.finished
        df_finished = df[mask_finished].reset_index(drop=True)
        df_not_finished = df[~mask_finished].reset_index(drop=True)

        if not df_not_finished.empty:

            df_not_finished = self.treatment_outliers_back_and_win(df_not_finished)
            return pd.concat([df_not_finished, df_finished], axis=0, ignore_index=True)

        else:
            return df

    def treatment_outliers_back_and_win(self, df):

        modes = ['back', 'win']
        events = ['win', 'draw', 'defeat']

        existing_columns = [f'{mode}_{event}' for mode in modes for event in events if f'{mode}_{event}' in df.columns]
        df[existing_columns] = df[existing_columns].apply(pd.to_numeric, errors='coerce')

        df = self.drop_outlier_rows('lay', df)
        df = self.drop_outlier_rows('back', df)

        columns_to_delete = ['sum_back', 'sum_lay']
        existing_columns = [col for col in columns_to_delete if col in df.columns]
        df.drop(columns=existing_columns, inplace=True)

        return df

    def drop_outlier_rows(self, mode, df):

        try:

            col_name = f'sum_{mode}'
            df[col_name] = (1 / df[f'{mode}_win']) + (1 / df[f'{mode}_draw']) + (1 / df[f'{mode}_defeat'])
            back = mode == 'back'
            outliers = df[col_name] > self.limit_back if back else df['sum'] < self.limit_lay
            df = df[~outliers].reset_index(drop=True)

        except KeyError:
            pass

        return df