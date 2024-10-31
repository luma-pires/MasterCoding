import time
from datetime import timedelta


class Sleep:

    def sleep(self, time_start, time_end, frequency):

        try:

            delay = timedelta(minutes=frequency) - (time_end - time_start)

            if delay > timedelta(0):
                self.print_time_lasting(delay)
                time.sleep(delay.total_seconds())

        except Exception as e:
            print(f'Error in line {e.__traceback__.tb_lineno}: {e}')

    @staticmethod
    def print_time_lasting(delay):

        delay_seconds = delay.total_seconds()

        if 60 < delay_seconds < 3600:
            minutes = delay_seconds // 60
            seconds = int(delay_seconds % 60)
            print(f'Waiting 00:{str(minutes)[:-2]}:{seconds}')

        else:
            print(f'Waiting {delay_seconds} seconds...')
