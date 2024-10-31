from datetime import datetime, timedelta
import time


class Date_and_Time:

    @staticmethod
    def today_date():
        datetime_now = datetime.now()
        return datetime(datetime_now.year, datetime_now.month, datetime_now.day)

    @staticmethod
    def tomorrow_date():
        return Date_and_Time.today_date() + timedelta(days=1)

    @staticmethod
    def yesterday_date():
        return Date_and_Time.today_date() + timedelta(days=-1)

    @staticmethod
    def sleep(sec_frequency, time_start):
        time_left = sec_frequency - (datetime.now() - time_start).seconds - 1
        time_left = 0 if time_left < 0 else time_left
        print(f'Waiting {time_left} seconds - {datetime.now()}')
        time.sleep(time_left)
