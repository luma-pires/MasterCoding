from Root_Classes.Info import Info
from Root_Classes.Staff import Staff
from Config.settings import sett
from bs4 import BeautifulSoup
from Registration.Scraper import dict_scrapers


class Scraper(Info, Staff):

    def __init__(self, sett, link):

        super().__init__(sett)

        self.sett = sett
        self.link = link
        self.sett_machine = getattr(sett.machines, self.machine)
        self.scrapers = dict_scrapers()

    @staticmethod
    def convert_soup(html):
        return BeautifulSoup(html, 'html.parser')
