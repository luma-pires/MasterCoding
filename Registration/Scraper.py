
def dict_scrapers():

    from Scraping.Selenium.Selenium import Selenium
    from Scraping.Autogui.Autogui import Autogui

    return {'autogui': Autogui, 'selenium': Selenium}
