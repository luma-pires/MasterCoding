from Scraping.Scraper import Scraper
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException, StaleElementReferenceException, \
    ElementClickInterceptedException, ElementNotInteractableException
import time


class Selenium(Scraper):

    def __init__(self, sett, link, sett_classes):

        super().__init__(sett, link)

        self.sett = sett
        self.classes = sett_classes
        self.sleep_load_page = self.sett_machine.sec_sleep_load_page
        self.sleep_next_page = self.sett_machine.sec_sleep_next_page
        self.sleep_click = self.sett_machine.sec_sleep_click
        self.n_try_locate_element = 3
        self.n_try_deal_with_cookies = 8
        self.sleep_load_cookies = 3
        self.original_window = None
        self.by = {'ID': By.ID, 'CLASS_NAME': By.CLASS_NAME, 'XPATH': By.XPATH, 'CSS_SELECTOR': By.CSS_SELECTOR}
        self.driver = self.open_browser()
        self.get_link(link)

    # Click:
    def click(self, element):

        by = self.by[element[0]]
        label = element[1]

        while True:

            try:
                element = self.driver.find_element(by, label)
                element.click()
                return True

            except ElementClickInterceptedException:
                self.dealing_with_cookies()

            except NoSuchElementException:
                return False

            except ElementNotInteractableException:
                pass

    def open_browser(self):
        driver = webdriver.Chrome()
        self.driver = driver
        self.original_window = self.define_current_sheet_as_original()
        return driver

    def close_browser(self):
        try:
            self.driver.quit()
        except AttributeError:
            pass

    def get_current_link(self):
        return self.driver.current_url

    # Get link:
    def get_link(self, link):
        self.driver.get(link)
        time.sleep(self.sleep_load_page)
        self.dealing_with_cookies()

    # Cookies:
    def accepting_cookies(self, cookies_element):
        self.click(cookies_element)
        time.sleep(self.sett_machine.sec_sleep_load_page)

    def dealing_with_cookies(self):

        cookies_element = self.classes.get("cookies")

        while self.is_element_on(cookies_element):

            try:
                self.accepting_cookies(cookies_element)

            except (KeyError, NoSuchElementException):
                time.sleep(self.sleep_load_cookies)

    def is_element_on(self, element):

        by = element[0]
        label = element[1]

        try:
            return self.driver.find_element(self.by[by], label).is_displayed()

        except NoSuchElementException:
            return False

        except KeyError:
            return True if self.get_soup().find_all(by, class_=element) else False

    # Hidden content:
    def open_hidden_content(self, open_hidden_content_symbol):

        by = self.by[open_hidden_content_symbol[0]]
        label = open_hidden_content_symbol[1]

        elements = self.driver.find_elements(by, label)

        for item in elements:

            boolean = True

            while boolean:

                try:

                    time.sleep(0.5)
                    item.click()
                    time.sleep(1.0)
                    boolean = False

                except NoSuchElementException:
                    boolean = False

                except StaleElementReferenceException:
                    boolean = False

                except ElementClickInterceptedException:
                    self.driver.execute_script("arguments[0].scrollIntoView();", item)
                    window_height = self.driver.execute_script("return window.innerHeight;")
                    offset = window_height * 0.8
                    self.driver.execute_script("window.scrollBy(0, -arguments[0]);", offset)

                except Exception as e:
                    self.describe_error(e)
                    boolean = False

    # Dealing with sheets:
    def define_current_sheet_as_original(self):
        return self.driver.current_window_handle

    def open_new_sheet(self):
        self.driver.execute_script("window.open('');")

    def get_number_of_sheets(self):
        return len(self.driver.window_handles)

    def get_current_sheet(self):

        all_windows = self.driver.window_handles
        current_window = self.driver.current_window_handle

        return all_windows.index(current_window)

    def go_to_n_sheet(self, n):

        if n < 0:
            print('Error: you are trying to get negative sheets')

        elif n >= self.get_number_of_sheets():
            print('Error: Index out of bonds')

        else:
            self.driver.switch_to.window(self.driver.window_handles[n])

    def go_to_next_sheet(self):
        try:
            next_sheet = 1
            self.go_to_n_sheet(next_sheet)
            return False
        except IndexError:
            return True
        except Exception as e:
            self.describe_error(e)
            return True

    def go_to_previous_sheet(self):
        previous_sheet = self.get_current_sheet() - 1
        self.go_to_n_sheet(previous_sheet)

    def go_to_original_sheet(self):
        self.driver.switch_to.window(self.original_window)

    # Getting content from different pages:
    def getting_content_from_multiple_pages(self):

        n_pages, soup = self.get_number_of_pages()
        list_content = [soup]

        for p in range(0, n_pages - 1):
            self.next_page(p)
            list_content.extend(self.get_soup())

        self.go_to_first_page()

        return list_content

    def get_number_of_pages(self):

        # Do not include current page!!!
        soup = self.get_soup()
        n_pages = len(soup.find_all(self.classes.pages, class_=self.classes.pages))

        return n_pages if n_pages != 0 else 1, soup

    def next_page(self, p):

        try:

            self.click(self.classes.next_button)
            print(f'{p + 2}° page')
            time.sleep(self.sleep_load_page)

        except NoSuchElementException:
            pass

        return

    def go_to_first_page(self):
        self.driver.get(self.link)

    # Getting soup:
    def get_html(self):
        return self.driver.page_source

    def get_soup(self):
        html = self.get_html()
        return self.convert_soup(html)

    # Refresh:
    def refresh(self):
        self.driver.refresh()
        time.sleep(self.sleep_load_page)
