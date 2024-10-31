import pyautogui
import time
import pyperclip
import os
from Scraping.Scraper import Scraper


class Autogui(Scraper):

    def __init__(self, sett, link):

        super().__init__(sett, link)

        self.sett = sett
        self.confidence = 0.7
        self.n_try_locate_image = 8
        self.browser = 'chrome'
        self.image_folder_path = self.image_folder()
        self.n_clicks_open_browser = self.sett_machine.n_clicks_open_browser
        self.n_clicks_elements = self.sett_machine.n_clicks_elements
        self.sleep_open_browser = self.sett_machine.sec_sleep_open_browser
        self.sleep_load_page = self.sett_machine.sec_sleep_load_page
        self.sleep_load_elements = self.sett_machine.sec_sleep_load_elements
        self.n_try_locate_elements = 10
        self.positions_browser = self.get_position_icon_browser()
        self.positions = self.html_pos(link)

    # Image Folder path:
    def image_folder(self):

        project_path = self.system.get_project_path()
        machine_name = self.system.get_machine_name()
        image_folder = f"{project_path}\\Images\\{machine_name}"

        return image_folder

    # Basics:
    @staticmethod
    def ctrlc_ctrlv():
        pyautogui.hotkey('ctrl', 'c')
        time.sleep(0.05)
        pyautogui.hotkey('ctrl', 'v')
        time.sleep(0.05)
        content = pyperclip.paste()
        print('', end="")
        pyperclip.copy('')
        return content

    @staticmethod
    def inspect_elements():
        pyautogui.hotkey('ctrl', 'shift', 'i')

    @staticmethod
    def url():
        pyautogui.hotkey('ctrl', 'l')

    # Sheets:

    @staticmethod
    def new_sheet():
        pyautogui.hotkey('ctrl', 't')

    @staticmethod
    def prev_sheet():
        pyautogui.hotkey('ctrl', 'shift', 'tab')

    @staticmethod
    def next_sheet():
        pyautogui.hotkey('ctrl', 'tab')

    @staticmethod
    def close_sheet():
        pyautogui.hotkey('ctrl', 'w')

    @staticmethod
    def close_window():
        pyautogui.hotkey('ctrl', 'shift', 'w')

    # Searching Images:
    def locate_image(self, image_path):
        relative_path_image = self.system.relative_path(image_path, os.getcwd())
        positions = pyautogui.locateOnScreen(relative_path_image, confidence=self.confidence)
        return positions

    def try_until_find_image(self, image, sleep=0.5):
        n = 0
        while n < self.n_try_locate_image:
            try:
                positions = self.locate_image(image)
                return positions
            except pyautogui.ImageNotFoundException:
                print(f'Could not click on image :(')
                n += 1
                time.sleep(sleep)
        return None

    # Clicking:
    @staticmethod
    def click(positions, n_clicks):
        pyautogui.moveTo(positions)
        # time.sleep(0.1)
        for i in range(0, n_clicks):
            pyautogui.click(positions)

    # Opening browser:

    def get_position_icon_browser(self):
        icon_path = f'{self.image_folder_path}\\browsers\\{self.browser}\\icon.png'
        positions_browser = self.locate_image(icon_path)
        return positions_browser

    def open_browser(self):
        self.click(self.positions_browser, self.n_clicks_open_browser)
        time.sleep(self.sleep_open_browser)

    def close_browser(self):
        self.close_window()

    # Getting link:
    def get_link(self, link):
        self.url()
        pyautogui.write(link)
        pyautogui.hotkey('enter')
        time.sleep(self.sleep_load_page)

    # Getting soup:
    def html_pos(self, link):

        positions = None
        elements_path = f'{self.image_folder_path}\\browsers\\{self.browser}\\elements.png'

        while positions is None:

            self.open_browser()
            self.get_link(link)
            self.inspect_elements()
            time.sleep(self.sleep_load_elements)

            positions = self.try_until_find_image(elements_path)
            self.inspect_elements()
            self.close_browser()

        self.open_browser()
        self.get_link(link)

        return positions

    def get_html(self):
        self.inspect_elements()
        time.sleep(self.sleep_load_elements)
        self.click(self.positions, 2)
        html = self.ctrlc_ctrlv()
        self.inspect_elements()
        return html

    def get_soup(self):

        control = True
        html = ""

        while control:

            try:
                html = self.get_html()
                control = False
            except pyautogui.FailSafeException:
                pass

        print('', end="")
        return self.convert_soup(html)

    # Refresh page:
    def get_current_link(self):
        self.url()
        return self.ctrlc_ctrlv()

    def refresh_sheet(self):
        link = self.get_current_link()
        self.new_sheet()
        self.prev_sheet()
        self.close_sheet()
        self.get_link(link)
