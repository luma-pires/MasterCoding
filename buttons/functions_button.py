from functions.funtions_autogui import *
from functions.functions_SQL import *
import pyautogui
import time
import os
from datetime import datetime, timedelta


# from functions.functions_aux import get_user


def get_link_live_odds(conn, chrome_path, sleep_load_page=2):
    df_links = get_df('bet365.links', conn)
    link = df_links[df_links['link_name'] == 'live_games'].iloc[0]['link']
    # Open chrome and get link of live odds:
    click(chrome_path)
    get_link(link, sleep_load_page)


def get_dict_odds_position(path_odds_folder, full_screen_path):
    dict_odds = {}

    try:
        screenshot = pyautogui.screenshot()
        screenshot.save(full_screen_path)
        time.sleep(1)
    except Exception as e:
        print('Could not save screenshot. Error:', e)

    for odd in os.listdir(path_odds_folder):
        odd_file = path_odds_folder + r'//' + odd
        x, y = get_positions_from_crop_image(full_screen_path, odd_file)
        dict_odds[odd.split('.')[0]] = (x, y)
        print(f'Got {odd} positions successfully :D')

    return dict_odds


def get_orders(conn, main_schema, tbl_orders):
    # Getting orders table:
    df_orders = get_df(f'{main_schema}.{tbl_orders}', conn)

    if len(df_orders) == 0:
        return False

    # TODO: Rever isso de tirar o fuso:
    df_orders['datetime_order'] = df_orders['datetime_order'].dt.tz_localize(None)
    df_orders['datetime_expiration'] = df_orders['datetime_expiration'].dt.tz_localize(None)

    df_orders = df_orders[(df_orders['datetime_expiration'] >= datetime.now()) & (~df_orders['done'])]

    # TODO: Como ordenar?
    df_orders = df_orders.sort_values(by='datetime_expiration', ascending=True)

    return df_orders


def get_team_ref(game_row, source_name, conn):
    home_first = game_row['home_first']

    if home_first:
        team_id = game_row['team_1']
    else:
        team_id = game_row['team_2']

    team_name = get_id(f'{source_name}.teams', team_id, False, 'team', conn=conn)

    if team_name is not None:
        return team_name
    else:
        return None


def login_bet365(path_already_logged_in, path_first_login_button, path_last_login_button, path_username_filled,
                 path_user, path_password):
    print('#################### LOGIN ####################')
    username = 'gabrielsifu'
    password = 'Mmdc!1932c'

    # Already logged in?
    try:
        profile_location = pyautogui.locateOnScreen(path_already_logged_in, confidence=0.8)
        print('Already logged in :D')
        print('###############################################')

    except pyautogui.ImageNotFoundException:
        profile_location = None
        print('Not logged in')

    if profile_location is None:

        # Login button:
        login_button = pyautogui.locateOnScreen(path_first_login_button, confidence=0.7)
        pyautogui.moveTo(login_button)
        pyautogui.click()
        time.sleep(1)
        print('Clicked on login button')

        # User:
        try:
            writen_user = pyautogui.locateOnScreen(path_username_filled, confidence=0.8)
            print('User already writen')

        except pyautogui.ImageNotFoundException:
            writen_user = None

        if writen_user is None:
            user_button = pyautogui.locateOnScreen(path_user, confidence=0.8)
            pyautogui.moveTo(user_button)
            pyautogui.write(username)
            print('Username written')
            time.sleep(1)

        # Password:
        password_button = pyautogui.locateOnScreen(path_password, confidence=0.8)
        pyautogui.moveTo(password_button)
        pyautogui.click()
        time.sleep(1)
        pyautogui.write(password)
        print('Password written')
        time.sleep(1)

        # Finally login:
        user_final_button = pyautogui.locateOnScreen(path_last_login_button, confidence=0.8)
        pyautogui.moveTo(user_final_button)
        pyautogui.click()
        print('Clicked in the final button login')

        time.sleep(5)

        print('Logged in successfully :D')
        print('###############################################')


def betting(value, value_button, betting_button, close_button, seconds_patience_bet):
    try:
        try:
            value_positions = pyautogui.locateOnScreen(value_button, confidence=0.7)
        except:
            print('Clicked on wrong place...')

            try:
                # TODO: Deixar em variável path:
                alteracoes = pyautogui.locateOnScreen(r'..\Images\betting\alteracoes.png', confidence=0.8)
                pyautogui.moveTo(alteracoes)
                pyautogui.click()
                pass

            except:
                return False

        pyautogui.click(value_positions)
        pyautogui.write(value)
        time.sleep(0.5)
        try:
            bet_positions = pyautogui.locateOnScreen(betting_button, confidence=0.8)
        except:
            # TODO: Colocar bet gray em variável:
            try:
                time.sleep(0.5)
                bet_positions = pyautogui.locateOnScreen(r'..\Images\betting\bet_gray.png', confidence=0.7)
            except:
                # TODO: Colocar alteracoes.png em variável:
                alteracoes = pyautogui.locateOnScreen(r'..\Images\betting\alteracoes.png', confidence=0.8)
                print('Odds alteradas!')

                return True

        pyautogui.moveTo(bet_positions)
        time.sleep(0.5)
        pyautogui.click()
        time.sleep(0.5)
        print('Bet placed! Waiting confirm...')
        control = True

        first_try = datetime.now()

        while control:
            try:
                close_positions = pyautogui.locateOnScreen(close_button, confidence=0.8)
                print('Confirmed! :D')
                pyautogui.moveTo(close_positions)
                pyautogui.click()
                time.sleep(1)

                try:
                    # TODO: Colocar x.png em variável:
                    x_positions = pyautogui.locateOnScreen(r'..\Images\betting\dupla.png', confidence=0.8)
                    pyautogui.moveTo(x_positions)
                    pyautogui.click()
                    time.sleep(1)

                    print('Xô apostas duplas!')
                    return True

                except:
                    print('Sem apostas duplas')
                    return True
                    pass

            except:

                try:
                    # TODO: Colocar em path:
                    bet_positions = pyautogui.locateOnScreen(r'..\Images\betting\bet_gray.png', confidence=0.8)
                    pyautogui.moveTo(bet_positions)
                    time.sleep(0.5)
                    pyautogui.click()
                    time.sleep(0.5)
                    print('Trying place bet again... (not an error)')

                except:
                    if datetime.now() - first_try <= timedelta(seconds=seconds_patience_bet):
                        print('Placing bet')
                        time.sleep(0.1)
                        pass
                    else:
                        return False

    except Exception as e3:
        print(f'Error in line {e3.__traceback__.tb_lineno}')
        print(e3)


def click_if_exists(image_path, confidence=0.8):
    try:
        x_positions = pyautogui.locateOnScreen(image_path, confidence=confidence)
        pyautogui.moveTo(x_positions)
        pyautogui.click()
        time.sleep(1)
        return True

    except:
        return False
