import pyautogui

from buttons.functions_button import *
from functions.functions_aux import *
from functions.functions_time import *
from unidecode import unidecode


screenshot_path = r'Images\screenshot.png'
browser_icon_path = r'../Images/chrome_icon.png'
orange_path = r'Images\orange.png'
odds_folder = r'Images\odds_position'
source = 'bet365'


def orders_button(n, source_name, main_schema, tbl_orders, chrome_path, found_orange_path,
                  full_screen_path, path_odds_folder, path_already_logged_in, path_first_login_button,
                  path_last_login_button, path_username_filled, path_user, path_password, value_betting,
                  betting_button, close_button, seconds_load_page, seconds_patience_bet):

    # Opening browser:
    conn, engine = open_connection()
    if n == 0:
        get_link_live_odds(conn, chrome_path, sleep_load_page=seconds_load_page)
        close_connection(conn)
        # Getting odds positions:
        dict_odds = get_dict_odds_position(path_odds_folder, full_screen_path)
    else:
        # TODO: Colocar como variável
        get_link('https://www.bet365.com/?#/IP/B1', seconds_load_page)

    while True:

        try:

            # Login:
            login_bet365(path_already_logged_in, path_first_login_button, path_last_login_button, path_username_filled,
                         path_user, path_password)

            # Getting source and sports ids:

            conn, engine = open_connection()

            source_df = get_df(f'{main_schema}.source')
            source_id = source_df[source_df['source_name'] == source_name].iloc[0]['source_id']

            # Getting games with orders:
            df_orders = get_orders(conn, main_schema, tbl_orders)

            if not isinstance(df_orders, pd.DataFrame):
                print('No available orders for now')
                continue

            df_orders = df_orders[df_orders['source_id'] == source_id]

            # Getting today games:
            df_games_today = get_df(f'{source}.games', conn)
            df_games_today = df_games_today[(df_games_today['start_date'] > today()) &
                                            (df_games_today['start_date'] < tomorrow())]

            # Getting teams df:
            df_teams = get_df(f'{source}.teams', conn)
            df_teams['team_name'] = df_teams['team_name'].apply(lambda x: unidecode(x))

            # Dict index columns:
            dict_col_idx = dict(enumerate(df_orders.columns))
            dict_col_idx = {value: key for key, value in dict_col_idx.items()}

            # Getting positions for each game in orders table (ref original team_1):
            if len(df_orders) == 0:
                print('No order available for bet365 now')

            else:
                print(f'{len(df_orders)} orders available now in bet365')

                df_orders = df_orders.sort_values(by=['datetime_expiration', 'order_value'], ascending=[True, False])

                for row in df_orders.values:

                    # Expired?
                    if row[dict_col_idx['datetime_expiration']] < datetime.now():
                        print('Order expired :( Going to the next one')
                        continue

                    # Reference team  name:
                    game_ref = row[dict_col_idx['game_id']]

                    # Home First?
                    home_first = df_games_today[df_games_today['game_id']==game_ref].iloc[0]['home_first']

                    # Amount:
                    amount = row[dict_col_idx['order_value']]

                    try:
                        team_id_ref = df_games_today[df_games_today['game_id'] == game_ref].iloc[0]['team_1']
                        team_id_to_name = df_teams.set_index('team_id')['team_name'].to_dict()
                        team_name_ref = team_id_to_name[team_id_ref]

                    except Exception as e1:
                        print(e1)
                        print('Could not find game_id. Next!')
                        continue

                    # Odds with orders:
                    if home_first:
                        odds_with_orders = {'win': row[dict_col_idx['win']], 'draw': row[dict_col_idx['draw']],
                                            'defeat': row[dict_col_idx['defeat']]}
                    else:
                        odds_with_orders = {'win': row[dict_col_idx['defeat']], 'draw': row[dict_col_idx['draw']],
                                            'defeat': row[dict_col_idx['win']]}

                    odds_with_orders = {key: value for key, value in odds_with_orders.items() if value}

                    # Looking for team and taking a screenshot:
                    found = finding_string_ctrl_f(team_name_ref, full_screen_path)
                    if not found:
                        print(f'Could not find {team_name_ref}. Going to next betting!')
                        continue

                    # Position x:
                    x_team, y_team = get_positions_from_crop_image(full_screen_path, found_orange_path)

                    for key, values in odds_with_orders.items():

                        # Position y:
                        x_odd = dict_odds[key][0]

                        # Moving to right place:
                        pyautogui.moveTo(x_odd, y_team)
                        if not home_first:
                            print('Not home first!')
                        pyautogui.click()
                        time.sleep(1)

                        control = True

                        # Betting:
                        while control:
                            try:
                                bet = betting(str(amount), value_betting, betting_button, close_button,
                                              seconds_patience_bet)
                                pyautogui.hotkey('home')
                                control = False

                                try:
                                    # TODO: Trocar para path:
                                    time.sleep(1)
                                    positions_x = pyautogui.locateOnScreen(r'..\Images\betting\x.png')
                                    pyautogui.moveTo(positions_x)
                                    pyautogui.click()

                                except:
                                    pass
                            except:
                                try:
                                    # TODO: Colocar em path
                                    alteracoes = pyautogui.locateOnScreen(r'..\\Images\betting\accept_and_bet.png',
                                                                          confidence=0.8)
                                    pyautogui.moveTo(alteracoes)
                                    pyautogui.click()
                                    bet = betting(str(amount), value_betting, betting_button, close_button,
                                                  seconds_patience_bet)
                                    control = False

                                except:
                                    # TODO: Colocar em path
                                    alteracoes = pyautogui.locateOnScreen(r'..\Images\betting\accept_changes.png',
                                                                          confidence=0.8)
                                    pyautogui.moveTo(alteracoes)
                                    pyautogui.click()
                                    bet = betting(str(amount), value_betting, betting_button, close_button,
                                                  seconds_patience_bet)
                                    control = False

                        if not bet:
                            print('Could not place bet :(')

                            # Avoiding errors. Close old page and open a new one:
                            pyautogui.hotkey('ctrl', 'l')
                            time.sleep(0.5)
                            pyautogui.hotkey('f5')
                            time.sleep(2)

                            continue

                        # Change df:
                        row_before = row
                        row[dict_col_idx['done']] = True
                        row[dict_col_idx['datetime_execution']] = datetime.now()
                        update(main_schema, tbl_orders,
                               pd.DataFrame([row_before], columns=list(dict_col_idx.keys())),
                               pd.DataFrame([row], columns=list(dict_col_idx.keys())),
                               ['done', 'datetime_execution'])
                        print('Updated!')

        except Exception as e2:
            print('Erro:', e2)
            pyautogui.hotkey('ctrl', 'l')
            time.sleep(0.5)
            pyautogui.hotkey('f5')
            time.sleep(2)
            login_bet365(path_already_logged_in, path_first_login_button, path_last_login_button, path_username_filled,
                         path_user, path_password)

