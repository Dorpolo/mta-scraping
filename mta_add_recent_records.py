import re
import numpy as np
import requests
from bs4 import BeautifulSoup
import pandas as pd
import datetime
import psycopg2
from psycopg2.extras import execute_values
from tabulate import tabulate


class ScrapMta:

    def __init__(self, my_url='https://www.maccabi-tlv.co.il/en/result-fixtures/first-team/results/', n=5):

        """
        :param my_url: season results page
        :param n: number of games, if the required output is only
                  fetching the last game, so the 5  default value is enough
        """

        # get current season
        now = datetime.datetime.now()
        s_year = now.year
        s_month = now.month

        if s_month > 7:
            s_year_1 = s_year
            s_year_2 = s_year + 1
        else:
            s_year_1 = s_year - 1
            s_year_2 = s_year

        self.url = my_url
        self.n = n
        self.season = str(s_year_1)[2:] + '-' + str(s_year_2)[2:]
        self.soup_mta = BeautifulSoup(requests.get(my_url).content, "html.parser")

        # get all links from page_url
        self.game_links = [link.get('href') for link in self.soup_mta.find_all('a')]

        # choose only relevant urls (might be changed)
        self.relevant_links = [item for item in self.game_links if
                               ('https://www.maccabi-tlv.co.il/en/match/' in item) &
                               ('overview/' not in item)][0:(self.n+1)]

    def mta_results(self):
        """
        :return: DataFrame with mta_games records
        """

        # mta - goal scored per match
        mta_result = self.soup_mta.find_all("span", {"class": "ss maccabi h"})
        df_mta_res = [item.text for item in mta_result]

        # opponent - goal scored per match
        not_mta_result = self.soup_mta.find_all("span", {"class": "ss h"})
        df_not_mta_res = [item.text for item in not_mta_result]

        # opponent name
        not_mta_teams = self.soup_mta.find_all("div",{"class": "holder notmaccabi nn"})
        df_not_mta_teams = [item.text for item in not_mta_teams]

        # stadium, date
        location_date = self.soup_mta.find_all("div", {"class": "location"})
        df_location = [item.find_all("div")[0].text for item in location_date]
        df_date = [item.find_all("span")[0].text for item in location_date]

        # competition type
        league = self.soup_mta.find_all("div", {"class": "league-title"})
        df_league = [item.text for item in league]

        # constructing the unique game identifier - includes from date and opponent name shortcut
        id_location_date = self.soup_mta.find_all("div", {"class": "location"})
        id_date_list = [item.find_all("span")[0].text for item in id_location_date]

        id_not_mta_teams = self.soup_mta.find_all("div", {"class": "holder notmaccabi nn"})
        id_opponent_list = [item.text for item in id_not_mta_teams]

        gen_id_map = pd.DataFrame({'date': id_date_list,
                                   'opp': id_opponent_list})

        df_gen = gen_id_map['date'].str.split(" ", n=2, expand=True).rename(columns={0: 'day',
                                                                                     1: 'name',
                                                                                     2: 'year'})

        months = [(datetime.date(2020, i, 1).strftime('%m')[:3],
                   datetime.date(2020, i, 1).strftime('%B')[:3]) for i in range(1, 13)]

        month_map = pd.DataFrame(months).rename(columns={0: 'number', 1: 'name'})

        game_id_split = pd.merge(df_gen, month_map, on='name', how='left')

        game_id = game_id_split['day'] + game_id_split['number'] + game_id_split['year'] + [w.replace(' ','') for w in id_opponent_list]

        dict = {'game_id': game_id,
                'season': [self.season]*len(df_not_mta_teams),
                'date': df_date,
                'location': df_location,
                'maccabi': ['MTA']*len(df_not_mta_teams),
                'opponent': df_not_mta_teams,
                'res_1': df_mta_res,
                'res_2': df_not_mta_res,
                'league': df_league
        }

        df = pd.DataFrame(dict)

        loc_split = df["location"].str.split(" ", n=1, expand=True)
        df['hour'] = loc_split[0]
        df['stadium'] = loc_split[1]
        df['date'] = pd.to_datetime(df['date'])
        mta_df = df.sort_values(by=['date']).drop(columns=['location'])
        mta_df['round'] = mta_df.groupby(['season', 'league'])['date'].rank(method="first", ascending=True).astype(int)

        return mta_df

    def game_id_table(self):

        def game_id_page(url):
            """
            :param url:
            :return: game_id
            """
            soup_mta_2 = BeautifulSoup(requests.get(url + 'teams').content, "html.parser")
            game_id_date_text = soup_mta_2.find_all("header", {"class": "entry-header"})[0]\
                .text.replace('\n', '').replace('\t', '')
            loc_date_text = game_id_date_text.find(" ", 2)
            date_final = game_id_date_text[loc_date_text + 1:loc_date_text + 11]
            opponent_final = soup_mta_2.find_all("div", {"class": "team not-maccabi"})[0]\
                .text.replace('\n', '').replace('\t', '').replace('0', '')
            game_id = date_final + opponent_final
            output = game_id.replace(' ', '').replace('/', '')

            return output

        game_ids = [game_id_page(x) for x in self.relevant_links]
        loc = 0

        game_ids_list = []
        for string in game_ids:
            m = re.search(r'\d+$', string)
            if m is not None:
                game_ids_list.append(string[:-1])
            else:
                game_ids_list.append(string)
            loc = loc + 1

        relevant_links_teams = [item + 'teams/' for item in self.relevant_links]

        df_game_connection = pd.DataFrame({'game_id': game_ids_list,
                                           'game_url': relevant_links_teams})

        # checking if the game had already played
        df_game_connection['date'] = df_game_connection['game_id'].str[0:8]
        df_game_connection['relevant_link'] = df_game_connection["date"].str.isdigit()

        return df_game_connection.loc[df_game_connection['relevant_link']].reset_index()

    def get_players_data(self, url):
        """
        :return: creating records per player per game
        """

        soup_mta = BeautifulSoup(requests.get(url + 'teams').content, "html.parser")

        def game_id_page():
            """

            :param my_url:
            :return: game_id
            """

            game_id_date_text = soup_mta.find_all("header", {"class": "entry-header"})[0].text.replace('\n', ''). \
                replace('\t', '')
            loc_date_text = game_id_date_text.find(" ", 2)
            date_final = game_id_date_text[loc_date_text + 1:loc_date_text + 11]
            opponent_final = soup_mta.find_all("div", {"class": "team not-maccabi"})[0].text.replace('\n', '').\
                replace('\t', '').replace('0', '')
            game_id = date_final + opponent_final

            if game_id[len(game_id) - 1].isdigit():
                game_id = game_id[:-1]

            return game_id.replace(' ', '').replace('/', '')

        # get player number
        def player_number(number):
            start = str(number[0]).find('>')
            end = str(number[0]).find('<', 2)
            return int(str(number[0])[start + 1:end])

        players = soup_mta.find_all("div", {"class": "p50 yellow"})
        p_numbers = [player_number(item.find_all("b")) for item in players[0].find_all("li")[1:]]
        p_numbers_sub = [player_number(item.find_all("b")) for item in players[1].find_all("li")[1:]]

        df_numbers = p_numbers + p_numbers_sub

        n1 = len(p_numbers)
        n2 = len(p_numbers_sub)

        game_ids = game_id_page()
        list_ids = [game_ids] * (n1 + n2)

        def player_name(i, string):
            """

            :param i: running index
            :param string: any string
            :return: player name
            """
            t = string.find_all("li")[i].text
            name_indicators = []
            index = 0

            for char in t:
                if char.isalpha():
                    name_indicators.append(index)
                index = index + 1

            # edge case when the player name is missing
            if not name_indicators:
                return 'NA'

            return t[name_indicators[0]:name_indicators[len(name_indicators) - 1] + 1]

        names = soup_mta.find_all("div", {"class": "p50 yellow"})

        p_names_1 = [player_name(i, names[0]) for i in range(1, n1 + 1)]
        p_names_2 = [player_name(i, names[1]) for i in range(1, n2 + 1)]

        p_names = p_names_1 + p_names_2

        p_names = [item[:-3].replace('  ', ' ') if '(C' in item else item.replace('  ', ' ') for item in p_names]
        p_is_captain = [True if '(C' in item else False for item in p_names]

        # cards, goals and substitutions
        icon_goal = soup_mta.find_all("div", {"class": "p50 yellow"})

        goal_list = icon_goal[0].find_all("div", {'class': 'icons team-players goals'})
        goal_sub_list = icon_goal[1].find_all("div", {'class': 'icons team-players goals'})
        icon_exchange_list = icon_goal[0].find_all("div", {"class": "icons team-players",
                                                           'id': re.compile('exchange')})

        icon_exchange_sub_list = icon_goal[1].find_all("div", {"class": "icons team-players",
                                                               'id': re.compile('exchange')})

        goals = [item.text if item.text != '' else None for item in goal_list]
        goals_sub = [item.text if item.text != '' else None for item in goal_sub_list]

        df_goals = goals + goals_sub

        exchange = []
        for item in icon_exchange_list:
            if item.text != '' and item.text != '\n':
                if len(item.text) > 4:
                    exchange.append(float(re.sub(" ", '.', re.sub("'", '', item.text)[0:len(item.text)])))
                else:
                    exchange.append(float(re.sub("'", '', item.text)[0:len(item.text)]))
            else:
                exchange.append(None)

        exchange_sub_sub = []
        for item in icon_exchange_sub_list:
            if item.text != '' and item.text != '\n':
                if len(item.text) > 4:
                    exchange_sub_sub.append(float(re.sub(" ", '.', re.sub("'", '', item.text)[0:len(item.text)])))
                else:
                    exchange_sub_sub.append(float(re.sub("'", '', item.text)[0:len(item.text)]))
            else:
                exchange_sub_sub.append(None)

        df_exchange = exchange + exchange_sub_sub

        icon_card_list = icon_goal[0].find_all("div", {"class": "icons team-players", 'id': re.compile('red')})
        icon_card_sub_list = icon_goal[1].find_all("div", {"class": "icons team-players", 'id': re.compile('red')})

        card = [item.text.replace('\t', '').replace('\n', '') if item.text != '' and item.text != '\n' else None
                for item in icon_card_list]

        card_sub = [item.text.replace('\t', '').replace('\n', '') if item.text != '' and item.text != '\n' else None
                    for item in icon_card_sub_list]

        df_card = card + card_sub

        p_dict = {'game_id': list_ids[:(n1+n2)],
                  'player_number': df_numbers[:(n1+n2)],
                  'game_status': ['opening']*n1 + ['substitute']*n2,
                  'player_name': p_names[:(n1+n2)],
                  'is_captain': p_is_captain[:(n1+n2)],
                  'goals': df_goals[:(n1+n2)],
                  'subtitution': df_exchange[:(n1+n2)],
                  'card': df_card[:(n1+n2)]
                  }

        players_df = pd.DataFrame(p_dict)

        players_df['minute_played'] = np.where(
            (players_df.subtitution.notna()) & (players_df.game_status == 'substitute'),
            90 - players_df['subtitution'],
            np.where((players_df.subtitution.notna()) & (players_df.game_status == 'opening'),
                     players_df.subtitution,
                     np.where(players_df.game_status == 'opening', 90, 0))
            )

        players_df['is_played'] = np.where(players_df.minute_played > 0, True, False)
        # special treatment for M. Baltaxa
        players_df['player_name'] = np.where((players_df.player_name == 'NA') &
                                             (players_df.player_number == 3), 'Matan Baltaxa', players_df.player_name)

        return players_df

    def game_home_away(self, url):
        r_mta_2 = requests.get(url)
        c_mta_2 = r_mta_2.content
        soup_mta_2 = BeautifulSoup(c_mta_2,"html.parser")
        mta = soup_mta_2.find_all("div",{'class':re.compile('teams')})[0]
        return str(mta)[18:22]

    def get_game_coach(self, url):
        try:
            r_mta_2 = requests.get(url)
            c_mta_2 = r_mta_2.content
            soup_mta_2 = BeautifulSoup(c_mta_2, "html.parser")
            coaches = soup_mta_2.find_all("div", {"class":"p50 yellow"})
            game_coach = coaches[2].find_all("li")[0].text
            output = game_coach
        except:
            output = None
        return output

    def apply_goals_table(self, players_data_table):

        def adjust_date(col_name):
            for col in col_name:
                col = col[0:8]
                day = col[0:2]
                month = col[2:4]
                year = col[4:8]
                op = year + "-" + month + "-" + day
            return op

        mta_events_base = players_data_table[players_data_table.goals.notnull()][['game_id', 'player_name', 'goals',
                                                                                  'subtitution', 'card', 'game_status',
                                                                                  'minute_played']]

        init_goals = mta_events_base[(mta_events_base.goals.notnull())][['game_id', 'player_name', 'goals']]

        if init_goals.shape[0] == 0:
            goal_melted = None
        else:
            goals_new = init_goals['goals'].str.split("'", n = 6, expand = True)
            goals_df = pd.concat([init_goals, goals_new], axis=1)
            goal_melted = pd.melt(goals_df, id_vars=['game_id', 'player_name', 'goals'])
            goal_melted = goal_melted.loc[(goal_melted.value.notnull()) & (goal_melted.value != '')].\
                drop(columns=['goals', 'variable'])
            goal_melted.value = goal_melted.value.astype(int)
            goal_melted['date'] = pd.to_datetime(goal_melted[['game_id']].apply(adjust_date, axis=1))
            goal_melted = goal_melted.sort_values(by=['date', 'game_id', 'value'], ascending = False)
            goal_melted['event_type'] = 'goal_scored'
            goal_melted = goal_melted.reset_index(drop=True)[['date', 'game_id', 'player_name', 'event_type', 'value']]
        return goal_melted

    def mta_lego(self, main_list):
        """

        :param main_list: gets a list with one specific game parameters
        :return:
        """
        def lego_players(list):
            """

            :param list: inherits list from main function
            :return: ready players records
            """
            df_players = list[2]
            df = df_players[['game_id', 'player_number', 'game_status', 'player_name',
                             'is_captain', 'subtitution', 'is_played']]

            df['minutes_played'] = np.where(df.game_status == 'opening',
                                            np.where(df['subtitution'].notnull(), df['subtitution'], 90),
                                            np.where(df['subtitution'].notnull(), 90-df['subtitution'], 0))

            df['is_played'] = np.where(df.minutes_played > 0, True, False)
            df['player_number'] = df['player_number'].astype(str)
            df['con_id'] = df[['game_id', 'player_number']].apply(lambda x: '_'.join(x), axis = 1)
            df = df[['con_id', 'game_id', 'player_number', 'game_status', 'player_name',
                     'is_captain', 'subtitution', 'is_played', 'minutes_played']]

            df['con_id'] = df['con_id'] + df['player_name'].str[0:2]

            return df

        def lego_game(list):
            """

            :param list: inherits list from main function
            :return: ready games records
            """
            my_new_df = pd.DataFrame({'game_id': list[0].game_id,
                                      'season': list[0].season,
                                      'date': list[0].date,
                                      'hour': list[0].hour,
                                      'stadium': list[0].stadium,
                                      'location': list[3],
                                      'opponent': list[0].opponent,
                                      'mta_score': list[0].res_1,
                                      'opponent_score': list[0].res_2,
                                      'league_name': list[0].league,
                                      'round': list[0]['round'],
                                      'coach': [list[4]],
                                      'game_url': list[1].game_url})

            my_new_df['league'] = np.where(my_new_df['league_name'].\
                                           isin(['Tel aviv stock exchange League', 'Winner League', 'Ligat Japanika']),
                                           'League',
                                           np.where(my_new_df['league_name'].isin(['Europa League qualifying phase',
                                                                                   'Champions League Qualification',
                                                                                   'Champions League',
                                                                                   'Europa League Play-off',
                                                                                   'Europa League']), 'Europe',
                                                    my_new_df['league_name']))

            my_new_df['game_type'] = np.where(my_new_df['league_name'].\
                                              isin(['Tel aviv stock exchange League','Winner League',
                                                    'Ligat Japanika','Champions League','Europa League']),
                                              '3points',
                                              np.where(my_new_df['league_name'].isin(['Europa League qualifying phase',
                                                                                      'Champions League Qualification',
                                                                                      'Europa League Play-off',
                                                                                      'State Cup',
                                                                                      'Toto Cup']), 'knockout',
                                                       'other'))

            my_new_df['game_result'] = np.where(my_new_df.mta_score > my_new_df.opponent_score, 'W',
                                            np.where(my_new_df.mta_score < my_new_df.opponent_score, 'L', 'D'))

            return my_new_df

        def lego_events(list):

            mta_events = list[5]
            mta_events['event_id'] = mta_events['game_id'].str[0:8] + '_' + \
                                     mta_events['value'].astype(str).str[0:3] + '_' + \
                                     mta_events['event_type'] + '_' + \
                                     mta_events['player_name'].str[0:2]

            mta_events = mta_events[['event_id', 'date', 'game_id', 'player_name', 'event_type', 'value']]\
                .rename(columns={'value': 'minute'})

            return mta_events

        try:
            m_games = lego_game(main_list)
        except Exception as ex:
            print(ex)
        try:
            m_players = lego_players(main_list)
        except Exception as ex:
            print(ex)
        try:
            m_events = lego_events(main_list)
        except Exception as ex:
            print(ex)

        df_list = [m_games, m_players, m_events]

        return df_list


class MtaEvents:

    def __init__(self, game_id):

        self.gameId = game_id
        self.conn = psycopg2.connect("host = ec2-184-73-232-93.compute-1.amazonaws.com \
                                      port = 5432 \
                                      dbname = d5m2p6kka0vf8d \
                                      user = fzgxltqkgmaklf \
                                      password = 6ad610f8f95f1f570ad6c846b68e74f0d692386a8e43d2fce5976f1718e2b779")

    def fetch_game_events(self):

        """
        :url: game_url
        :return: events_data_frame
        """

        games = pd.read_sql("""SELECT DISTINCT c.player_name,
                                               'confirmed' as status,
                                               g.game_url,
                                               g.game_id,
                                               g.date,
                                               g.season
                               FROM mta_player_con c 
                               INNER JOIN mta_games g ON (g.game_id = c.game_id) 
                               WHERE c.game_id = '{g_id}'
                               ORDER BY 1 DESC""".format(g_id=self.gameId),
                            self.conn)

        url = games.game_url[0]

        if '/teams/' in url:
            url = url.replace('/teams/', '')
        else:
            url = url.replace('/teams', '')

        val_game_id = self.gameId
        val_game_date = games['date'][0]

        r_mta = requests.get(url)
        c_mta = r_mta.content
        soup_mta = BeautifulSoup(c_mta, "html.parser")

        dt = soup_mta.find_all("div", {'class': 'play-by-play-homepage'})

        minutes = [item.text for item in dt[0].find_all("div", {'class': 'min'})]
        events = [item.text.replace('\t', '').replace('\n', '') for item in dt[0].find_all("p")]

        df_init = pd.DataFrame({'minute': minutes, 'event_name': events})

        df_init['assist'] = df_init['event_name'].str.contains('Assist')
        df_init['goal_scored'] = df_init['event_name'].str.contains('Goal')
        df_init['yellow_card'] = df_init['event_name'].str.contains('Yellow')
        df_init['red_card'] = df_init['event_name'].str.contains('Red')
        df_init['con'] = df_init['event_name'].str.replace('Yellow', '')
        df_init['con'] = df_init['con'].str.replace(' to', '')
        df_init['con'] = df_init['con'].str.replace('card ', '')
        df_init['con'] = df_init['con'].str.replace('by ', '')
        df_init['con'] = df_init['con'].str.replace('scored ', '')
        df_init['con'] = df_init['con'].str.replace('Goal ', '')
        df_init['con'] = df_init['con'].str.replace('player ', '')
        df_init['con'] = df_init['con'].str.replace('Assist ', '')
        df_init['con'] = df_init['con'].str.replace('red ', '')

        df_init_filtered = df_init.loc[((df_init.assist == True) | (df_init.goal_scored == True) |
                                        (df_init.yellow_card == True) | (df_init.red_card == True)) & (
                                       (df_init.con != ''))]

        df2 = df_init_filtered[['minute', 'event_name', 'con', 'assist', 'goal_scored', 'yellow_card', 'red_card']].\
            rename(columns={'con': 'player_name'})

        df2['date'] = val_game_date
        df2['game_id'] = val_game_id

        df_melted = df2.melt(id_vars=['game_id', 'date', 'event_name', 'player_name', 'minute'], var_name='event_type')
        df_melted = df_melted.loc[df_melted.value]
        df_melted['player_name'] = df_melted['player_name'].str.replace('  ', '')

        df_melted.player_name = np.where(df_melted.player_name.str[0] == " ",
                                         df_melted.player_name.str.replace(' ', '', 1),
                                         df_melted.player_name)

        df_melted['player_name'] = df_melted.player_name.str.replace("Penalty", '', 1)
        df_melted['player_name'] = df_melted.player_name.str.replace("(", '', 1)
        df_melted['player_name'] = df_melted.player_name.str.replace(")", '', 1)
        df_melted['player_name'] = df_melted.player_name.str.replace("  ", '', 1)
        df_melted['player_name'] = df_melted['player_name'].str.replace(r"([A-Z])", r" \1")
        df_melted['player_name'] = df_melted['player_name'].str.replace("  ", " ")
        df_melted['player_name'] = df_melted['player_name'].apply(lambda x: x[1:] if x.startswith(" ") else x)
        df_melted['player_name'] = df_melted['player_name'].apply(lambda x: x[:-1] if x.endswith(" ") else x)

        data = pd.merge(df_melted,
                        games[['player_name', 'status']],
                        on='player_name',
                        how='left')

        output = data.loc[data.status == 'confirmed'].sort_values(by='player_name')

        return output[['game_id', 'date', 'minute', 'event_type', 'player_name', 'event_name']]


if __name__ == '__main__':

    Mta = ScrapMta(my_url='https://www.maccabi-tlv.co.il/en/result-fixtures/first-team/results/',n=10)

    # config the i parameter in order to get its location in the ordered date list
    # e.g: i == 0 -> the scraped data will be related to the recent match
    #      i == 1 -> the scraped data will be related to the previous match

    i = 0

    # get all Maccabi games (per season page)
    mta_df = Mta.mta_results()

    # get game ids
    mta_game_id_url = Mta.game_id_table()
    mta_player_con = mta_game_id_url[['game_id', 'game_url']]

    # get last game data points
    l_game_loc = Mta.game_home_away(mta_player_con.game_url[i])
    l_coach = Mta.get_game_coach(mta_player_con.game_url[i])
    l_players_data = Mta.get_players_data(mta_player_con.game_url[i])
    l_events = Mta.apply_goals_table(l_players_data)

    mta_df = mta_df[mta_df['league'] != 'Friendly'].sort_values(by=['date'], ascending=False)

    final_list = [
                  mta_df.loc[mta_df.game_id == mta_player_con.game_id[i]],
                  mta_game_id_url[i:(i+1)],
                  l_players_data,
                  l_game_loc,
                  l_coach,
                  l_events
                 ]

    # db connection
    conn = psycopg2.connect("dbname = 'd5m2p6kka0vf8d' \
                             user = 'fzgxltqkgmaklf' \
                             password = '6ad610f8f95f1f570ad6c846b68e74f0d692386a8e43d2fce5976f1718e2b779' \
                             host = 'ec2-184-73-232-93.compute-1.amazonaws.com' \
                             port = '5432'")

    dst_g_cursor = conn.cursor()
    dst_p_cursor = conn.cursor()

    games = Mta.mta_lego(final_list)[0]
    players = Mta.mta_lego(final_list)[1]
    events = Mta.mta_lego(final_list)[2]

    try:
        values_list_g = [tuple(x) for x in games.values]
        execute_values(dst_g_cursor,
                       """INSERT INTO mta_games (game_id, season, date, hour, stadium, location,
                                                opponent, mta_score, opponent_score, league_name, 
                                                round, coach, game_url, league, game_type, game_result)
                          VALUES %s
                          ON CONFLICT (game_id)
                          DO UPDATE
                          SET game_result = excluded.game_result,
                              season = excluded.season,
                              date = excluded.date,
                              hour = excluded.hour,
                              stadium = excluded.stadium,
                              location = excluded.location,
                              opponent = excluded.opponent,
                              mta_score = excluded.mta_score,
                              opponent_score = excluded.opponent_score,
                              league_name = excluded.league_name,
                              round = excluded.round,
                              coach = excluded.coach,
                              league = excluded.league,
                              game_url = excluded.game_url,
                          game_type = excluded.game_type""",
                       values_list_g)
        conn.commit()
        dst_g_cursor.close()
        print(tabulate(games, headers='keys', tablefmt='psql'))

    except Exception as ex:
        print(ex)

    try:
        values_list_p = [tuple(x) for x in players.values]
        execute_values(dst_p_cursor,
                       """INSERT INTO mta_player_con (con_id, game_id, player_number, game_status,
                                                      player_name, is_captain, sub, is_played, minutes_played)
                          VALUES %s
                          ON CONFLICT (con_id)
                          DO UPDATE
                          SET game_id = excluded.game_id,
                               player_number = excluded.player_number,
                               game_status = excluded.game_status,
                               player_name = excluded.player_name,
                               is_captain = excluded.is_captain,
                               sub = excluded.sub,
                               is_played = excluded.is_played,
                               minutes_played = excluded.minutes_played""",
                       values_list_p)
        conn.commit()
        print(tabulate(players, headers='keys', tablefmt='psql'))
        dst_p_cursor.close()
        print('players_db_ok')

    except Exception as ex:
        print(ex)

    try:
        dst_e_cursor = conn.cursor()

        values_list_e = [tuple(x) for x in events.values]

        execute_values(dst_e_cursor,
                       """INSERT INTO "mta_events" (event_id, date, game_id,
                                                 player_name, event_type, minute)
                           VALUES %s
                           ON CONFLICT (event_id)
                           DO UPDATE
                           SET minute = excluded.minute,
                               date = excluded.date,
                               player_name = excluded.player_name,
                               event_type = excluded.event_type""",
                       values_list_e)
        conn.commit()
        print(tabulate(events, headers='keys', tablefmt='psql'))
        dst_e_cursor.close()

    except Exception as ex:
        print(ex)

    conn.close()

    print('Task 1 (Update mta_games, mta_player_con, mta_events) -- DONE')

    conn = psycopg2.connect("host = ec2-184-73-232-93.compute-1.amazonaws.com \
                               port = 5432 \
                               dbname = d5m2p6kka0vf8d \
                               user = fzgxltqkgmaklf \
                               password = 6ad610f8f95f1f570ad6c846b68e74f0d692386a8e43d2fce5976f1718e2b779")

    # getting the game id
    gid = pd.read_sql("""SELECT game_id 
                         FROM mta_games
                         ORDER BY date DESC
                         LIMIT 1;""", conn)['game_id']

    for row in gid:

        p_names = pd.read_sql("""SELECT distinct player_name FROM mta_player_con""", conn)

        Events = MtaEvents(row)
        df = Events.fetch_game_events()

        df = df.loc[df.player_name.isin(p_names.player_name)]

        print(str(i) + ' --> ' + row)

        if df.shape[0] > 0:

            try:
                cur = conn.cursor()
                values = [tuple(x) for x in df.values]

                execute_values(cur,
                               """INSERT INTO events (game_id, date, minute, event_type, player_name, event_name)
                                   VALUES %s
                                   ON CONFLICT ON CONSTRAINT events_pkey
                                   DO UPDATE
                                   SET event_name = excluded.event_name""",
                               values)

                conn.commit()
                cur.close()

                print(tabulate(df[['game_id', 'date', 'player_name', 'event_type', 'minute']], headers='keys', tablefmt='psql'))

            except Exception as ex:
                print(ex)

        conn.close()
        i += 1

