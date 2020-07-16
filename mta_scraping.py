import re
import urllib
import urllib3
import numpy as np
import requests
from bs4 import BeautifulSoup
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

# Phase I - Create initial game record (last game)

def bring_it_on(n=1,main_url='https://www.maccabi-tlv.co.il/en/result-fixtures/first-team/results/'):

    def mta_results(url = main_url,season='19-20'):
        r_mta = requests.get(url)
        c_mta = r_mta.content
        soup_mta=BeautifulSoup(c_mta,"html.parser")
        df_mta_res = []
        df_not_mta_res = []
        df_not_mta_teams = []
        df_date = []
        df_location = []
        df_league = []
        df_round = []

        # all maccabi goals
        mta_result = soup_mta.find_all("span",{"class":"ss maccabi h"})
        for item in mta_result:
            df_mta_res.append(item.text)

        # all maccabi opponent goals
        not_mta_result = soup_mta.find_all("span",{"class":"ss h"})
        for item in not_mta_result:
            df_not_mta_res.append(item.text)

        # all maccabi opponent teams
        not_mta_teams = soup_mta.find_all("div",{"class":"holder notmaccabi nn"})
        for item in not_mta_teams:
            df_not_mta_teams.append(item.text)

        location_date = soup_mta.find_all("div",{"class":"location"})
        for item in location_date:
            df_location.append(item.find_all("div")[0].text)
            df_date.append(item.find_all("span")[0].text)

        league = soup_mta.find_all("div",{"class":"league-title"})
        for item in league:
            df_league.append(item.text)

        round = soup_mta.find_all("div",{"class":"round"})
        for item in round:
            df_round.append(item.text)

        #adding friendly to empty values
        df_round.insert(len(df_round), 'Friendly')
        df_round.insert(len(df_round), 'Friendly')
        df_round.insert(len(df_round), 'Friendly')

        df_round_full = df_round

        df_season = [season]*len(df_league)
        df_maccabi = ['MTA']*len(df_not_mta_teams)

        # adding game_id

        id_date_list = []
        id_location_date = soup_mta.find_all("div",{"class":"location"})
        for item in id_location_date:
            id_date_list.append(item.find_all("span")[0].text)

        id_opponent_list = []
        id_not_mta_teams = soup_mta.find_all("div",{"class":"holder notmaccabi nn"})
        for item in id_not_mta_teams:
            id_opponent_list.append(item.text)

        gen_id_map = pd.DataFrame({'date':id_date_list,'opp':id_opponent_list})

        df_gen = gen_id_map['date'].str.split(" ", n = 2, expand = True)

        df_gen = df_gen.rename(columns={0: 'day', 1: 'name',2:'year'})

        month_map = pd.DataFrame({'name':['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'],
                 'number':['01','02','03','04','05','06','07','08','09','10','11','12']})

        game_id_split = pd.merge(df_gen,
                         month_map,
                         on = 'name',
                         how= 'left')

        game_id = game_id_split['day'] + game_id_split['number'] + game_id_split['year'] + [w.replace(' ','') for w in id_opponent_list]

        dict = {'game_id':game_id,
                'season':df_season,'date':df_date,'location':df_location,
                'maccabi':df_maccabi,'opponent':df_not_mta_teams,
                'res_1':df_mta_res,'res_2':df_not_mta_res,
                'league':df_league}

        df = pd.DataFrame(dict)

        return df

    my_games = mta_results()
    all_time = my_games
    new = all_time["location"].str.split(" ", n = 1, expand = True)
    all_time['hour'] = new[0]
    all_time['stadium'] = new[1]
    all_time['date'] = pd.to_datetime(all_time['date'])
    mta_df = all_time.sort_values(by=['date']).drop(columns=['location'])
    mta_df['round'] = mta_df.groupby(['season','league'])['date'].rank(method="first", ascending=True).astype(int)

    #####################################################


    ### Get Relevant Links from a seson page

    def game_id_table(page_url = 'https://www.maccabi-tlv.co.il/en/result-fixtures/first-team/results/'):
        #setup
        r_mta = requests.get(page_url)
        c_mta = r_mta.content
        soup_mta=BeautifulSoup(c_mta,"html.parser")

        #all game links from geven url
        game_links = []
        for link in soup_mta.find_all('a'):
            game_links.append(link.get('href'))

        a = [i for i in range(len(game_links)) if ('https://www.maccabi-tlv.co.il/en/match/' in game_links[i]) &
                                                    ('overview/' not in game_links[i])]
        relevant_links = []
        for item in a:
            relevant_links.append(game_links[item])

        def game_id_page(url):
            r_mta_2 = requests.get(url+'teams')
            c_mta_2 = r_mta_2.content
            soup_mta_2 = BeautifulSoup(c_mta_2,"html.parser")
            game_id_date_text = soup_mta_2.find_all("header",{"class":"entry-header"})[0].text.replace('\n','').replace('\t','')
            loc_date_text = game_id_date_text.find(" ",2)
            date_final    = game_id_date_text[loc_date_text+1:loc_date_text+11]

            oponent_final = soup_mta_2.find_all("div",{"class":"team not-maccabi"})[0].text.replace('\n','').replace('\t','').replace('0','')
            game_id = date_final + oponent_final
            return game_id.replace(' ','').replace('/','')

        game_ids = []
        for item in relevant_links[0:8]:
            game_ids.append(game_id_page(item))

        loc = 0
        game_ids_list = []
        for string in game_ids:
            m = re.search(r'\d+$', string)
            if m is not None:
                game_ids_list.append(string[:-1])
            else:
                game_ids_list.append(string)
            loc = loc + 1

        relevant_links_teams = []
        for item in relevant_links[0:8]:
            relevant_links_teams.append(item+'teams')

        game_connection = {'game_id':game_ids_list,
                           'game_url':relevant_links_teams
                          }

        df_game_connection = pd.DataFrame(game_connection)
        df_game_connection['date'] = df_game_connection['game_id'].str[0:8]
        df_game_connection['relevant_link'] = df_game_connection["date"].str.isdigit()

        return df_game_connection[df_game_connection['relevant_link'] == True].reset_index()

    mta_game_id_url = game_id_table('https://www.maccabi-tlv.co.il/en/result-fixtures/first-team/results/')

    def get_players_data(url):
        r_mta_2 = requests.get(url)
        c_mta_2 = r_mta_2.content
        soup_mta_2 = BeautifulSoup(c_mta_2,"html.parser")

        ### Ready !!! Player Numbers ###

        def player_number(number):
            start = str(number[0]).find('>')
            end = str(number[0]).find('<',2)
            return int(str(number[0])[start+1:end])

        p_numbers = []
        p_numbers_sub = []
        players = soup_mta_2.find_all("div",{"class":"p50 yellow"})

        for item in players[0].find_all("li")[1:]:
            p_numbers.append(player_number(item.find_all("b")))

        for item in players[1].find_all("li"):
            p_numbers_sub.append(player_number(item.find_all("b")))

        df_numbers = p_numbers + p_numbers_sub
        N1 = len(p_numbers)
        N2 = len(p_numbers_sub)

        def game_id_page(url):
            r_mta_2 = requests.get(url+'teams')
            c_mta_2 = r_mta_2.content
            soup_mta_2 = BeautifulSoup(c_mta_2,"html.parser")
            game_id_date_text = soup_mta_2.find_all("header",{"class":"entry-header"})[0].text.replace('\n','').replace('\t','')
            loc_date_text = game_id_date_text.find(" ",2)
            date_final    = game_id_date_text[loc_date_text+1:loc_date_text+11]

            oponent_final = soup_mta_2.find_all("div",{"class":"team not-maccabi"})[0].text.replace('\n','').replace('\t','').replace('0','')
            game_id = date_final + oponent_final
            if(game_id[len(game_id)-1].isdigit()):
                game_id = game_id[:-1]

            return game_id.replace(' ','').replace('/','')

        game_ids = game_id_page(url)
        list_ids = [game_ids]*(N1+N2)
        # Ready !!! Players Names and is Captain

        names = soup_mta_2.find_all("div",{"class":"p50 yellow"})

        def player_name(i,string):
            t = string.find_all("li")[i].text
            name_indicators = []
            index = 0

            for char in t:
                if char.isalpha():
                    name_indicators.append(index)
                index = index + 1
            return t[name_indicators[0]:name_indicators[len(name_indicators)-1]+1]

        p_names = []
        p_is_captain = []
        for i in range(1,N1 + 1):
            p_names.append(player_name(i,names[0]))

        for i in range(0,N2):
            p_names.append(player_name(i,names[1]))

        index = 0
        for item in p_names:
            if '(C' in item:
                p_names[index] = p_names[index][:-3]
                p_is_captain.append(True)
            else:
                p_is_captain.append(False)
            index = index + 1
            
        # remove spaces in player names
        p_names = [w.replace('  ', ' ') for w in p_names]

        # Ready !!! all game icons (cards, goals and subtitution)

        # Goals

        icon_goal = soup_mta_2.find_all("div",{"class":"p50 yellow"})
        goal_list = icon_goal[0].find_all("div",{'class':'icons team-players goals'})

        goals = []
        for item in goal_list:
            if item.text != '':
                goals.append(item.text)
            else:
                goals.append(None)

        goal_sub_list = icon_goal[1].find_all("div",{'class':'icons team-players goals'})
        goals_sub = []
        for item in goal_sub_list:
            if item.text != '':
                goals_sub.append(item.text)
            else:
                goals_sub.append(None)

        df_goals = goals + goals_sub

        ### Exchange

        icon_exchange_list = icon_goal[0].find_all("div",{"class":"icons team-players",'id':re.compile('exchange')})
        icon_exchange_sub_list = icon_goal[1].find_all("div",{"class":"icons team-players",'id':re.compile('exchange')})

        excange = []
        for item in icon_exchange_list:
            if item.text != '' and item.text != '\n':
                if(len(item.text) > 4):
                    excange.append(float(re.sub(" ", '.',re.sub("'", '',item.text)[0:len(item.text)])))
                else:
                    excange.append(float(re.sub("'", '',item.text)[0:len(item.text)]))
            else:
                excange.append(None)

        excange_sub = []
        for item in icon_exchange_sub_list:
            if item.text != '' and item.text != '\n':
                if(len(item.text) > 4):
                    excange_sub.append(float(re.sub(" ", '.',re.sub("'", '',item.text)[0:len(item.text)])))
                else:
                    excange_sub.append(float(re.sub("'", '',item.text)[0:len(item.text)]))
            else:
                excange_sub.append(None)

        df_exchange = excange + excange_sub


        ### Cards

        icon_card_list = icon_goal[0].find_all("div",{"class":"icons team-players",'id':re.compile('red')})
        icon_card_sub_list = icon_goal[1].find_all("div",{"class":"icons team-players",'id':re.compile('red')})

        card = []
        for item in icon_card_list:
            if item.text != '' and item.text != '\n':
                card.append(item.text.replace('\t','').replace('\n',''))
            else:
                card.append(None)

        card_sub = []
        for item in icon_card_sub_list:
            if item.text != '' and item.text != '\n':
                card_sub.append(item.text.replace('\t','').replace('\n',''))
            else:
                card_sub.append(None)

        df_card = card + card_sub

        # final dataframe

        p_dict = {'game_id':list_ids,'player_number':df_numbers,
                  'game_status':['opening']*N1 + ['substitute']*N2,
                   'player_name':p_names,
                   'is_captain':p_is_captain,
                  'goals':df_goals,'subtitution':df_exchange,'card':df_card
                 }

        players_df = pd.DataFrame(p_dict)

        players_df['minute_played'] = np.where((players_df.subtitution.notna()) & (players_df.game_status == 'substitute'),
                                               90-players_df['subtitution'],
                                              np.where((players_df.subtitution.notna()) & (players_df.game_status == 'opening'),
                                                      players_df.subtitution,
                                                      np.where(players_df.game_status == 'opening',90,0))
                                              )
        players_df['is_played'] = np.where(players_df.minute_played > 0,True,False)

        return players_df

    #get_players_data()
    mta_player_con = mta_game_id_url['game_url']

    def game_home_away(url):
        r_mta_2 = requests.get(url)
        c_mta_2 = r_mta_2.content
        soup_mta_2 = BeautifulSoup(c_mta_2,"html.parser")
        mta = soup_mta_2.find_all("div",{'class':re.compile('teams')})[0]
        return str(mta)[18:22]

    def get_game_coach(url):
        try:
            r_mta_2 = requests.get(url)
            c_mta_2 = r_mta_2.content
            soup_mta_2 = BeautifulSoup(c_mta_2,"html.parser")
            coaches = soup_mta_2.find_all("div",{"class":"p50 yellow"})
            game_coach = coaches[2].find_all("li")[0].text
            output = game_coach
        except:
            output = None
        return output

    def apply_goals_table(players_data_table):

        def adjust_date(col_name):
            for col in col_name:
                col = col[0:8]
                day = col[0:2]
                month = col[2:4]
                year = col[4:8]
            return year + "-" + month + "-" + day

        mta_events_base = players_data_table[players_data_table.goals.notnull()][['game_id','player_name',
                                                                                  'goals','subtitution','card',
                                                                                  'game_status','minute_played']]

        init_goals = mta_events_base[(mta_events_base.goals.notnull())][['game_id','player_name','goals']]

        if init_goals.shape[0] == 0:
            goal_melted = None
        else:
            goals_new = init_goals['goals'].str.split("'", n = 6, expand = True)
            goals_df =  pd.concat([init_goals, goals_new], axis=1)
            goal_melted = pd.melt(goals_df, id_vars=['game_id','player_name','goals'])
            goal_melted = goal_melted[(goal_melted.value.notnull()) & (goal_melted.value != '')].drop(columns=['goals','variable'])
            goal_melted.value = goal_melted.value.astype(int)
            goal_melted['date'] = pd.to_datetime(goal_melted[['game_id']].apply(adjust_date, axis=1))
            goal_melted = goal_melted.sort_values(by=['date','game_id','value'],ascending = False)
            goal_melted['event_type'] = 'goal_scored'
            goal_melted = goal_melted.reset_index(drop=True)[['date','game_id','player_name','event_type','value']]
        return goal_melted

    game_loc   = game_home_away(mta_player_con[0])
    coach      = get_game_coach(mta_player_con[0])
    players_data = get_players_data(mta_player_con[0])
    events     = apply_goals_table(players_data)

    mta_df = mta_df[mta_df['league'] != 'Friendly'].sort_values(by=['date'],ascending=False)

    final_list = [mta_df[:1],
                  mta_game_id_url[:1],
                  players_data,
                  game_loc,
                  coach,
                  events
                 ]


    return final_list

print('bring_it_on applied')

def mta_lego(main_list):

    def lego_players(list):
        df_players = list[2]
        df = df_players[['game_id','player_number','game_status','player_name','is_captain','subtitution','is_played']]
        df['minutes_played'] = np.where(df.game_status == 'opening',
                                        np.where(df['subtitution'].notnull(),df['subtitution'],90),
                                        np.where(df['subtitution'].notnull(),90-df['subtitution'],0))
        df['is_played'] = np.where(df.minutes_played > 0,True,False)
        df['player_number'] = df['player_number'].astype(str)
        df['con_id'] = df[['game_id', 'player_number']].apply(lambda x: '_'.join(x), axis = 1)
        df = df[['con_id','game_id','player_number','game_status','player_name','is_captain','subtitution','is_played','minutes_played']]
        df['con_id'] = df['con_id'] + df['player_name'].str[0:2]
        return df

    def lego_game(list):
        my_new_df = pd.DataFrame({'game_id':list[0].game_id,
                                  'season':list[0].season,
                                  'date':list[0].date,
                                  'hour':list[0].hour,
                                  'stadium':list[0].stadium,
                                  'location':list[3],
                                  'opponent':list[0].opponent,
                                  'mta_score':list[0].res_1,
                                  'opponent_score':list[0].res_2,
                                  'league_name':list[0].league,
                                  'round':list[0]['round'],
                                  'coach':[list[4]],
                                  'game_url':list[1].game_url})

        my_new_df['league'] = np.where(my_new_df['league_name'].isin(['Tel aviv stock exchange League',
                                                                     'Winner League',
                                                                     'Ligat Japanika']),'League',
                                   np.where(my_new_df['league_name'].isin([' Europa League qualifying phase',
                                                                    'Champions League Qualification', 'Champions League',
                                                                    'Europa League Play-off','Europa League']),'Europe',
                                           my_new_df['league_name']))

        my_new_df['game_type'] = np.where(my_new_df['league_name'].isin(['Tel aviv stock exchange League',
                                                                     'Winner League',
                                                                     'Ligat Japanika','Champions League',
                                                               'Europa League']),'3points',
                                  np.where(my_new_df['league_name'].isin([' Europa League qualifying phase','Champions League Qualification',
                                                                    'Europa League Play-off','State Cup','Toto Cup']),'knockout',
                                           'other'))

        my_new_df['game_result'] = np.where(my_new_df.mta_score > my_new_df.opponent_score,'W',
                                        np.where(my_new_df.mta_score < my_new_df.opponent_score,'L','D'))
        return my_new_df


    def lego_events(list):
        mta_events = list[5]
        mta_events['event_id'] = mta_events['game_id'].str[0:8] + '_' + mta_events['value'].astype(str).str[0:3] + '_' + mta_events['event_type'] + '_' + mta_events['player_name'].str[0:2]
        mta_events = mta_events[['event_id','date','game_id','player_name','event_type','value']].rename(columns={'value':'minute'})
        return mta_events

    try:
        games = lego_game(main_list)
        print('games_ok')
    except Exception as ex:
        print(ex)
    try:
        players = lego_players(main_list)
        print('players_ok')
    except Exception as ex:
        print(ex)
    try:
        events  = lego_events(main_list)
        print('events_ok')
    except Exception as ex:
        print(ex)

    #conn = psycopg2.connect("dbname = 'mta_prod' user = 'postgres' password = 'Fcm180111' host = 'localhost' port = '5434'")
    conn = psycopg2.connect("dbname = 'd5m2p6kka0vf8d' user = 'fzgxltqkgmaklf' password = '6ad610f8f95f1f570ad6c846b68e74f0d692386a8e43d2fce5976f1718e2b779' host = 'ec2-184-73-232-93.compute-1.amazonaws.com' port = '5432'")
    dst_g_cursor = conn.cursor()
    dst_p_cursor = conn.cursor()

        # update carlin relevant table
    try:
        values_list_g = [tuple(x) for x in games.values]
        execute_values(dst_g_cursor,
                      """
                      INSERT INTO "mta_games" ("game_id","season","date","hour","stadium","location",
                      "opponent","mta_score","opponent_score","league_name","round","coach","game_url",
                      "league","game_type","game_result")
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
                          game_type = excluded.game_type
                      """,
                     values_list_g)
        print(values_list_g)
        conn.commit()
        dst_g_cursor.close()
        print('games_db_ok')
    except Exception as ex:
        print(ex)

    try:
        values_list_p = [tuple(x) for x in players.values]
        execute_values(dst_p_cursor,
                      """
                      INSERT INTO "mta_player_con" ("con_id","game_id","player_number","game_status","player_name",
                                  "is_captain","sub","is_played","minutes_played")
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
                          minutes_played = excluded.minutes_played
                      """,
                      values_list_p)
        conn.commit()
        dst_p_cursor.close()
        print('players_db_ok')

    except Exception as ex:
        print(ex)

    try:
        dst_e_cursor = conn.cursor()
        values_list_e = [tuple(x) for x in events.values]
        execute_values(dst_e_cursor,
                              """
                              INSERT INTO "mta_events" ("event_id","date","game_id",
                                                        "player_name","event_type","minute")
                              VALUES %s
                              ON CONFLICT (event_id)
                              DO UPDATE
                              SET minute = excluded.minute,
                                  date = excluded.date,
                                  player_name = excluded.player_name,
                                  event_type = excluded.event_type
                              """,
                               values_list_e)
        print(values_list_e)
        conn.commit()
        dst_e_cursor.close()
        print('events_db_ok')
    except Exception as ex:
        print(ex)
    conn.close()

def mta_scrap_insert():
    try:
        list_output = bring_it_on()
        print(list_output)
        print('new data has been scraped')
    except Exception as ex:
        print('scraping process failed: ---->>')
        print(ex)


    try:
        mta_lego(list_output)
        print('database has been updated')
    except Exception as ex:
        print('database update failed: ---->>')
        print(ex)

### Final command
mta_scrap_insert()
