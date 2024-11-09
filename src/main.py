import requests
import pandas as pd
import duckdb
import datetime
import time
# Set up API details and DuckDB connection
api_key = 'Your API KEY'
game_name = 'BlackInter69'
tag_line = 'NA1'
duckdb_conn = duckdb.connect('league_data.db')

# Define function to get item data from Data Dragon API
def get_item_mapping():
    version_url = 'https://ddragon.leagueoflegends.com/api/versions.json'
    version = requests.get(version_url).json()[0]  # Get the latest version

    items_url = f'https://ddragon.leagueoflegends.com/cdn/{version}/data/en_US/item.json'
    items_data = requests.get(items_url).json()['data']

    # Create a mapping of item ID to item name
    item_mapping = {int(item_id): item_info['name'] for item_id, item_info in items_data.items()}
    return item_mapping

# Define function to get the player's PUUID
def get_puuid():
    url = f'https://americas.api.riotgames.com/riot/account/v1/accounts/by-riot-id/{game_name}/{tag_line}'
    headers = {'X-Riot-Token': api_key}
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.json().get('puuid')
    else:
        print("Error fetching summoner data")
        return None

# Define function to get match history IDs
def get_match_ids(puuid,count):
    match_url = f'https://americas.api.riotgames.com/lol/match/v5/matches/by-puuid/{puuid}/ids?start=0&count={count}'
    headers = {'X-Riot-Token': api_key}
    response = requests.get(match_url, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        print("Error fetching match history")
        return []

# Define function to fetch match details
def get_match_details(match_id):
    url = f'https://americas.api.riotgames.com/lol/match/v5/matches/{match_id}'
    headers = {'X-Riot-Token': api_key}
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error fetching data for match {match_id}")
        return None


def create_tables():
    # funciton to create tables
    duckdb_conn.execute("""
        CREATE TABLE IF NOT EXISTS matches (
            match_id TEXT PRIMARY KEY,
            datetime TIMESTAMP,
            game_duration INTEGER,
            win BOOLEAN,
            lane TEXT,
            player_champ TEXT,
            opposing_champ TEXT
        );
    """)
    duckdb_conn.execute("""
        CREATE TABLE IF NOT EXISTS champs (
            match_id TEXT PRIMARY KEY,
            friend_champ1 TEXT,
            friend_champ2 TEXT,
            friend_champ3 TEXT,
            friend_champ4 TEXT,
            friend_champ5 TEXT
        );
    """)
    duckdb_conn.execute("""
        CREATE TABLE IF NOT EXISTS items (
            match_id TEXT PRIMARY KEY,
            item1 TEXT, item2 TEXT, item3 TEXT, item4 TEXT,
            item5 TEXT, item6 TEXT, item7 TEXT, item8 TEXT
        );
    """)
    duckdb_conn.execute("""
        CREATE TABLE IF NOT EXISTS gold (
            match_id TEXT PRIMARY KEY,
            friend_gold1 INTEGER, friend_gold2 INTEGER, friend_gold3 INTEGER, friend_gold4 INTEGER, friend_gold5 INTEGER,
            enemy_gold1 INTEGER, enemy_gold2 INTEGER, enemy_gold3 INTEGER, enemy_gold4 INTEGER, enemy_gold5 INTEGER
        );
    """)



def reset_tables():
    #funciton to delete data in all tables   
    duckdb_conn = duckdb.connect('league_data.db')
    duckdb_conn.execute("delete from matches")
    duckdb_conn.execute("delete from items")
    duckdb_conn.execute("delete from champs")
    duckdb_conn.execute("delete from gold")
    print("\nTable Data Deleted")


# Main function to fetch match data and populate tables
def fetch_and_store_match_data():
    global game_name
    
    item_mapping = get_item_mapping()
    puuid = get_puuid()
    if not puuid:
        return
    
    #ask user how many games
    while True:
        count = input("\nHow many games would you like to retrieve?\n")
        try:
            # Check if the input is a valid integer
            int(count)
            # Convert the validated integer input to a string
            count = str(count)
            print(f"Retrieving last {count} games for {game_name}")
            break
        except ValueError:
            print("Please enter a valid integer for the number of games.")
    match_ids = get_match_ids(puuid,count)
    
    #count to initialize wait
    match_count = 0
    #wait count variable for api
    wait_count = 0
    for match_id in match_ids:
        match_data = get_match_details(match_id)
        if not match_data:
            continue

        # Extract general match information
        match_count += 1
        wait_count += 1
        match_datetime = datetime.datetime.fromtimestamp(match_data['info']['gameCreation'] / 1000)
        game_duration = match_data['info']['gameDuration']
        participant_data = next((p for p in match_data['info']['participants'] if p['puuid'] == puuid), None)
        if not participant_data:
            continue
        win = participant_data['win']
        player_champ = participant_data['championName']
        lane = participant_data['lane']
        team_id = participant_data['teamId']
        # Function to find the opposing champion in the same lane/position
        opponent_data = next((p for p in match_data['info']['participants'] 
                      if p['puuid'] != puuid 
                      and p['lane'] == lane 
                      and p['teamId'] != team_id), None)
        
        try:
            opposing_champ = opponent_data['championName']
        except TypeError:
            #if cant find opposing champ, put unknown
            opposing_champ = "Unknown"
        
        # Insert into matches table
        duckdb_conn.execute("""
            INSERT INTO matches (match_id, datetime, game_duration, win, lane, player_champ, opposing_champ)
            VALUES (?, ?, ?, ?, ?, ?,?)
        """, (match_id, match_datetime, game_duration, win, lane, player_champ, opposing_champ))
        
        
        # Collect friendly champions
        friend_champs = [p['championName'] for p in match_data['info']['participants'] if p['teamId'] == participant_data['teamId']]
        friend_champs += [None] * (5 - len(friend_champs))  # Pad to ensure 5 entries

        # Insert into champs table
        duckdb_conn.execute("""
            INSERT INTO champs (match_id, friend_champ1, friend_champ2, friend_champ3, friend_champ4, friend_champ5)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (match_id, *friend_champs))

        # Collect items (up to 8)
        items = [
            item_mapping.get(participant_data[f'item{i}'], 'Unknown') if f'item{i}' in participant_data else None
            for i in range(8)
        ]

        # Insert into items table
        duckdb_conn.execute("""
            INSERT INTO items (match_id, item1, item2, item3, item4, item5, item6, item7, item8)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (match_id, *items))

        # Collect gold data for friendly and enemy teams
        friend_gold = [p['goldEarned'] for p in match_data['info']['participants'] if p['teamId'] == participant_data['teamId']]
        enemy_gold = [p['goldEarned'] for p in match_data['info']['participants'] if p['teamId'] != participant_data['teamId']]
        friend_gold += [None] * (5 - len(friend_gold))  # Pad to ensure 5 entries
        enemy_gold += [None] * (5 - len(enemy_gold))  # Pad to ensure 5 entries

        # Insert into gold table
        duckdb_conn.execute("""
            INSERT INTO gold (match_id, friend_gold1, friend_gold2, friend_gold3, friend_gold4, friend_gold5,
                              enemy_gold1, enemy_gold2, enemy_gold3, enemy_gold4, enemy_gold5)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (match_id, *friend_gold, *enemy_gold))

        print(f"Inserted match {match_count} out of {count}: {match_id} from {match_datetime}")
        if wait_count == 25:
            print("\nWaiting 10 sec for api rate limit...\n")
            time.sleep(10)
            wait_count = 0



def main():
    # create tables
    #ask if wants to reset tables
    global game_name
    while True:
        decision = input("Would you like to delete existing table data? y or n?:\n")
        if decision.lower() == 'y':
            print("\nDeleting table data")
            reset_tables()
            break
        if decision.lower() == 'n':
            break
        else:
            print("\nPlease enter y or n")
    game_name = input("\nWhat is your summoner_name?:\n").strip()      
    create_tables()
    fetch_and_store_match_data()
    
     
    lol_df = duckdb_conn.execute("""SELECT *
    FROM matches
    JOIN champs ON matches.match_id = champs.match_id
    JOIN items ON matches.match_id = items.match_id
    JOIN gold ON matches.match_id = gold.match_id;""").df()
    
    
    lol_df.to_csv('lol_data.csv')
    return print("csv updated")


main()
