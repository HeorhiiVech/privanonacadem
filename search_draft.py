import sqlite3
import json
import os
from database import get_db_connection

LEAGUES_FILE = 'leagues.json'

def load_leagues():
    if os.path.exists(LEAGUES_FILE):
        with open(LEAGUES_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {}

def get_filter_options():
    conn = get_db_connection()
    options = {
        'patches': [],
        'teams': [],
        'champions': [],
        'game_numbers': [],
        'leagues': list(load_leagues().keys())
    }
    
    if not conn:
        return options

    try:
        cursor = conn.cursor()
        cursor.execute('SELECT DISTINCT Patch FROM tournament_games WHERE Patch IS NOT NULL AND Patch != "N/A" ORDER BY Patch DESC')
        options['patches'] = [row['Patch'] for row in cursor.fetchall()]
        
        cursor.execute('''
            SELECT DISTINCT Blue_Team_Name as team FROM tournament_games WHERE Blue_Team_Name NOT IN ('UnknownBlue', 'Blue Team', 'N/A')
            UNION
            SELECT DISTINCT Red_Team_Name as team FROM tournament_games WHERE Red_Team_Name NOT IN ('UnknownRed', 'Red Team', 'N/A')
            ORDER BY team ASC
        ''')
        options['teams'] = [row['team'] for row in cursor.fetchall()]
        
        cursor.execute('SELECT DISTINCT Sequence_Number FROM tournament_games WHERE Sequence_Number IS NOT NULL AND Sequence_Number > 0 ORDER BY Sequence_Number ASC')
        options['game_numbers'] = [row['Sequence_Number'] for row in cursor.fetchall()]
        
        pick_slots = [7, 8, 9, 10, 11, 12, 17, 18, 19, 20]
        union_query = " UNION ".join([f"SELECT DISTINCT Draft_Action_{i}_ChampName as champ FROM tournament_games" for i in pick_slots])
        cursor.execute(union_query)
        
        champs = set(row['champ'] for row in cursor.fetchall() if row['champ'] and row['champ'] != 'N/A')
        options['champions'] = sorted(list(champs))
        
    except sqlite3.Error as e:
        print(f"Error fetching filter options: {e}")
    finally:
        conn.close()
    return options

def get_filtered_drafts(filters):
    conn = get_db_connection()
    if not conn:
        return []

    leagues_data = load_leagues()
    drafts = []

    try:
        cursor = conn.cursor()
        query = "SELECT * FROM tournament_games WHERE 1=1"
        params = []

        if filters.get('league') and filters['league'] in leagues_data:
            teams_in_league = leagues_data[filters['league']]
            if teams_in_league:
                placeholders = ','.join(['?'] * len(teams_in_league))
                query += f" AND (Blue_Team_Name IN ({placeholders}) OR Red_Team_Name IN ({placeholders}))"
                params.extend(teams_in_league * 2)

        if filters.get('patch'):
            query += " AND Patch = ?"
            params.append(filters['patch'])

        if filters.get('team'):
            query += " AND (Blue_Team_Name = ? OR Red_Team_Name = ?)"
            params.extend([filters['team'], filters['team']])

        if filters.get('game_number'):
            query += " AND Sequence_Number = ?"
            params.append(filters['game_number'])

        blue_pick_slots = [7, 10, 11, 18, 19]
        red_pick_slots = [8, 9, 12, 17, 20]
        all_pick_slots = blue_pick_slots + red_pick_slots

        selected_champ = filters.get('champion')
        selected_result = filters.get('result')
        selected_side = filters.get('side')

        if selected_champ:
            if selected_side == 'Blue':
                side_query = " OR ".join([f"Draft_Action_{i}_ChampName = ?" for i in blue_pick_slots])
                query += f" AND ({side_query})"
                params.extend([selected_champ] * len(blue_pick_slots))
            elif selected_side == 'Red':
                side_query = " OR ".join([f"Draft_Action_{i}_ChampName = ?" for i in red_pick_slots])
                query += f" AND ({side_query})"
                params.extend([selected_champ] * len(red_pick_slots))
            else:
                pick_placeholders = " OR ".join([f"Draft_Action_{i}_ChampName = ?" for i in all_pick_slots])
                query += f" AND ({pick_placeholders})"
                params.extend([selected_champ] * len(all_pick_slots))
            
            if selected_result:
                if selected_result == "Win":
                    query += f" AND (({(' OR '.join([f'Draft_Action_{i}_ChampName = ?' for i in blue_pick_slots]))}) AND Winner_Side = 'Blue' OR ({(' OR '.join([f'Draft_Action_{i}_ChampName = ?' for i in red_pick_slots]))}) AND Winner_Side = 'Red')"
                else:
                    query += f" AND (({(' OR '.join([f'Draft_Action_{i}_ChampName = ?' for i in blue_pick_slots]))}) AND Winner_Side = 'Red' OR ({(' OR '.join([f'Draft_Action_{i}_ChampName = ?' for i in red_pick_slots]))}) AND Winner_Side = 'Blue')"
                
                params.extend([selected_champ] * len(blue_pick_slots))
                params.extend([selected_champ] * len(red_pick_slots))

        query += ' ORDER BY "Date" DESC LIMIT 50'
        
        cursor.execute(query, params)
        rows = cursor.fetchall()

        for row in rows:
            game = dict(row)
            draft_actions_dict = {}
            for i in range(1, 21):
                champ_name = game.get(f"Draft_Action_{i}_ChampName")
                draft_actions_dict[i] = {"Champion_Name": champ_name if champ_name else "N/A"}

            winner = game.get('Winner_Side', 'Unknown')
            blue_team = game.get('Blue_Team_Name', 'Unknown Blue')
            red_team = game.get('Red_Team_Name', 'Unknown Red')
            
            blue_result = "WIN" if winner == "Blue" else ("LOSS" if winner == "Red" else "-")
            red_result = "WIN" if winner == "Red" else ("LOSS" if winner == "Blue" else "-")

            # --- Логика проверки перевернутого драфта ---
            blue_map_champs = set()
            for role in ["TOP", "JGL", "MID", "BOT", "SUP"]:
                champ = game.get(f"Blue_{role}_Champ")
                if champ and champ != "N/A":
                    blue_map_champs.add(champ)
                    
            first_pick_champs = []
            for seq in [7, 10, 11, 18, 19]:
                champ = game.get(f"Draft_Action_{seq}_ChampName")
                if champ and champ != "N/A":
                    first_pick_champs.append(champ)
                    
            match_count = sum(1 for c in first_pick_champs if c in blue_map_champs)
            is_draft_swapped = (match_count < 3 and len(first_pick_champs) > 0)

            # Определяем, кто реально слева (First Pick), а кто справа
            draft_left_team = red_team if is_draft_swapped else blue_team
            draft_right_team = blue_team if is_draft_swapped else red_team
            draft_left_result = red_result if is_draft_swapped else blue_result
            draft_right_result = blue_result if is_draft_swapped else red_result

            drafts.append({
                "game_id": game.get('Game_ID'),
                "date": game.get('Date', 'N/A'),
                "patch": game.get('Patch', 'N/A'),
                "sequence_number": game.get('Sequence_Number', 0),
                # Используем новые переменные для вывода в HTML
                "draft_left_team_tag": draft_left_team,
                "draft_right_team_tag": draft_right_team,
                "draft_left_result": draft_left_result,
                "draft_right_result": draft_right_result,
                "draft_actions_dict": draft_actions_dict
            })

    except sqlite3.Error as e:
        print(f"Error filtering drafts: {e}")
    finally:
        conn.close()
    return drafts