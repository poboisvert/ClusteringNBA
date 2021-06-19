# Dependencies
from pymongo import MongoClient
from bson.objectid import ObjectId
import pandas as pd
import numpy as np

# Init Mongo
MONGO_DETAILS = "mongodb://localhost:27017"

client = MongoClient(MONGO_DETAILS)

# Database Name
db = client.players

# Collection Name
seasons_collection = db.get_collection("Season_Dataset")
cleaned_collection = db.get_collection("Cleaned_Dataset")
players_collection = db.get_collection("Player_Dataset")


# Helpers - Response JSON content from server
def player_helper(player) -> dict:
    return {
        "id": str(player["_id"]),
        "Year": player["Year"],
        "Player": player["Player"],
        "Age": player["Age"],
        "Tm": player["Tm"],
        "G": player["G"],
        # "GS": player["GS"],
        "MP": player["MP"],
        "PER": player["PER"],
        "TS_rel": player["TS_rel"],
        "treePAr": player["treePAr"],
        "FTr": player["FTr"],
        "ORB_rel": player["ORB_rel"],
        "DRB_rel": player["DRB_rel"],
        "TRB_rel": player["TRB_rel"],
        "AST_rel": player["AST_rel"],
        "STL_rel": player["STL_rel"],
        "BLK_rel": player["BLK_rel"],
        "TOV_rel": player["TOV_rel"],
        "USG_rel": player["USG_rel"],
        "blanl": player["blanl"],
        "OWS": player["OWS"],
        "DWS": player["DWS"],
        "WS": player["WS"],
        "WSon48": player["WSon48"],
        # "blank2": player["blank2"],
        "OBPM": player["OBPM"],
        "DBPM": player["DBPM"],
        "BPM": player["BPM"],
        "VORP": player["VORP"],
        "FG": player["FG"],
        "FGA": player["FGA"],
        "FG_rel": player["FG_rel"],
        "treeP": player["treeP"],
        "treePA": player["treePA"],
        "treeP_rel": player["treeP_rel"],
        "twoP": player["twoP"],
        "twoPA": player["twoPA"],
        "twoP_rel": player["twoP_rel"],
        "eFG_rel": player["eFG_rel"],
        "FT": player["FT"],
        "FTA": player["FTA"],
        "FT_rel": player["FT_rel"],
        "ORB": player["ORB"],
        "DRB": player["DRB"],
        "TRB": player["TRB"],
        "AST": player["AST"],
        "STL": player["STL"],
        "BLK": player["BLK"],
        "TOV": player["TOV"],
        "PF": player["PF"],
        "PTS": player["PTS"],
        # "name": player["name"],
        "year_start": player["year_start"],
        # "year_end": player["year_end"],
        # "position": player["position"],
        "height": player["height"],
        "weight": player["weight"],
        # "birth_date": player["birth_date"],
        # "college": player["college"],
        "year_born": player["year_born"],
    }

# CSV Reader Helper


def csv_to_json(filename, header=0):
    data = pd.read_csv(filename)
    return data.to_dict('records')

# Retrieve all players present in the database


async def retrieve_allplayers():
    players = []
    for player in seasons_collection.find():
        players.append(player_helper(player))
    return players

# Retrieve a players with a matching ID


async def retrieve_player(id: str) -> dict:
    player = await seasons_collection.find_one({"_id": ObjectId(id)})
    if player:
        return player_helper(player)

# Add a new players into to the database


async def add_dataset(filename):
    # Push all dataset to MongoDB
    if filename == 'datasets/SeasonsDataRaw.csv':
        seasons_collection.insert_many(csv_to_json(filename))

    if filename == 'datasets/SeasonsDataCleaned.csv':
        cleaned_collection.insert_many(csv_to_json(filename))

    if filename == 'datasets/PlayerDataRaw.csv':
        players_collection.insert_many(csv_to_json(filename))

    return filename


async def clean_dataset():

    # Connection to DB
    collection_conn = db['Season_Dataset']
    collection_cursor = collection_conn.find()
    collection_nba_df = pd.DataFrame(list(collection_cursor))

    # First drop columns
    clean_df = collection_nba_df.drop(columns=[
        'GS', 'Pos', 'position', 'year_end', 'college', 'birth_date', 'name', ])

    # Drop
    b_df = pd.DataFrame(columns=clean_df.columns)

    # Matching on Player and year_born players DataFrame
    for index, row in clean_df.iterrows():
        subset = clean_df[(clean_df["Year"] == row["Year"]) & (
            clean_df["Player"] == row["Player"]) & (clean_df["year_born"] == row["year_born"])]
        if len(subset) == 1:
            b_df = b_df.append(subset)
        elif subset[subset["Tm"] == "TOT"].index[0] not in b_df.index:
            b_df = b_df.append(subset[subset["Tm"] == "TOT"])

    # Converting height from feets to cm
    b_df["Status"] = (b_df["Year"] - b_df["year_start"] + 1)

    # Seperate feet and inches and convert all measurments to inches
    b_df['feet'] = b_df.height.str.split('-').str[0].astype(np.float) * 12
    b_df['inch'] = b_df.height.str.split('-').str[1].astype(np.float)

    # Combine measurements in new column "height(inch)"
    b_df["height(inch)"] = b_df.feet+b_df.inch

    # Drop the columns used for calculation, blanks and repetitive data
    b_df = b_df.drop(columns=['Tm', 'height', 'feet', 'inch',
                              'blanl', 'blank2', 'FG_rel', 'treeP_rel', 'twoP_rel', 'FT_rel'], axis=1)

    #   Business rule
    b_df = b_df[b_df["G"] > 10]

    # Export to csv
    b_df.to_csv('datasets/SeasonsDataCleaned.csv', index=False)

    # print(collection_pandas_df)
    print('Max Pain 1.01 Completed - Cleaned')
    return b_df
