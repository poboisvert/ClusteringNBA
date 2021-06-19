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

# Collection Name - Datasets
seasons_collection = db.get_collection("Season_Dataset")
cleaned_collection = db.get_collection("Cleaned_Dataset")
# Collection Name - Players
players_collection = db.get_collection("Player_Dataset")


# Helpers - Response JSON content from server
def player_helper(player) -> dict:
    return {
        "id": str(player["_id"]),
        "Year": player["Year"],
        "Player": player["Player"],
        "Age": player["Age"],
        "G": player["G"],
        "MP": player["MP"],
        "PER": player["PER"],
        "TS_rel": float(player["TS_rel"]),
        "treePAr": float(player["treePAr"]),
        "FTr": float(player["FTr"]),
        "ORB_rel": float(player["ORB_rel"]),
        "DRB_rel": float(player["DRB_rel"]),
        "TRB_rel": float(player["TRB_rel"]),
        "AST_rel": float(player["AST_rel"]),
        "STL_rel": float(player["STL_rel"]),
        "BLK_rel": float(player["BLK_rel"]),
        "TOV_rel": float(player["TOV_rel"]),
        "USG_rel": float(player["USG_rel"]),
        "OWS": float(player["OWS"]),
        "DWS": float(player["DWS"]),
        "OBPM": float(player["OBPM"]),
        "DBPM": float(player["DBPM"]),
        "BPM": float(player["BPM"]),
        "VORP": float(player["VORP"]),
        "FG": float(player["FG"]),
        "FGA": float(player["FGA"]),
        "treeP": float(player["treeP"]),
        "treePA": float(player["treePA"]),
        "twoP": float(player["twoP"]),
        "twoPA": float(player["twoPA"]),
        "eFG_rel": float(player["eFG_rel"]),
        "FT": float(player["FT"]),
        "FTA": float(player["FTA"]),
        "ORB": float(player["ORB"]),
        "DRB": float(player["DRB"]),
        "TRB": float(player["TRB"]),
        "AST": float(player["AST"]),
        "STL": float(player["STL"]),
        "BLK": float(player["BLK"]),
        "TOV": float(player["TOV"]),
        "PF": float(player["PF"]),
        "PTS": float(player["PTS"]),
        "weight": float(player["weight"]),
        "height(inch)": float(player["height(inch)"]),
    }

# CSV Reader Helper


def csv_to_json(filename, header=0):
    data = pd.read_csv(filename)
    return data.to_dict('records')

# Retrieve all players present in the database


async def retrieve_allplayers():
    players = []
    for player in cleaned_collection.find():
        players.append(player_helper(player))
    return players

# Retrieve a players with a matching ID


async def retrieve_player(id: str) -> dict:
    player = cleaned_collection.find_one(
        {"_id": ObjectId(id)})
    if player:
        return player_helper(player)
    else:
        return

# Add a new players into to the database


async def add_dataset(filename):
    # Push all dataset to MongoDB
    # Convert to a switch case
    if filename == 'datasets/SeasonsDataRaw.csv':
        seasons_collection.insert_many(csv_to_json(filename))

    if filename == 'datasets/SeasonsDataCleaned.csv':
        cleaned_collection.insert_many(csv_to_json(filename))

    if filename == 'datasets/PlayerDataRaw.csv':
        players_collection.insert_many(csv_to_json(filename))

    return filename


async def clean_dataset():

    # Connection to DB
    #collection_conn = db['Season_Dataset']
    #collection_cursor = collection_conn.find()
    #collection_nba_df = pd.DataFrame(list(collection_cursor))
    file_path = 'datasets/SeasonsDataRaw.csv'
    collection_nba_df = pd.read_csv(file_path)
    # First drop columns
    clean_df = collection_nba_df.drop(
        columns=['GS', 'Pos', 'position', 'year_end', 'college', 'birth_date', 'name'])

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

    # FASTAPI do not return if NaN
    b_df.fillna(0)

    # Export to csv
    b_df.to_csv('datasets/SeasonsDataCleaned.csv', index=False)

    # print(collection_pandas_df)
    print('Max Pain 1.01 Completed - Cleaned')
    return b_df
