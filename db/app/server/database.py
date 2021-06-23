# Dependencies
from pymongo import MongoClient
from bson.objectid import ObjectId
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

from sklearn.preprocessing import StandardScaler, MinMaxScaler
from sklearn.decomposition import PCA
from sklearn.cluster import KMeans

from collections import Counter

# Init Mongo
MONGO_DETAILS = "mongodb://localhost:27017"

client = MongoClient(MONGO_DETAILS)

# Database Name
db = client.players

# Collection Name - Datasets
seasons_collection = db.get_collection("Season_Dataset")
cleaned_collection = db.get_collection("Cleaned_Dataset")
# Collection Name - ML
pca_collection = db.get_collection("PCA_Dataset")
timeseries_collection = db.get_collection("Timeseries_Dataset")


# Helpers - Response JSON content from server
def player_helper(player) -> dict:
    return {
        "id": str(player["_id"]),
        "Year": player["Year"],
        "Player": player["Player"],
        "Age": player["Age"],
        "G": player["G"],
        "MP": player["MP"],
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

    if filename == 'datasets/SeasonsDataCleanedPCA.csv':
        pca_collection.insert_many(csv_to_json(filename))

    if filename == 'datasets/SeasonsDataCleanedPCATS.csv':
        timeseries_collection.insert_many(csv_to_json(filename))

    return filename


async def clean_dataset(filepath):

    # file_path = 'datasets/SeasonsDataRaw.csv'
    if filepath is None:
        return

    collection_nba_df = pd.read_csv(filepath)
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
    # you might run into issues where b_df.feet is not numeric. It'll depend on the flow of the logic, but keep this in mind
    b_df["height(inch)"] = b_df.feet+b_df.inch

    # Drop the columns used for calculation, blanks and repetitive data
    b_df = b_df.drop(columns=['Tm', 'height', 'feet', 'inch',
                              'blanl', 'blank2', 'FG_rel', 'treeP_rel', 'twoP_rel', 'FT_rel'], axis=1)

    #   Business rule
    b_df = b_df[b_df["G"] > 10]

    # FASTAPI do not return if NaN
    b_df.fillna(0, inplace=True)

    # Export to csv
    b_df.to_csv('datasets/SeasonsDataCleaned.csv', index=False)

    # print(collection_pandas_df)
    print(' === Max Pain 1.01 - Cleaned === ')

    return 'datasets/SeasonsDataCleaned.csv'

# Add a ML filtered data to DB


async def get_pca():
    try:
            # Connection to DB
            collection_conn = db['Cleaned_Dataset']
            collection_cursor = collection_conn.find()
            a_df = pd.DataFrame(list(collection_cursor))
    except KeyError:
            print("ERROR")
            return ''
    # print(a_df)

    # Player columns
    player_name = pd.DataFrame(a_df['Player'])

    # Drop to perfom model - business rule
    nba_drop_df = a_df.drop(['_id', 'Player', 'year_born', 'year_start',
                             'Status', 'OWS', 'DWS', 'WS', 'WSon48', 'VORP', 'PER'], axis=1)

    nba_scaled = StandardScaler().fit_transform(nba_drop_df)

    variances = []
    for n in range(1, nba_scaled.shape[1]):
        pca = PCA(n_components=n)
        pca.fit(nba_scaled)
        variances.append(sum(pca.explained_variance_ratio_))

    # Using PCA to reduce dimension to 10 principal components.
    n_comp = 10

    pca = PCA(n_components=n_comp)

    nba_pca = pca.fit_transform(nba_scaled)

    # Create New Dataframe with the princiapl components
    pcs_df = pd.DataFrame(data=nba_pca, columns=[
                          f'PC {x}' for x in range(n_comp)], index=player_name.index)

    # Calculate the inertia for the range of K values
    inertias = []
    for k in range(1, 20):
        model = KMeans(n_clusters=k, random_state=0)
        model.fit(pcs_df)
        inertias.append(model.inertia_)

    secondDeriv = [0]
    for i in range(1, 18):
        secondDeriv.append(inertias[i+1] + inertias[i-1] - 2*inertias[i])
    secondDeriv.append(0)

    # Initialize the K-Means model.
    model = KMeans(n_clusters=9, random_state=0)

    # Fit the model
    model.fit(pcs_df)

    # Predict clusters
    predictions = model.predict(pcs_df)

    cluster_df = pd.DataFrame(predictions, columns=['cluster'])
    cluster_vorp = cluster_df.join(a_df)
    cluster_df = pd.DataFrame(predictions, columns=['cluster'])
    cluster_ws = cluster_df.join(a_df)
    a_df['cluster'] = model.labels_

    # Create new df with correct column headers
    cols = ["Player", "year_born"]
    for i in range(1, 22):
        cols.extend([f"Cluster {i}", f"VORP {i}", f"WS {i}"])
    b_df = pd.DataFrame(columns=cols)

    # Create loop that fills in the data from original df
    for index, row in a_df.iterrows():
        subset = a_df[(a_df["Player"] == row["Player"]) &
                      (a_df["year_born"] == row["year_born"])]
        if (subset["Player"].iloc[0], subset["year_born"].iloc[0]) not in zip(b_df["Player"], b_df["year_born"]):
            new_row = {}
            new_row["Player"] = subset["Player"].iloc[0]
            new_row["year_born"] = subset["year_born"].iloc[0]
            for index2, sub in subset.iterrows():
                new_row[f"Cluster {sub['Status']}"] = sub["cluster"]
                new_row[f"VORP {sub['Status']}"] = sub["VORP"]
                new_row[f"WS {sub['Status']}"] = sub["WS"]
            b_df = b_df.append(new_row, ignore_index=True)

    # Export to csv
    b_df.to_csv('datasets/SeasonsDataCleanedPCA.csv', index=False)
    return 'datasets/SeasonsDataCleanedPCA.csv'


async def get_timeseries():

    # Connection to DB
    collection_conn = db['PCA_Dataset']
    collection_cursor = collection_conn.find()
    clusters_df = pd.DataFrame(list(collection_cursor))

    # Mongo DB to drop - Result from Export then Import
    clusters_df = clusters_df.drop(['_id'], axis=1)

    # Array for storage
    improvements = []
    regressions = []

    # Loop accross clusters
    for index, example in clusters_df.iterrows():

        # To edit since at the moment we have 21 clusters
        for i in range(1, 21):
            # if no nan is cluster i ; cluster i + 1 ; clust i is not cluster i + 1 - last
            if not pd.isna(example[f"Cluster {i}"]) and not pd.isna(example[f"Cluster {i+1}"]) and (example[f"Cluster {i}"] != example[f"Cluster {i+1}"]):
                if example[f"VORP {i}"] > example[f"VORP {i+1}"]:
                    # Save as int and a list - regressions
                    regressions.append(
                        (int(example[f"Cluster {i}"]), int(example[f"Cluster {i+1}"])))
                else:
                    # Save as int and a list - improvements
                    improvements.append(
                        (int(example[f"Cluster {i}"]), int(example[f"Cluster {i+1}"])))

    # Improvement to DF
    improvements_list = dict(Counter(improvements))
    improvements_df = pd.DataFrame({"Count": [x for x in list(improvements_list)], "Number": [
                                   improvements_list[x] for x in list(improvements_list)]})
    # Regressions to DF
    regressions_list = dict(Counter(regressions))
    regressions_df = pd.DataFrame({"Count": [x for x in list(regressions_list)], "Number": [
                                  regressions_list[x] for x in list(regressions_list)]})
    # Merge Improvement && Regressions
    merged_df = pd.merge(regressions_df, improvements_df,
                         on='Count', how='outer')
    # Add naming
    merged_df.columns = ['Change', 'Regressions', 'Improvements']

    # Fill na with 0 - Team decision
    merged_df = merged_df.fillna(0)
    # Create a Total column
    merged_df["Total"] = merged_df["Regressions"] + merged_df["Improvements"]

    # Create a Total Rel. column
    merged_df["Improvement Change %"] = merged_df["Improvements"] / \
        merged_df["Total"] * 100

    # Export to csv
    merged_df.to_csv('datasets/SeasonsDataCleanedPCATS.csv', index=False)
    return 'datasets/SeasonsDataCleanedPCATS.csv'
