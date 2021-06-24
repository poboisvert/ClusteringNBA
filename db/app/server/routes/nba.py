from fastapi import APIRouter, Body
from fastapi.encoders import jsonable_encoder

from app.server.database import (
    add_dataset,
    clean_dataset,
    retrieve_player,
    retrieve_allplayers,
    get_pca,
    get_timeseries
)
from app.server.models.nba import (
    ErrorResponseModel,
    ResponseModel,
    PlayerSchema,
)

router = APIRouter()


@router.post("/etl/load", response_description="Dataset data added into the database")
async def dataset(filename='datasets/SeasonsDataCleaned.csv'):

    # Call add_dataset
    # generic = await add_dataset('datasets/PlayerDataRaw.csv')

    # Import the generic statistics for all seasons
    custom = await add_dataset(filename)

    # Clean the first import and save it as a new file
    if filename == 'datasets/SeasonsDataCleaned.csv':
        custom = await add_dataset('datasets/SeasonsDataCleaned.csv')

    return {custom, "Dataset is in MongoDB"}

# ETL
# https://testdriven.io/blog/fastapi-mongo/


@ router.post("/etl/transformLoad", response_description="All players retreived from the database")
async def cleaning(name='SeasonsDataRaw.csv'):

    # Create the filepath
    filepath = 'datasets/' + name
    # Clean the csv file
    print(' === Cleaning the .CSV === ')
    filepath_clean = await clean_dataset(filepath)
    print(' === Cleaning the .CSV - Completed === ')
    # print(filepath_clean)

    # Load to Mongo DB after cleaning
    print(' === TransformLoad DB - Start === ')
    await add_dataset(filepath_clean)
    print(' === TransformLoad DB - End === ')

    return {"Dataset is cleaned"}


@ router.get("/players/all", response_description="All players retreived from the database")
async def get_players_data():
    allplayers = await retrieve_allplayers()
    return ResponseModel(allplayers, "All players fetch successfully.")


@ router.get("/players/{id}", response_description="Player ID retreived from the database")
async def get_players_data(id):
    player = await retrieve_player(id)
    return ResponseModel(player, "Player ID fetch successfully.")
 # Machine Learning


@ router.get("/ml/pca", response_description="All data for ML retreived from the database")
async def get_pca_data():
    print(' === ML PCA - Start === ')
    pca_result = await get_pca()
    print(' === ML PCA - End === ')

    # Load to Mongo DB after cleaning && ML
    print(' === PCA DB - Start === ')
    await add_dataset(pca_result)
    print(' === PCA DB - End === ')
    return ResponseModel(pca_result, "ML PCA in Mongo DB created successfully.")


@ router.get("/ml/timeseries", response_description="All data for ML retreived from the database")
async def get_pca_data():
    print(' === ML Timeseries - Start === ')
    timeseries_result = await get_timeseries()
    print(' === ML Timeseries - End === ')

    # Load to Mongo DB after cleaning && ML
    print(' === ML Timeseries DB - Start === ')
    await add_dataset(timeseries_result)
    print(' === ML Timeseries DB - End === ')

    return ResponseModel(timeseries_result, "ML Timeseries in Mongo DB created successfully.")
