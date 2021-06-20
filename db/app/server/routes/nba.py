from fastapi import APIRouter, Body
from fastapi.encoders import jsonable_encoder

from app.server.database import (
    add_dataset,
    clean_dataset,
    retrieve_player,
    retrieve_allplayers,
    get_pca
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


@ router.post("/etl/transform", response_description="All players retreived from the database")
async def cleaning(name='SeasonsDataRaw.csv'):

    # Create the filepath
    filepath = 'datasets/' + name
    # Clean the csv file
    print(' === Cleaning the .CSV === ')
    filepath_clean = await clean_dataset(filepath)
    print(' === Cleaning the .CSV - Completed === ')
    print(' === Dataset is loaded in Mongo DB === ')
    print(filepath_clean)

    # Load to Mongo DB after cleaning
    await add_dataset(filepath_clean)
    print(' === Dataset is loaded in Mongo DB - Completed ===')

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


@ router.get("/ml", response_description="All data for ML retreived from the database")
async def get_pca_data():
    print(' === ML - Start === ')
    pca_result = await get_pca()

    # Load to Mongo DB after cleaning && ML
    await add_dataset(pca_result)

    return ResponseModel(pca_result, "ML in Mongo DB created successfully.")
