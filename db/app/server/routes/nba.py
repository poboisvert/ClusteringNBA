from fastapi import APIRouter, Body
from fastapi.encoders import jsonable_encoder

from app.server.database import (
    add_dataset,
    clean_dataset,
    retrieve_player,
    retrieve_allplayers,
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
    filepath = 'datasets/' + name
    print(' === Cleaning the .CSV === ')
    clean = await clean_dataset(filepath)
    print(' === Cleaning the .CSV - Completed === ')
    print(' === Dataset is loaded in Mongo DB === ')
    print(clean)
    await add_dataset(clean)
    print(' === Dataset is loaded in Mongo DB - Completed ===')
    return {"Dataset is cleaned"}


@ router.get("/players/all", response_description="All players retreived from the database")
async def get_players_data():
    allplayers = await retrieve_allplayers()
    return ResponseModel(allplayers, "All players fetch successfully.")


@ router.get("/players/{id}", response_description="All players retreived from the database")
async def get_players_data(id):
    player = await retrieve_player(id)
    return ResponseModel(player, "Player fetch successfully.")
 # Machine Learning


@ router.get("/ml/{id}", response_description="All players retreived from the database")
async def get_players_data(id):
    player = await retrieve_player(id)
    return ResponseModel(player, "Player fetch successfully.")
