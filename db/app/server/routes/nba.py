from fastapi import APIRouter, Body
from fastapi.encoders import jsonable_encoder

from app.server.database import (
    add_dataset,
    retrieve_player,
    retrieve_allplayers,
)
from app.server.models.nba import (
    ErrorResponseModel,
    ResponseModel,
    PlayerSchema,
)

router = APIRouter()


@router.post("/dataset", response_description="Dataset data added into the database")
async def dataset(filename='datasets/finalized_data.csv'):

    # stream = jsonable_encoder(stream)
    # Call add_dataset
    confirm = await add_dataset(filename)
    return {confirm, "Dataset is alive"}


@ router.get("/allplayers", response_description="All players retreived from the database")
async def get_players_data():
    allplayers = await retrieve_allplayers()
    return ResponseModel(allplayers, "All players fetch successfully.")


@ router.get("/player/{id}", response_description="All players retreived from the database")
async def get_players_data(id):
    player = await retrieve_player(id)
    return ResponseModel(player, "Player fetch successfully.")
