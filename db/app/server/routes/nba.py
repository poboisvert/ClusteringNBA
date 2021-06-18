from fastapi import APIRouter, Body
from fastapi.encoders import jsonable_encoder

from app.server.database import (
    add_dataset,
    retrieve_player,
    retrieve_players,
)
from app.server.models.nba import (
    ErrorResponseModel,
    ResponseModel,
    PlayerSchema,
)

router = APIRouter()


@router.get("/add_dataset", response_description="Dataset data added into the database")
async def add_stream_data():
    # stream = jsonable_encoder(stream)
    # Call add_dataset
    confirm = await add_dataset()
    return {"message": "Dataset is alive"}
