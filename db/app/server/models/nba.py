from pydantic import BaseModel, Field


class PlayerSchema(BaseModel):
    Year: str = Field(...)
    Player: str = Field(...)
    Age: str = Field(...)
    G: str = Field(...)
    MP: str = Field(...)
    PER: str = Field(...)
    TS: str = Field(...)
    treePAr: str = Field(...)
    FTr: str = Field(...)
    ORB_rel: str = Field(...)
    DRB_rel: str = Field(...)
    TRB_rel: str = Field(...)
    AST_rel: str = Field(...)
    STL_rel: str = Field(...)
    BLK_rel: str = Field(...)
    TOV_rel: str = Field(...)
    USG_rel: str = Field(...)
    OWS: str = Field(...)
    DWS: str = Field(...)
    WS: str = Field(...)
    WSon48: str = Field(...)
    BPM: str = Field(...)
    DBPM: str = Field(...)
    BPM: str = Field(...)
    VORP: str = Field(...)
    FG: str = Field(...)
    FGA: str = Field(...)
    treeP: str = Field(...)
    treePA: str = Field(...)
    twoP: str = Field(...)
    twoPA: str = Field(...)
    eFG_rel: str = Field(...)
    FT: str = Field(...)
    TA: str = Field(...)
    ORB: str = Field(...)
    DRB: str = Field(...)
    TRB: str = Field(...)
    AST: str = Field(...)
    STL: str = Field(...)
    BLK: str = Field(...)
    TOV: str = Field(...)
    PF: str = Field(...)
    PTS: str = Field(...)
    year_start: str = Field(...)
    weight: str = Field(...)
    year_born: str = Field(...)
    Status: str = Field(...)
    height: str = Field(...)

    class Config:
        schema_extra = {
            "example": {
                "Year": "John Doe",
                "Player": "jdoe@x.edu.ng",
                "Age": 22,
            }
        }

# Reply from the server


def ResponseModel(data, message):
    return {
        "data": [data],
        "code": 200,
        "message": message,
    }

# How the error is return in JSON format


def ErrorResponseModel(error, code, message):
    return {"error": error, "code": code, "message": message}
