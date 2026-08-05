from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

import database
import history

router = APIRouter()


class RetentionUpdate(BaseModel):
    entries: int


@router.get("/settings/retention")
async def get_retention():
    return {"entries": history.get_retention()}


@router.put("/settings/retention")
async def set_retention(body: RetentionUpdate):
    try:
        history.set_retention(body.entries)
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))
    # Immediately trim if the new window is smaller than the current row count
    database.trim_snapshots(history.get_retention())
    return {"entries": history.get_retention()}
