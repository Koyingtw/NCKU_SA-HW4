from typing import Any

import schemas
from fastapi import APIRouter, status

router = APIRouter()

GET_HEALTH = {
    200: {
        "description": "API response successfully",
        "content": {
            "application/json": {
                "schema": {
                    "type": "object",
                    "properties": {"detail": {"type": "string"}},
                }
            }
        },
    }
}


@router.get(
    "/",
    status_code=status.HTTP_200_OK,
    responses=GET_HEALTH,
    response_model=schemas.Msg,
    name="health:get_health",
)
@router.get(
    "",
    status_code=status.HTTP_200_OK,
    responses=GET_HEALTH,
    response_model=schemas.Msg,
    name="health:get_health",
)
def get_health() -> Any:
    return schemas.Msg(detail="Service healthy test2")
