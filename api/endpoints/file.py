import json

import schemas
from config import settings
from fastapi import APIRouter, Response, UploadFile, status
from storage import storage

router = APIRouter()

POST_FILE = {
    201: {
        "description": "Successful Response",
        "content": {
            "application/json": {
                "schema": {
                    "type": "object",
                    "properties": {
                        "name": {"type": "string"},
                        "size": {"type": "integer"},
                        "checksum": {"type": "string"},
                        "content": {"type": "bytes"},
                        "content_type": {"type": "string"},
                    },
                }
            }
        },
    }
}


@router.post(
    "/",
    status_code=status.HTTP_201_CREATED,
    response_model=schemas.File,
    responses=POST_FILE,
    name="file:create_file",
)
async def create_file(file: UploadFile):
    return await storage.create_file(file)

    content = await file.read()
    if len(content) > settings.MAX_SIZE:
        detail = {"detail": "File too large"}
        response = Response(
            content=json.dumps(detail),
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
        )
        response.headers["Content-Type"] = "application/json"
        return response

    return await storage.create_file(file)
    try:
        ret = await storage.create_file(file)
        return ret
    except Exception as e:
        if str(e) == "File already exists":
            print(e)
            detail = {"detail": "File already exists"}
            response = Response(
                content=json.dumps(detail), status_code=status.HTTP_409_CONFLICT
            )
            response.headers["Content-Type"] = "application/json"
            return response
        else:
            print(e)
            detail = {"detail": "Validation Error"}
            response = Response(
                content=json.dumps(detail),
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )
            response.headers["Content-Type"] = "application/json"
            return response


@router.get("/", status_code=status.HTTP_200_OK, name="file:retrieve_file")
async def retrieve_file(filename: str) -> Response:
    # TODO: Add headers to ensure the filename is displayed correctly
    #       You should also ensure that enables the judge to download files directly
    return Response(
        await storage.retrieve_file(filename),
        media_type="application/octet-stream",
        headers={},
    )


@router.put("/", status_code=status.HTTP_200_OK, name="file:update_file")
async def update_file(file: UploadFile) -> schemas.File:
    return await storage.update_file(file)


@router.delete("/", status_code=status.HTTP_200_OK, name="file:delete_file")
async def delete_file(filename: str) -> str:
    await storage.delete_file(filename)
    return schemas.Msg(detail="File deleted")
