import json

import schemas
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
    },
    409: {
        "description": "File already exists",
        "content": {
            "application/json": {
                "schema": {
                    "type": "object",
                    "properties": {"detail": {"type": "string"}},
                }
            }
        },
    },
    413: {
        "description": "File too large",
        "content": {
            "application/json": {
                "schema": {
                    "type": "object",
                    "properties": {"detail": {"type": "string"}},
                }
            }
        },
    },
}


@router.post(
    "/",
    response_model=schemas.File,
    responses=POST_FILE,
    name="file:create_file",
)
async def create_file(file: UploadFile):
    if await storage.file_exist(file.filename) and await storage.file_integrity(
        file.filename
    ):
        detail = {"detail": "File already exists"}
        response = Response(
            content=json.dumps(detail),
            status_code=status.HTTP_409_CONFLICT,
        )
        response.headers["Content-Type"] = "application/json"
        return response
    return await storage.create_file(file)


@router.get("/", status_code=status.HTTP_200_OK, name="file:retrieve_file")
async def retrieve_file(filename: str) -> Response:
    # TODO: Add headers to ensure the filename is displayed correctly
    #       You should also ensure that enables the judge to download files directly

    if not await storage.file_exist(filename) or not await storage.file_integrity(
        filename
    ):
        detail = {"detail": "File not found"}
        response = Response(
            content=json.dumps(detail),
            status_code=status.HTTP_404_NOT_FOUND,
        )
        response.headers["Content-Type"] = "application/json"
        return response
    else:
        file_data = await storage.retrieve_file(filename)
        return Response(
            file_data,
            media_type="application/octet-stream",
            headers={
                "Content-Disposition": f"attachment; filename={filename}",
                "Content-Length": str(len(file_data)),
            },
        )

    file_data = await storage.retrieve_file(filename)

    if len(file_data) == 0:
        detail = {"detail": "File not found"}
        response = Response(
            content=json.dumps(detail),
            status_code=status.HTTP_404_NOT_FOUND,
        )
        response.headers["Content-Type"] = "application/json"
        return response
    else:
        return Response(
            file_data,
            media_type="application/octet-stream",
            headers={
                "Content-Disposition": f"attachment; filename={filename}",
                "Content-Length": str(len(file_data)),
            },
        )


@router.put("/", status_code=status.HTTP_200_OK, name="file:update_file")
async def update_file(file: UploadFile) -> schemas.File:
    if not await storage.file_exist(file.filename):
        detail = {"detail": "File not found"}
        response = Response(
            content=json.dumps(detail),
            status_code=status.HTTP_404_NOT_FOUND,
        )
        response.headers["Content-Type"] = "application/json"
        return response
    return await storage.update_file(file)


@router.delete("/", status_code=status.HTTP_200_OK, name="file:delete_file")
async def delete_file(filename: str) -> str:
    if not await storage.file_exist(filename) or not await storage.file_integrity(
        filename
    ):
        detail = {"detail": "File not found"}
        response = Response(
            content=json.dumps(detail),
            status_code=status.HTTP_404_NOT_FOUND,
        )
        response.headers["Content-Type"] = "application/json"
        return response
    await storage.delete_file(filename)
    return schemas.Msg(detail="File deleted")
