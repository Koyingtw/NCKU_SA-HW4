import base64
import hashlib
import json
import os
import sys
from pathlib import Path
from typing import List

import schemas
from config import settings
from fastapi import Response, UploadFile, status
from loguru import logger


def byte_xor(ba1, ba2):
    return bytes([_a ^ _b for _a, _b in zip(ba1, ba2)])


class Storage:
    def __init__(self, is_test: bool):
        self.block_path: List[Path] = [
            Path("/var/raid") / f"{settings.FOLDER_PREFIX}-{i}-test"
            if is_test
            else Path(settings.UPLOAD_PATH) / f"{settings.FOLDER_PREFIX}-{i}"
            for i in range(settings.NUM_DISKS)
        ]
        self.__create_block()

    def __create_block(self):
        for path in self.block_path:
            logger.warning(f"Creating folder: {path}")
            path.mkdir(parents=True, exist_ok=True)

    async def file_integrity(self, filename: str) -> bool:
        """TODO: check if file integrity is valid
        file integrated must satisfy following conditions:
            1. all data blocks must exist
            2. size of all data blocks must be equal
            3. parity block must exist
            4. parity verify must success

        if one of the above conditions is not satisfied
        the file does not exist
        and the file is considered to be damaged
        so we need to delete the file
        """

        # 1. all data blocks must exist
        num_disks = settings.NUM_DISKS
        data_blocks = [f"/var/raid/block-{i}" for i in range(num_disks - 1)]
        if not all(os.path.exists(block) for block in data_blocks):
            return False

        # 2. size of all data blocks must be equal
        first_block_size = os.path.getsize(data_blocks[0])
        if not all(os.path.getsize(block) == first_block_size for block in data_blocks):
            return False

        # 3. parity block must exist
        parity_block = f"/var/raid/block-{num_disks - 1}"
        if not os.path.exists(parity_block):
            return False

        # parity verify must success

        return True

    async def create_file(self, file: UploadFile) -> schemas.File:
        content = await file.read()

        if len(content) > settings.MAX_SIZE:
            detail = {"detail": "File too large"}
            response = Response(
                content=json.dumps(detail),
                status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            )
            response.headers["Content-Type"] = "application/json"
            return response
        # TODO: create file with data block and parity block and return it's schema

        # create file with data block and parity block and return it's schema
        n = settings.NUM_DISKS
        chunk_size = len(content) // (n - 1)
        print(chunk_size)

        parts = []
        now = 0

        File_exist = False

        for i in range(len(content) % (n - 1)):
            part = content[now : now + chunk_size + 1]
            parts.append(part)
            part_file = f"/var/raid/block-{i}/{file.filename}"  # 部分檔案的檔名，例如 part1.bin、part2.bin、part3.bin 等

            # if os.path.exists(part_file):
            #     with open(part_file, "rb") as f:
            #         old_part = f.read()
            #         if (
            #             hashlib.md5(part).hexdigest()
            #             == hashlib.md5(old_part).hexdigest()
            #         ):
            #             File_exist = True

            with open(part_file, "wb") as f:
                f.write(part)
            now += chunk_size + 1

        for i in range(len(content) % (n - 1), n - 1):
            part = content[now : now + chunk_size] + b"\x00"
            parts.append(part)
            part_file = f"/var/raid/block-{i}/{file.filename}"  # 部分檔案的檔名，例如 part1.bin、part2.bin、part3.bin 等

            # if os.path.exists(part_file):
            #     with open(part_file, "rb") as f:
            #         old_part = f.read()
            #         if (
            #             hashlib.md5(part).hexdigest()
            #             == hashlib.md5(old_part).hexdigest()
            #         ):
            #             File_exist = True

            with open(part_file, "wb") as f:
                f.write(part)
            now += chunk_size

        parity_block = bytearray(parts[0])

        for part in parts[1:]:
            parity_block = bytes(_a ^ _b for _a, _b in zip(parity_block, part))

        parity_file = (
            f"/var/raid/block-{n - 1}/{file.filename}"  # 奇偶校驗檔案的檔名，例如 parity.bin
        )
        with open(parity_file, "wb") as f:
            f.write(parity_block)

        if File_exist:
            detail = {"detail": "File already exists"}
            response = Response(
                content=json.dumps(detail), status_code=status.HTTP_409_CONFLICT
            )
            response.headers["Content-Type"] = "application/json"
            return response

        return schemas.File(
            name=file.filename,
            size=len(content),
            checksum=hashlib.md5(content).hexdigest(),
            content=base64.b64encode(content),
            content_type=file.content_type,
        )

    async def retrieve_file(self, filename: str) -> bytes:
        # TODO: retrieve the binary data of file

        return b"".join("m3ow".encode() for _ in range(100))

    async def update_file(self, file: UploadFile) -> schemas.File:
        # TODO: update file's data block and parity block and return it's schema

        content = "何?!"
        return schemas.File(
            name="m3ow.txt",
            size=123,
            checksum=hashlib.md5(content.encode()).hexdigest(),
            content=base64.b64decode(content.encode()),
            content_type="text/plain",
        )

    async def delete_file(self, filename: str) -> None:
        # TODO: delete file's data block and parity block
        pass

    async def fix_block(self, block_id: int) -> None:
        # TODO: fix the broke block by using rest of block
        pass


storage: Storage = Storage(is_test="pytest" in sys.modules)
