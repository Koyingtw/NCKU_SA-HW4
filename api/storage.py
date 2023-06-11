import asyncio
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


async def write_part_file(part_file, part):
    with open(part_file, "wb") as f:
        f.write(part)
        f.close()


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

    async def file_exist(self, filename: str) -> bool:
        # 1. all data blocks must exist
        num_disks = settings.NUM_DISKS

        for i in range(num_disks):
            if not os.path.exists(f"/var/raid/block-{i}/{filename}"):
                print(f"/var/raid/block-{i}/{filename} Not exist")
                await self.delete_file(filename)
                return False

        return True

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

        num_disks = settings.NUM_DISKS
        data_blocks = [f"/var/raid/block-{i}/{filename}" for i in range(num_disks - 1)]
        parity_block = f"/var/raid/block-{num_disks - 1}/{filename}"

        if not await self.file_exist(filename):
            await self.delete_file(filename)
            return False

        # 2. size of all data blocks must be equal
        first_block_size = os.path.getsize(data_blocks[0])
        if not all(os.path.getsize(block) == first_block_size for block in data_blocks):
            await self.delete_file(filename)
            return False

        # parity verify must success
        xor_result = bytearray()
        for i in range(num_disks - 1):
            if i == 0:
                with open(f"{data_blocks[i]}", "rb") as f:
                    xor_result = bytearray(f.read())
                    f.close()
            else:
                with open(data_blocks[i], "rb") as f:
                    xor_result = byte_xor(xor_result, bytearray(f.read()))
                    f.close()
        with open(parity_block, "rb") as f:
            if xor_result != bytearray(f.read()):
                await self.delete_file(filename)
                return False
            f.close()
        return True

    async def create_file(self, file: UploadFile) -> schemas.File:
        content = await file.read()
        # TODO: create file with data block and parity block and return it's schema

        # create file with data block and parity block and return it's schema

        length = len(content)

        if length > settings.MAX_SIZE:
            detail = {"detail": "File too large"}
            response = Response(
                content=json.dumps(detail),
                status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
                headers={"Content-Type": "application/json"},
            )
            return response

        n = settings.NUM_DISKS

        chunk_size = length // (n - 1)
        print(chunk_size)

        parts = []
        now = 0

        for i in range(length % (n - 1)):
            part = content[now : now + chunk_size + 1]
            parts.append(part)

            now += chunk_size + 1

        for i in range(length % (n - 1), n - 1):
            part = content[now : now + chunk_size] + b"\x00"
            parts.append(part)

            now += chunk_size

        parity_block = bytearray(parts[0])

        # 寫入所有部分檔案
        tasks = [
            write_part_file(f"/var/raid/block-{i}/{file.filename}", part)
            for i, part in enumerate(parts)
        ]
        await asyncio.gather(*tasks)

        for part in parts[1:]:
            parity_block = bytes(_a ^ _b for _a, _b in zip(parity_block, part))

        parity_file = (
            f"/var/raid/block-{n - 1}/{file.filename}"  # 奇偶校驗檔案的檔名，例如 parity.bin
        )

        with open(parity_file, "wb") as f:
            f.write(parity_block)
            f.close()

        # await asyncio.sleep(0.5)
        while True:
            with open(parity_file, "rb") as f:
                parity = bytearray(f.read())
                f.close()
                if parity == parity_block:
                    schema = {
                        "name": file.filename,
                        "size": length,
                        "checksum": hashlib.md5(content).hexdigest(),
                        "content": base64.b64encode(content).decode("utf-8"),
                        "content_type": file.content_type,
                    }

                    response = Response(
                        content=json.dumps(schema),
                        status_code=status.HTTP_201_CREATED,
                        headers={"Content-Type": "application/json"},
                    )

                    return response

    async def retrieve_file(self, filename: str) -> bytes:
        # TODO: retrieve the binary data of file
        file_data = b""

        folder_names = os.listdir("/var/raid/")
        folder_names.sort()  # 確保按照順序讀取檔案

        if not await self.file_integrity(filename):
            return b""

        for i in range(len(folder_names) - 1):
            file_path = f"/var/raid/block-{i}/{filename}"

            if os.path.exists(file_path):
                with open(file_path, "rb") as f:
                    file_content = f.read()
                    f.close()

                # 移除尾部的填充 0x00
                file_content = file_content.rstrip(b"\x00")

                # 連接二進位數據
                file_data += file_content

        return file_data

    async def update_file(self, file: UploadFile) -> schemas.File:
        # TODO: update file's data block and parity block and return it's schema
        content = await file.read()
        # TODO: create file with data block and parity block and return it's schema

        # create file with data block and parity block and return it's schema

        length = len(content)

        if length > settings.MAX_SIZE:
            detail = {"detail": "File too large"}
            response = Response(
                content=json.dumps(detail),
                status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
                headers={"Content-Type": "application/json"},
            )
            return response

        n = settings.NUM_DISKS

        chunk_size = length // (n - 1)
        print(chunk_size)

        parts = []
        now = 0

        File_exist = False

        for i in range(length % (n - 1)):
            part = content[now : now + chunk_size + 1]
            parts.append(part)

            now += chunk_size + 1

        for i in range(length % (n - 1), n - 1):
            part = content[now : now + chunk_size] + b"\x00"
            parts.append(part)

            now += chunk_size

        parity_block = bytearray(parts[0])

        # 寫入所有部分檔案
        tasks = [
            write_part_file(f"/var/raid/block-{i}/{file.filename}", part)
            for i, part in enumerate(parts)
        ]
        await asyncio.gather(*tasks)

        for part in parts[1:]:
            parity_block = bytes(_a ^ _b for _a, _b in zip(parity_block, part))

        parity_file = (
            f"/var/raid/block-{n - 1}/{file.filename}"  # 奇偶校驗檔案的檔名，例如 parity.bin
        )

        with open(parity_file, "wb") as f:
            f.write(parity_block)
            f.close()

        if File_exist:
            detail = {"detail": "File already exists"}
            response = Response(
                content=json.dumps(detail), status_code=status.HTTP_409_CONFLICT
            )
            response.headers["Content-Type"] = "application/json"
            return response

        # await asyncio.sleep(1)
        while True:
            with open(parity_file, "rb") as f:
                parity = bytearray(f.read())
                f.close()
                if parity == parity_block:
                    schema = {
                        "name": file.filename,
                        "size": length,
                        "checksum": hashlib.md5(content).hexdigest(),
                        "content": base64.b64encode(content).decode("utf-8"),
                        "content_type": file.content_type,
                    }

                    response = Response(
                        content=json.dumps(schema),
                        status_code=status.HTTP_200_OK,
                        headers={"Content-Type": "application/json"},
                    )

                    return response

    async def delete_file(self, filename: str) -> None:
        # TODO: delete file's data block and parity block

        for i in range(settings.NUM_DISKS):
            file_path = f"/var/raid/block-{i}/{filename}"
            if os.path.exists(file_path):
                os.remove(file_path)

        pass

    async def fix_block(self, block_id: int) -> None:
        # TODO: fix the broke block by using rest of block

        # list all filename in folder
        folder_names = os.listdir(f"/var/raid/block-{settings.NUM_DISKS - 1}/")
        folder_names.sort()  # 確保按照順序讀取檔案

        for filename in folder_names:
            with open(
                f"/var/raid/block-{settings.NUM_DISKS - 1}/{filename}", "rb"
            ) as f:
                xor_result = bytearray(f.read())
            for i in range(settings.NUM_DISKS - 1):
                if i == block_id:
                    continue
                with open(f"/var/raid/block-{i}/{filename}", "rb") as f:
                    xor_result = byte_xor(xor_result, bytearray(f.read()))

            with open(f"/var/raid/block-{block_id}/{filename}", "wb") as f:
                f.write(xor_result)
                f.close()
            pass


storage: Storage = Storage(is_test="pytest" in sys.modules)
