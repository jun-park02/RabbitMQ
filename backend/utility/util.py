from fastapi import HTTPException
from pymongo import AsyncMongoClient
from aio_pika.exceptions import CONNECTION_EXCEPTIONS
from dotenv import load_dotenv
import aio_pika
import os

load_dotenv()

MONGO_ID = os.getenv("MONGODB_INITDB_ROOT_USERNAME")
MONGO_PASSWORD = os.getenv("MONGODB_INITDB_ROOT_PASSWORD")

# 참고
# chat_app이 DB이름
# messages가 collection 이름
# document -> JSON 같은 데이터 1개
client = AsyncMongoClient(f"mongodb://{MONGO_ID}:{MONGO_PASSWORD}@localhost:27017/?authSource=admin")
db = client["chat_app"]
messages_collection = db["messages"]

async def get_channel():
    connection = None
    channel = None
    try:
        RABBITMQ_USER = os.getenv("RABBITMQ_DEFAULT_USER")
        RABBITMQ_PASSWORD = os.getenv("RABBITMQ_DEFAULT_PASS")

        connection = await aio_pika.connect_robust(
            host="localhost",
            port=5672,
            login=RABBITMQ_USER,
            password=RABBITMQ_PASSWORD
        )

        channel = await connection.channel()

        if channel.is_closed:
            raise HTTPException(
                status_code=503,
                detail="Message broker channel is closed"
            )
        yield channel
    except CONNECTION_EXCEPTIONS as e:
        raise HTTPException(
            status_code=503,
            detail="Failed to connect to message broker"
        ) from e
    finally:
        if channel is not None and not channel.is_closed:
            await channel.close()
        if connection is not None and not connection.is_closed:
            await connection.close()
