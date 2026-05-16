from fastapi import HTTPException
from pymongo import AsyncMongoClient
from aio_pika.exceptions import CONNECTION_EXCEPTIONS
from dotenv import load_dotenv
import aio_pika
import os

load_dotenv()


class MongoConnection:
    def __init__(self):
        mongo_id = os.getenv("MONGODB_INITDB_ROOT_USERNAME")
        mongo_password = os.getenv("MONGODB_INITDB_ROOT_PASSWORD")    

        self.client = AsyncMongoClient(f"mongodb://{mongo_id}:{mongo_password}@localhost:27017/?authSource=admin")
        self.db = self.client["chat_app"]
        self.messages_collection = self.db["messages"]


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


mongo = MongoConnection()