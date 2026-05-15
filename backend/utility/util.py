from fastapi import HTTPException
from pymongo import AsyncMongoClient
from pika.exceptions import AMQPError
import pika

# 참고
# chat_app이 DB이름
# messages가 collection 이름
# document -> JSON 같은 데이터 1개
client = AsyncMongoClient("mongodb://localhost:27017")
db = client["chat_app"]
messages_collection = db["messages"]

def get_channel():
    try:
        connection = None
        channel = None

        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host="localhost")
        )

        channel = connection.channel()

        if channel.is_closed:
            raise HTTPException(
                status_code=503,
                detail="Message broker channel is closed"
            )
        yield channel
    except AMQPError as e:
         raise HTTPException(
              status_code=503,
              detail="Failed to connect to message broker"
         ) from e
    finally:
        if channel is not None and channel.is_open:
            channel.close()
        if connection is not None and connection.is_open:
            connection.close()
