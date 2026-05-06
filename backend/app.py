from fastapi import FastAPI, Depends, HTTPException
from typing import Annotated
import pika
from pika.adapters.blocking_connection import BlockingChannel
from pika.exceptions import AMQPError
from schemas import MessageCreateRequest, MessageResponse

app = FastAPI()

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


@app.get("/")
def root():
    return "Hello World"


# 리턴 상태 코드를 바꾸고 싶으면 데코레이터에 씀. status_code=...
# 에러를 반환할 때는 return 보다 HTTPException을 많이 씀
@app.post("/messages", response_model=MessageResponse)
def publish_messages(message: MessageCreateRequest, channel: Annotated[BlockingChannel, Depends(get_channel)]):
    try:
        channel.queue_declare(queue="message_queue")

        channel.basic_publish(
            exchange="",
            routing_key="message_queue",
            body=message.message.encode("utf-8")
        )
    except AMQPError as e:
        raise HTTPException(
            status_code=503,
            detail="Message broker is unavailable"
        ) from e # 예외 체이닝. 사용자에게는 503을 응답하지만, 서버 로그나 traceback에는
                    # 원래 원인인 AMQPError도 같이 남음

    return MessageResponse(message=message.message)