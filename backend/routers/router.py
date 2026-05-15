from fastapi import APIRouter, HTTPException, Depends
from schemas import MessageCreateRequest, MessageResponse
from typing import Annotated
from pika.adapters.blocking_connection import BlockingChannel
from pika.exceptions import AMQPError
from utility.util import get_channel
from uuid import uuid4, UUID
from datetime import datetime, timezone
from utility.util import messages_collection

router = APIRouter()

async def save_message(sender_id: str, receiver_id: str, content: str) -> dict:
    document = {
        "_id": str(uuid4()), # _id는 각 document의 기본 고유 식별자 필드
        "sender_id": sender_id,
        "receiver_id": receiver_id,
        "content": content,
        "created_at": datetime.now(timezone.utc),
    }

    await messages_collection.insert_one(document)

    return document


# 리턴 상태 코드를 바꾸고 싶으면 데코레이터에 씀. status_code=...
# 에러를 반환할 때는 return 보다 HTTPException을 많이 씀
@router.post("/messages", response_model=MessageResponse)
async def publish_messages(message: MessageCreateRequest, channel: Annotated[BlockingChannel, Depends(get_channel)]):
    saved_message = await save_message(
        sender_id=message.sender_id
    )


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

    try:
        pass
        # await db.users.insert_one({"s": "kim"})
        # doc = await db.users.find_one({"name": "kim"})
    except Exception as e:
        pass
    return MessageResponse(message=message.message)