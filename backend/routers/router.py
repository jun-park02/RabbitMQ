from fastapi import APIRouter, HTTPException, Depends
from schemas import MessageCreateRequest, MessageResponse
from typing import Annotated
from utility.util import get_channel
from uuid import uuid4, UUID
from datetime import datetime, timezone
from utility.util import messages_collection
import aio_pika
from aio_pika.exceptions import AMQPError
from aio_pika.abc import AbstractRobustChannel
from utility.websocket_manager import manager
from fastapi import WebSocket, WebSocketDisconnect

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
async def publish_messages(message: MessageCreateRequest, channel: Annotated[AbstractRobustChannel, Depends(get_channel)]
):
    saved_message = await save_message(
        sender_id=message.sender_id,
        receiver_id=message.receiver_id,
        content=message.message
    )

    event = {
        "sender_id": message.sender_id,
        "receiver_id": message.receiver_id,
        "content": message.message
    }

    delivered = await manager.send_to_user(
        message.receiver_id,
        {
            "type": "message.created",
            "data": event
        }
    )

    if not delivered:
        try:
            await channel.declare_queue("message_queue", durable=True)

            await channel.default_exchange.publish(
                aio_pika.Message(body=message.message.encode("utf-8")),
                routing_key="message_queue",
            )
        except AMQPError as e:
            raise HTTPException(
                status_code=503,
                detail="Message broker is unavailable222"
            ) from e # 예외 체이닝. 사용자에게는 503을 응답하지만, 서버 로그나 traceback에는
                        # 원래 원인인 AMQPError도 같이 남음

    return MessageResponse(
        message_id=saved_message["_id"],
        sender_id=saved_message["sender_id"],
        receiver_id=saved_message["receiver_id"],
        message=saved_message["content"],
        delivered_realtime=delivered,
        created_at=saved_message["created_at"]
    )

@router.websocket("/ws/{user_id}")
async def websocket_endpoint(websocket: WebSocket, user_id: str):
    await manager.connect(user_id, websocket) # websocket은 fastapi가 자동으로 만들어줌

    try:
        while True:
            # 클라이언트 연결을 유지하기 위해 메시지를 기다림
            # 클라이언트가 ping이나 아무 메시지나 보내도 됨
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(user_id, websocket)
