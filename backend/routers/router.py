from fastapi import APIRouter, HTTPException, Depends
from schemas import MessageCreateRequest, MessageResponse
from typing import Annotated
from utility.util import get_channel
from uuid import uuid4
from datetime import datetime, timezone
from utility.util import mongo
from aio_pika.exceptions import AMQPError
from aio_pika.abc import AbstractRobustChannel
from utility.websocket_manager import manager
from fastapi import WebSocket, WebSocketDisconnect
import aio_pika
import json

router = APIRouter()


def make_room_id(user_a: str, user_b: str) -> str:
    return ":".join(sorted([user_a, user_b]))


async def save_message(client_message_id: str, sender_id: str, receiver_id: str, content: str) -> tuple[dict, bool]:
    document = {
        "_id": str(uuid4()),
        "client_message_id": client_message_id,
        "room_id": make_room_id(sender_id, receiver_id),
        "sender_id": sender_id,
        "receiver_id": receiver_id,
        "content": content,
        "delivery_status": "pending",
        "created_at": datetime.now(timezone.utc),
    }

    existing_message = await mongo.messages_collection.find_one({
        "client_message_id": client_message_id,
        "sender_id": sender_id
    })

    if existing_message:
        return existing_message, False

    await mongo.messages_collection.insert_one(document)

    return document, True


@router.post("/messages", response_model=MessageResponse)
async def publish_messages(message: MessageCreateRequest, channel: Annotated[AbstractRobustChannel, Depends(get_channel)]
):
    # DB 저장
    saved_message, created = await save_message(
        client_message_id=message.client_message_id,
        sender_id=message.sender_id,
        receiver_id=message.receiver_id,
        content=message.message
    )

    if not created:
        return MessageResponse(
            message_id=saved_message["_id"],
            client_message_id=saved_message["client_message_id"],
            room_id=saved_message["room_id"],
            sender_id=saved_message["sender_id"],
            receiver_id=saved_message["receiver_id"],
            message=saved_message["content"],
            delivered_realtime=False,
            delivery_status=saved_message["delivery_status"],
            created_at=saved_message["created_at"],
    )

    message_id=saved_message["_id"]
    room_id= saved_message["room_id"]
    sender_id=saved_message["sender_id"]
    receiver_id=saved_message["receiver_id"]
    msg=saved_message["content"]
    created_at=saved_message["created_at"]

    event = {
        "message_id": message_id,
        "client_message_id": message.client_message_id,
        "room_id": room_id,
        "sender_id": sender_id,
        "receiver_id": receiver_id,
        "content": msg,
        "created_at": created_at.isoformat(),
    }

    delivery_status = "pending"

    # 온라인이면 WebSocket으로 전송
    delivered = await manager.send_to_user(
        message.receiver_id,
        {
            "type": "message.created",
            "data": event
        }
    )

    if delivered:
        delivery_status = "delivered_realtime"
    # 오프라인(전송되지 않음)이면 큐에 발행
    else:
        try:
            await channel.declare_queue("message_queue", durable=True)

            await channel.default_exchange.publish(
                aio_pika.Message(
                    body=json.dumps(event).encode("utf-8"),
                    delivery_mode=aio_pika.DeliveryMode.PERSISTENT
                ),
                routing_key="message_queue",
            )

            delivery_status = "queued"
        except AMQPError:
            delivery_status = "notification_failed"

    await mongo.messages_collection.update_one(
        {"_id": message_id},
        {"$set": {"delivery_status": delivery_status}}

    )

    return MessageResponse(
        message_id=message_id,
        client_message_id=message.client_message_id,
        room_id= room_id,
        sender_id=sender_id,
        receiver_id=receiver_id,
        message=msg,
        delivered_realtime=delivered,
        delivery_status=delivery_status,
        created_at=created_at
    )


@router.get("/rooms/{room_id}/messages")
async def read_messages(room_id: str):
    # cursor 객체를 반환하기 때문에 await 안씀
    cursor = mongo.messages_collection.find(
        {
            "room_id": room_id
        }
    ).sort("created_at", 1) # 1은 오름차순

    messages = await cursor.to_list(length=1000)

    return messages



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
