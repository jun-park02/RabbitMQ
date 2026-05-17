from pydantic import BaseModel
from uuid import UUID
from datetime import datetime

class MessageCreateRequest(BaseModel):
    sender_id: str
    receiver_id: str
    message: str

class MessageResponse(BaseModel):
    message_id: UUID
    room_id: str
    sender_id: str
    receiver_id: str
    message: str
    delivered_realtime: bool
    created_at: datetime