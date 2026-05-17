from pydantic import BaseModel
from uuid import UUID
from datetime import datetime

class MessageCreateRequest(BaseModel):
    client_message_id: str # 중복방지를 위한 클라이언트 메시지 아이디 
    sender_id: str
    receiver_id: str
    message: str

class MessageResponse(BaseModel):
    message_id: UUID
    client_message_id: str
    room_id: str
    sender_id: str
    receiver_id: str
    message: str
    delivered_realtime: bool
    delivery_status: str
    created_at: datetime