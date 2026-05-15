from pydantic import BaseModel

class MessageCreateRequest(BaseModel):
    sender_id: str
    receiver_id: str
    message: str

class MessageResponse(BaseModel):
    message: str