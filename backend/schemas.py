from pydantic import BaseModel

class MessageCreateRequest(BaseModel):
    message: str

class MessageResponse(BaseModel):
    message: str