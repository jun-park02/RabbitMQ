from fastapi import WebSocket

class ConnectionManager:
    def __init__(self):
        # key: user_id
        # value: WebSocket list
        self.active_connections: dict[str, list[WebSocket]] = {}

    async def connect(self, user_id: str, websocket: WebSocket):
        # 클라이언트의 접속 요청 허용
        await websocket.accept() 

        if user_id not in self.active_connections:
            self.active_connections[user_id] = []

        self.active_connections[user_id].append(websocket)

    def disconnect(self, user_id:str, websocket: WebSocket):
        connections = self.active_connections.get(user_id)

        if not connections:
            return 
        
        if websocket in connections:
            connections.remove(websocket)

        if len(connections) == 0:
            del self.active_connections[user_id]
    
    def is_online(self, user_id: str) -> bool:
        return user_id in self.active_connections
    
    async def send_to_user(self, user_id: str, payload: dict) -> bool:
        connections = self.active_connections.get(user_id)

        if not connections:
            return False
        
        disconnected = []
        sent_count = 0

        for websocket in connections:
            try:
                await websocket.send_json(payload)
                sent_count += 1
            except Exception:
                disconnected.append(websocket)

        for websocket in disconnected:
            self.disconnect(user_id, websocket)
        
        return sent_count > 0
    
manager = ConnectionManager()



