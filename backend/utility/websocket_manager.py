from fastapi import WebSocket

class ConnectionManager:
    def __init__(self):
        # user_id -> WebSocket 목록
        # 한 사용자가 여러 기기에서 접속할 수 있으니 list로 관리
        self.active_connections: dict[str, list[WebSocket]] = {}

    async def connect(self, user_id: str, websocket: WebSocket):
        await websocket.accept() # 클라이언트의 접속 요청 허용

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
        
        # ?
        disconnected = []

        for websocket in connections:
            try:
                await websocket.send_json(payload)
            except Exception:
                disconnected.append(websocket)

        
        for websocket in disconnected:
            self.disconnect(user_id, websocket)

        
        return len(disconnected) < len(connections)
    
# 이건 왜 이렇게 만드는거지
manager = ConnectionManager()



