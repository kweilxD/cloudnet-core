import asyncio
import json
import websockets
import os

agents_pool = {}
clients_pool = {}

async def broadcast_machines_list():
    machines = [{"id": info["id"], "username": info["username"]} for info in agents_pool.values()]
    payload = json.dumps({"type": "machines_list", "list": machines})
    if clients_pool:
        await asyncio.gather(*[client.send(payload) for client in clients_pool.keys()], return_exceptions=True)

async def handler(websocket):
    global agents_pool, clients_pool
    client_id = f"cli_{str(id(websocket))}"
    agent_id = f"age_{str(id(websocket))}"
    
    try:
        async for message in websocket:
            data = json.loads(message)
            sender = data.get("sender")
            msg_type = data.get("type")
            
            if msg_type == "ping":
                await websocket.send(json.dumps({"type": "pong"}))
                continue

            if sender == "agent":
                if msg_type == "handshake":
                    agents_pool[websocket] = {"id": agent_id, "username": data.get("username", "Unknown Win32 Host")}
                    print(f"[NODE CONNECTED] -> {data.get('username')} ({agent_id})")
                    await broadcast_machines_list()
                elif msg_type == "answer":
                    target_client_id = data.get("targetClientId")
                    for c_ws, c_info in clients_pool.items():
                        if c_info["id"] == target_client_id:
                            await c_ws.send(json.dumps({"type": "answer", "sdp": data.get("sdp")}))
                            break
            elif sender == "client":
                if websocket not in clients_pool:
                    clients_pool[websocket] = {"id": client_id}
                if msg_type == "get_machines":
                    machines = [{"id": info["id"], "username": info["username"]} for info in agents_pool.values()]
                    await websocket.send(json.dumps({"type": "machines_list", "list": machines}))
                elif msg_type == "offer":
                    target_agent_id = data.get("targetId")
                    for a_ws, a_info in agents_pool.items():
                        if a_info["id"] == target_agent_id:
                            await a_ws.send(json.dumps({"type": "offer", "sdp": data.get("sdp"), "client_id": client_id}))
                            break
    except websockets.ConnectionClosed:
        pass
    finally:
        if websocket in agents_pool: agents_pool.pop(websocket, None)
        if websocket in clients_pool: clients_pool.pop(websocket, None)
        await broadcast_machines_list()

async def main():
    # Railway автоматически передает порт в переменную окружения PORT
    port = int(os.environ.get("PORT", 8080))
    print(f"[START] Signaling server started on port {port}")
    async with websockets.serve(handler, "0.0.0.0", port, ping_interval=20, ping_timeout=20):
        await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())
