import asyncio
import json
import websockets
import os

connected_sockets = {"agent": None, "client": None}

async def handler(websocket):
    global connected_sockets
    print("[СЕРВЕР] Зафиксировано новое сетевое подключение...")
    
    try:
        async for message in websocket:
            data = json.loads(message)
            sender = data.get("sender")
            msg_type = data.get("type")

            if sender == "agent":
                connected_sockets["agent"] = websocket
            elif sender == "client":
                connected_sockets["client"] = websocket

            print(f"[СЕРВЕР] Получен пакет '{msg_type}' от [{sender}]")

            if sender == "client" and connected_sockets["agent"]:
                await connected_sockets["agent"].send(message)
            elif sender == "agent" and connected_sockets["client"]:
                await connected_sockets["client"].send(message)

    except websockets.ConnectionClosed:
        pass
    finally:
        if websocket == connected_sockets["agent"]:
            connected_sockets["agent"] = None
            print("[СЕРВЕР] Агент (Игровой ПК) отключился.")
        elif websocket == connected_sockets["client"]:
            connected_sockets["client"] = None
            print("[СЕРВЕР] Клиент (Браузер) отключился.")

async def main():
    # Render сам назначит порт через переменную окружения PORT
    port = int(os.environ.get("PORT", 8080))
    
    print("====================================================")
    print(f"Сигнальный сервер CloudNET запущен на порту {port}...")
    print("====================================================")
    
    # Слушаем 0.0.0.0, чтобы сервер принимал внешние запросы из интернета
    async with websockets.serve(handler, "0.0.0.0", port):
        await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())
