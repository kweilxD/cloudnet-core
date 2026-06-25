import asyncio
import json
import websockets

# Хранилище для активных сетевых соединений
connected_sockets = {"agent": None, "client": None}

async def handler(websocket):
    global connected_sockets
    print("[СЕРВЕР] Зафиксировано новое сетевое подключение...")
    
    try:
        async for message in websocket:
            data = json.loads(message)
            sender = data.get("sender")
            msg_type = data.get("type")

            # Регистрируем участников в зависимости от того, кто прислал пакет
            if sender == "agent":
                connected_sockets["agent"] = websocket
            elif sender == "client":
                connected_sockets["client"] = websocket

            print(f"[СЕРВЕР] Получен пакет '{msg_type}' от [{sender}]")

            # Пересылаем сообщение второму участнику (маршрутизация)
            if sender == "client" and connected_sockets["agent"]:
                await connected_sockets["agent"].send(message)
            elif sender == "agent" and connected_sockets["client"]:
                await connected_sockets["client"].send(message)

    except websockets.ConnectionClosed:
        pass
    finally:
        # Очищаем ссылки, если кто-то отключился
        if websocket == connected_sockets["agent"]:
            connected_sockets["agent"] = None
            print("[СЕРВЕР] Агент (Игровой ПК) отключился.")
        elif websocket == connected_sockets["client"]:
            connected_sockets["client"] = None
            print("[СЕРВЕР] Клиент (Браузер) отключился.")

async def main():
    print("====================================================")
    print("Сигнальный сервер CloudNET запущен на порту 8080...")
    print("Ожидание подключений от сайта и агента...")
    print("====================================================")
    async with websockets.serve(handler, "localhost", 8080):
        await asyncio.Future() # Бесконечный цикл работы сервера

if __name__ == "__main__":
    asyncio.run(main())