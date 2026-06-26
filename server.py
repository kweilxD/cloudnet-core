import asyncio
import json
import websockets
import threading
import customtkinter as ctk

# Хранилища пулов
agents_pool = {}  # socket_obj: {"id": str, "username": str}
clients_pool = {} # socket_obj: {"id": str}

class ModernServerApp(ctk.CTk):
    def __init__(self):
        super().__init__()
        self.title("CloudNET Signaling Node")
        self.geometry("850x580")
        
        ctk.set_appearance_mode("dark")
        self.configure(fg_color="#010409")

        self.header = ctk.CTkLabel(
            self, 
            text="⚡ CLOUDNET PIPELINE SYSTEM : CENTRAL SIGNAL ROUTER", 
            font=ctk.CTkFont(family="Consolas", size=16, weight="bold"), 
            text_color="#58a6ff"
        )
        self.header.pack(padx=30, pady=25, anchor="w")

        self.status_frame = ctk.CTkFrame(self, fg_color="#0d1117", border_color="#30363d", border_width=2, corner_radius=12)
        self.status_frame.pack(padx=30, pady=5, fill="x")

        self.info_status = ctk.CTkLabel(
            self.status_frame, 
            text="● ACTIVE AGENTS: 0  |  ● WEB CLIENTS OVER WAN: 0", 
            font=ctk.CTkFont(family="Consolas", size=11, weight="bold"),
            text_color="#f85149"
        )
        self.info_status.pack(padx=25, pady=20, anchor="w")

        self.console = ctk.CTkTextbox(
            self, 
            font=ctk.CTkFont(family="Consolas", size=11), 
            fg_color="#010204", 
            border_color="#21262d", 
            border_width=1,
            text_color="#c9d1d9",
            corner_radius=10
        )
        self.console.pack(padx=30, pady=25, fill="both", expand=True)

        self.log("CORE", "Служба глобальной WAN маршрутизации запущен.")

    def log(self, tag, text):
        self.console.insert("end", f"[SYSTEM_LOG :: {tag}] -> {text}\n")
        self.console.see("end")

    def update_status_label(self):
        active_agents = len(agents_pool)
        active_clients = len(clients_pool)
        self.info_status.configure(
            text=f"● ACTIVE AGENTS IN POOL: {active_agents}  |  ● WEB CLIENTS OVER WAN: {active_clients}"
        )
        if active_agents > 0:
            self.info_status.configure(text_color="#56d364")
        else:
            self.info_status.configure(text_color="#f85149")

app = ModernServerApp()

async def broadcast_machines_list():
    """Рассылает актуальный список хостов всем подключенным веб-клиентам"""
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
            
            # Обработка пингов для удержания WAN-туннелей ngrok
            if msg_type == "ping":
                await websocket.send(json.dumps({"type": "pong"}))
                continue

            if sender == "agent":
                if msg_type == "handshake":
                    agents_pool[websocket] = {
                        "id": agent_id,
                        "username": data.get("username", "Unknown Win32 Host")
                    }
                    app.log("NODE", f"Добавлен игровой хост: [{data.get('username')}] (ID: {agent_id})")
                    app.update_status_label()
                    await broadcast_machines_list()
                
                elif msg_type == "answer":
                    target_client_id = data.get("targetClientId")
                    # Пересылаем SDP Answer строго запросившему клиенту
                    for c_ws, c_info in clients_pool.items():
                        if c_info["id"] == target_client_id:
                            await c_ws.send(json.dumps({
                                "type": "answer",
                                "sdp": data.get("sdp")
                            }))
                            app.log("BRIDGE", f"SDP Answer доставлен клиенту {target_client_id}")
                            break

            elif sender == "client":
                if websocket not in clients_pool:
                    clients_pool[websocket] = {"id": client_id}
                    app.log("NET", f"Веб-клиент подключился (ID: {client_id})")
                    app.update_status_label()
                
                if msg_type == "get_machines":
                    machines = [{"id": info["id"], "username": info["username"]} for info in agents_pool.values()]
                    await websocket.send(json.dumps({"type": "machines_list", "list": machines}))
                
                elif msg_type == "offer":
                    target_agent_id = data.get("targetId")
                    app.log("BRIDGE", f"Клиент {client_id} инициирует поток к хосту {target_agent_id}")
                    
                    # Пересылаем Offer конкретному агенту, добавляя ID клиента для обратного пути
                    for a_ws, a_info in agents_pool.items():
                        if a_info["id"] == target_agent_id:
                            await a_ws.send(json.dumps({
                                "type": "offer",
                                "sdp": data.get("sdp"),
                                "client_id": client_id
                            }))
                            break
                            
    except websockets.ConnectionClosed:
        pass
    finally:
        if websocket in agents_pool:
            lost_agent = agents_pool.pop(websocket)
            app.log("DROP", f"Игровой хост [{lost_agent['username']}] отключился.")
            await broadcast_machines_list()
        if websocket in clients_pool:
            clients_pool.pop(websocket, None)
            app.log("DROP", f"Веб-клиент сессии {client_id} покинул сеть.")
        app.update_status_label()

async def main():
    async with websockets.serve(handler, "0.0.0.0", 8080, ping_interval=20, ping_timeout=20):
        await asyncio.Future()

if __name__ == "__main__":
    threading.Thread(target=lambda: asyncio.run(main()), daemon=True).start()
    app.mainloop()
