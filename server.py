import asyncio
import json
import logging
import os
import random
import string
from aiohttp import web

PORT = int(os.environ.get("PORT", 8765))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)

rooms = {}
clients = {}

def generate_unique_room_id():
    while True:
        suffix = ''.join(random.choices(string.ascii_uppercase + string.digits, k=5))
        room_id = f"NET_{suffix}"
        if room_id not in rooms:
            return room_id

async def handle_user_leave(room_id, peer_id):
    if room_id not in rooms:
        return

    room_peers = rooms[room_id]
    if peer_id in room_peers:
        del room_peers[peer_id]

    leave_msg = json.dumps({"type": "user-left", "peerId": peer_id})
    for p_id, p_info in list(room_peers.items()):
        ws = p_info["ws"]
        if not ws.closed:
            try:
                await ws.send_str(leave_msg)
            except Exception:
                pass

    if not room_peers:
        del rooms[room_id]
        logging.info(f"Room destroyed: {room_id}")

async def get_active_rooms_payload():
    room_list = []
    for r_id, peers in rooms.items():
        room_list.append({
            "id": r_id,
            "peerCount": len(peers),
            "name": f"Room {r_id}"
        })
    return json.dumps({"type": "rooms-list", "rooms": room_list})

@web.middleware
async def cors_middleware(request, handler):
    if request.method == "OPTIONS":
        response = web.Response(status=204)
    else:
        try:
            response = await handler(request)
        except web.HTTPException as ex:
            response = ex

    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
    response.headers["Access-Control-Allow-Headers"] = "Content-Type, Authorization"
    return response

async def healthcheck_handler(request):
    return web.json_response({
        "status": "online",
        "active_rooms": len(rooms),
        "total_clients": len(clients)
    }, status=200)

async def websocket_handler(request):
    ws = web.WebSocketResponse(heartbeat=25.0, max_msg_size=4*1024*1024)
    await ws.prepare(request)

    client_peer_id = None
    current_room_id = None

    try:
        async for msg in ws:
            if msg.type == web.WSMsgType.TEXT:
                try:
                    data = json.loads(msg.data)
                except json.JSONDecodeError:
                    continue

                msg_type = data.get("type")

                if msg_type == "ping":
                    await ws.send_str(json.dumps({"type": "pong"}))
                    continue

                elif msg_type == "get-rooms":
                    payload = await get_active_rooms_payload()
                    await ws.send_str(payload)

                elif msg_type == "create-room":
                    client_peer_id = data.get("peerId")
                    username = data.get("username", "Anonymous")

                    if not client_peer_id:
                        continue

                    current_room_id = generate_unique_room_id()
                    rooms[current_room_id] = {
                        client_peer_id: {"ws": ws, "username": username}
                    }
                    clients[client_peer_id] = {
                        "ws": ws, "username": username, "room_id": current_room_id
                    }

                    logging.info(f"Room Created: {current_room_id} by {username} ({client_peer_id})")
                    await ws.send_str(json.dumps({
                        "type": "room-created",
                        "roomId": current_room_id
                    }))

                elif msg_type == "join-room":
                    client_peer_id = data.get("peerId")
                    room_id = data.get("roomId")
                    username = data.get("username", "Anonymous")

                    if not client_peer_id or not room_id:
                        continue

                    if room_id not in rooms:
                        await ws.send_str(json.dumps({
                            "type": "room-not-found",
                            "roomId": room_id
                        }))
                        continue

                    current_room_id = room_id
                    room_peers = rooms[room_id]
                    existing_peers = list(room_peers.keys())

                    await ws.send_str(json.dumps({
                        "type": "room-joined",
                        "roomId": room_id,
                        "existingPeers": existing_peers
                    }))

                    join_msg = json.dumps({
                        "type": "user-joined",
                        "peerId": client_peer_id,
                        "username": username
                    })
                    
                    for p_id, p_info in list(room_peers.items()):
                        target_ws = p_info["ws"]
                        if not target_ws.closed:
                            try:
                                await target_ws.send_str(join_msg)
                            except Exception:
                                pass

                    room_peers[client_peer_id] = {"ws": ws, "username": username}
                    clients[client_peer_id] = {
                        "ws": ws, "username": username, "room_id": room_id
                    }

                    logging.info(f"Peer {username} ({client_peer_id}) joined room {room_id}")

                elif msg_type == "rejoin-room":
                    client_peer_id = data.get("peerId")
                    room_id = data.get("roomId")
                    username = data.get("username", "Anonymous")

                    if room_id in rooms and client_peer_id:
                        current_room_id = room_id
                        rooms[room_id][client_peer_id] = {"ws": ws, "username": username}
                        clients[client_peer_id] = {"ws": ws, "username": username, "room_id": room_id}

                elif msg_type in ("offer", "answer", "candidate"):
                    target_id = data.get("targetId")
                    if target_id in clients:
                        target_ws = clients[target_id]["ws"]
                        if not target_ws.closed:
                            try:
                                await target_ws.send_str(json.dumps(data))
                            except Exception:
                                pass

                elif msg_type == "chat-message":
                    room_id = data.get("roomId")
                    sender_id = data.get("senderId")
                    if room_id in rooms:
                        chat_payload = json.dumps(data)
                        for p_id, p_info in list(rooms[room_id].items()):
                            if p_id != sender_id and not p_info["ws"].closed:
                                try:
                                    await p_info["ws"].send_str(chat_payload)
                                except Exception:
                                    pass

                elif msg_type == "leave-room":
                    room_id = data.get("roomId")
                    peer_id = data.get("peerId")
                    if room_id and peer_id:
                        await handle_user_leave(room_id, peer_id)
                        clients.pop(peer_id, None)

            elif msg.type in (web.WSMsgType.ERROR, web.WSMsgType.CLOSED):
                break

    except Exception as e:
        logging.error(f"WebSocket error: {e}")
    finally:
        if client_peer_id:
            clients.pop(client_peer_id, None)
            if current_room_id:
                await handle_user_leave(current_room_id, client_peer_id)

    return ws

def create_app():
    app = web.Application(middlewares=[cors_middleware])
    app.router.add_get("/", healthcheck_handler)
    app.router.add_get("/ping", healthcheck_handler)
    app.router.add_get("/healthcheck", healthcheck_handler)
    app.router.add_get("/ws", websocket_handler)
    app.router.add_get("/ws/", websocket_handler)
    return app

if __name__ == "__main__":
    app = create_app()
    web.run_app(app, host="0.0.0.0", port=PORT)
