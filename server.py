import asyncio
import json
import logging
import os
import random
import string
import websockets

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
    room_peers.discard(peer_id)

    leave_msg = json.dumps({"type": "user-left", "peerId": peer_id})
    for remaining_peer_id in list(room_peers):
        target_ws = clients.get(remaining_peer_id)
        if target_ws and target_ws.open:
            await target_ws.send(leave_msg)

    if not room_peers:
        del rooms[room_id]

async def handler(websocket):
    client_peer_id = None
    try:
        async for message in websocket:
            try:
                data = json.loads(message)
            except json.JSONDecodeError:
                continue

            msg_type = data.get("type")

            if msg_type == "create-room":
                client_peer_id = data.get("peerId")
                if not client_peer_id:
                    continue

                new_room_id = generate_unique_room_id()
                rooms[new_room_id] = {client_peer_id}
                clients[client_peer_id] = websocket

                logging.info(f"Room created: {new_room_id} by Peer: {client_peer_id}")
                await websocket.send(json.dumps({
                    "type": "room-created",
                    "roomId": new_room_id
                }))

            elif msg_type == "join-room":
                client_peer_id = data.get("peerId")
                room_id = data.get("roomId")

                if not client_peer_id or not room_id:
                    continue

                if room_id not in rooms:
                    await websocket.send(json.dumps({
                        "type": "room-not-found",
                        "roomId": room_id
                    }))
                    continue

                room_peers = rooms[room_id]
                existing_peers = list(room_peers)

                room_peers.add(client_peer_id)
                clients[client_peer_id] = websocket

                logging.info(f"Peer {client_peer_id} joined room {room_id}")

                await websocket.send(json.dumps({
                    "type": "room-joined",
                    "roomId": room_id,
                    "existingPeers": existing_peers
                }))

                join_msg = json.dumps({
                    "type": "user-joined",
                    "peerId": client_peer_id
                })
                for existing_peer_id in existing_peers:
                    target_ws = clients.get(existing_peer_id)
                    if target_ws and target_ws.open:
                        await target_ws.send(join_msg)

            elif msg_type in ("offer", "answer", "candidate"):
                target_id = data.get("targetId")
                target_ws = clients.get(target_id)
                if target_ws and target_ws.open:
                    await target_ws.send(json.dumps(data))

            elif msg_type == "chat-message":
                room_id = data.get("roomId")
                sender_id = data.get("senderId")
                room_peers = rooms.get(room_id, set())

                chat_msg = json.dumps(data)
                for peer_id in list(room_peers):
                    if peer_id != sender_id:
                        target_ws = clients.get(peer_id)
                        if target_ws and target_ws.open:
                            await target_ws.send(chat_msg)

            elif msg_type == "leave-room":
                room_id = data.get("roomId")
                peer_id = data.get("peerId")
                if room_id and peer_id:
                    await handle_user_leave(room_id, peer_id)

    except websockets.exceptions.ConnectionClosed:
        pass
    except Exception as e:
        logging.error(f"Unexpected connection error: {e}")
    finally:
        if client_peer_id:
            clients.pop(client_peer_id, None)
            for room_id, peers in list(rooms.items()):
                if client_peer_id in peers:
                    await handle_user_leave(room_id, client_peer_id)

async def main():
    port = int(os.environ.get("PORT", 8765))
    
    async with websockets.serve(
        handler, 
        "0.0.0.0", 
        port,
        ping_interval=20,
        ping_timeout=20
    ):
        logging.info(f"Signaling Server running on port {port}")
        await asyncio.Future()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Server stopping...")
