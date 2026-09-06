import asyncio
import json
import logging
import os
import websockets

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)

CLIENTS = set()

async def register(websocket):
    CLIENTS.add(websocket)
    logging.info(f"Client mới kết nối từ {websocket.remote_address} | Tổng client: {len(CLIENTS)}")

async def unregister(websocket):
    CLIENTS.discard(websocket)
    logging.info(f"Client ngắt kết nối {websocket.remote_address} | Tổng client: {len(CLIENTS)}")

async def handler(websocket):
    await register(websocket)
    try:
        async for message in websocket:
            receivers = {client for client in CLIENTS if client != websocket}
            if receivers:
                websockets.broadcast(receivers, message)
    except websockets.exceptions.ConnectionClosed:
        pass
    except Exception as e:
        logging.error(f"Lỗi không xác định với {websocket.remote_address}: {e}")
    finally:
        await unregister(websocket)

async def main():
    port = int(os.environ.get("PORT", 8765))
    
    async with websockets.serve(handler, "0.0.0.0", port):
        logging.info(f"⚡ P2P Signaling Server đang chạy tại ws://0.0.0.0:{port}")
        await asyncio.Future()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Server đang dừng...")